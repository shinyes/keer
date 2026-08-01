package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type AttachmentService struct {
	store         *store.SQLStore
	storageRouter *storage.Router
	tempDir       string
}

const (
	attachmentNanoIDLength     = 8
	attachmentStorageKeyTries  = 8
	attachmentNanoIDAlphabet   = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	uploadSessionTTL           = 24 * time.Hour
	uploadSessionCleanupBatch  = 200
	directUploadURLTTL         = 15 * time.Minute
	multipartUploadURLTTL      = 15 * time.Minute
	directDownloadURLTTL       = 10 * time.Minute
	directSessionPathPrefix    = "__S3_DIRECT__:"
	multipartSessionPathPrefix = "__S3_MULTIPART__:"
	s3MultipartPartSizeBytes   = 8 * 1024 * 1024
)

func NewAttachmentService(s *store.SQLStore, storageRouter *storage.Router) *AttachmentService {
	tempDir := filepath.Join(os.TempDir(), "keer", "upload_sessions")
	return &AttachmentService{
		store:         s,
		storageRouter: storageRouter,
		tempDir:       tempDir,
	}
}

type CreateAttachmentInput struct {
	Filename           string
	Type               string
	Content            string
	EncryptionMetadata string
	MemoName           *string
}

type CreateAttachmentUploadSessionInput struct {
	Filename           string
	Type               string
	Size               int64
	EncryptionMetadata string
	MemoName           *string
	Thumbnail          *CreateAttachmentUploadSessionThumbnailInput
}

type CreateAttachmentUploadSessionThumbnailInput struct {
	Filename string
	Type     string
	Content  string
}

type UpdateAttachmentThumbnailInput struct {
	Filename                string
	Type                    string
	Content                 string
	ThumbnailBlobEncryption *string
}

type StorageCleanupResult struct {
	ScannedKeys int
	DeletedKeys int
	FailedKeys  int
}

var (
	ErrUploadSessionNotFound      = errors.New("upload session not found")
	ErrUploadOffsetMismatch       = errors.New("upload offset mismatch")
	ErrUploadExceedsTotalSize     = errors.New("upload exceeds total size")
	ErrUploadNotComplete          = errors.New("upload not complete")
	ErrUploadChunkUnsupported     = errors.New("upload chunk is not supported for this session")
	ErrMultipartPartInvalid       = errors.New("multipart upload part is invalid")
	ErrAttachmentPermissionDenied = errors.New("attachment permission denied")
	ErrInvalidAttachmentThumbnail = errors.New("invalid attachment thumbnail")
)

type UploadOffsetMismatchError struct {
	CurrentOffset int64
}

func (e *UploadOffsetMismatchError) Error() string {
	return fmt.Sprintf("upload offset mismatch current=%d", e.CurrentOffset)
}

type DirectUploadSession struct {
	UploadURL string
	Method    string
}

type MultipartUploadPartSession struct {
	PartSize int64
}

type MultipartPartUploadURL struct {
	UploadURL  string
	Method     string
	PartNumber int32
	Offset     int64
	Size       int64
}

type multipartSessionInfo struct {
	StorageKey        string
	MultipartUploadID string
	PartSize          int64
}

func (s *AttachmentService) CreateAttachment(ctx context.Context, userID int64, input CreateAttachmentInput) (models.Attachment, error) {
	defaultStorage := s.defaultStorage()
	if defaultStorage == nil {
		return models.Attachment{}, fmt.Errorf("attachment storage is not configured")
	}
	filename := sanitizeFilename(input.Filename)
	if filename == "" {
		return models.Attachment{}, fmt.Errorf("filename cannot be empty")
	}
	contentType := strings.TrimSpace(input.Type)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	payload := strings.TrimSpace(input.Content)
	if payload == "" {
		return models.Attachment{}, fmt.Errorf("content cannot be empty")
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return models.Attachment{}, fmt.Errorf("invalid base64 content")
	}
	contentHash := hashAttachmentContent(data)

	var memoID *int64
	if input.MemoName != nil {
		id, err := parseMemoID(*input.MemoName)
		if err != nil {
			return models.Attachment{}, err
		}
		if _, err := s.store.GetMemoByIDAndCreator(ctx, id, userID); err != nil {
			return models.Attachment{}, err
		}
		memoID = &id
	}

	existing, found, err := s.store.FindAttachmentByContentHash(ctx, userID, contentHash)
	if err != nil {
		return models.Attachment{}, err
	}

	var storageKey string
	var size int64
	uploaded := false
	if found {
		storageKey = existing.StorageKey
		size = existing.Size
	} else {
		storageKey, err = s.newAttachmentStorageKey(ctx, userID, filename)
		if err != nil {
			return models.Attachment{}, err
		}
		size, err = defaultStorage.Put(ctx, storageKey, contentType, data)
		if err != nil {
			return models.Attachment{}, err
		}
		uploaded = true
	}

	attachment, err := s.store.CreateAttachment(
		ctx,
		userID,
		filename,
		"",
		contentType,
		size,
		contentHash,
		strings.TrimSpace(input.EncryptionMetadata),
		s.defaultStorageType(),
		storageKey,
	)
	if err != nil {
		if uploaded {
			_ = defaultStorage.Delete(ctx, storageKey)
		}
		return models.Attachment{}, err
	}
	if found {
		s.copyThumbnailMetadataFromExisting(ctx, attachment.ID, existing)
	} else {
		s.ensureThumbnailFromBytes(ctx, attachment, contentType, filename, data)
	}
	if refreshed, refreshErr := s.store.GetAttachmentByID(ctx, attachment.ID); refreshErr == nil {
		attachment = refreshed
	}

	if memoID != nil {
		if err := s.attachToMemo(ctx, *memoID, attachment); err != nil {
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
	}

	return attachment, nil
}


func (s *AttachmentService) PresignAttachmentURL(ctx context.Context, attachment models.Attachment) (string, bool, error) {
	if !strings.EqualFold(strings.TrimSpace(attachment.StorageType), storage.TypeS3) {
		return "", false, nil
	}
	store, err := s.storageForType(attachment.StorageType)
	if err != nil {
		return "", false, nil
	}
	s3Store, ok := store.(*storage.S3Store)
	if !ok {
		return "", false, nil
	}
	if strings.TrimSpace(attachment.StorageKey) == "" {
		return "", false, nil
	}
	url, err := s3Store.PresignGetObjectURL(ctx, attachment.StorageKey, directDownloadURLTTL)
	if err != nil {
		return "", false, err
	}
	return url, true, nil
}

func (s *AttachmentService) PresignAttachmentThumbnailURL(ctx context.Context, attachment models.Attachment) (string, bool, error) {
	if strings.TrimSpace(attachment.ThumbnailStorageKey) == "" {
		return "", false, nil
	}
	thumbnailStorageType := attachment.ThumbnailStorageType
	if strings.TrimSpace(thumbnailStorageType) == "" {
		thumbnailStorageType = attachment.StorageType
	}
	if !strings.EqualFold(strings.TrimSpace(thumbnailStorageType), storage.TypeS3) {
		return "", false, nil
	}
	store, err := s.storageForType(thumbnailStorageType)
	if err != nil {
		return "", false, nil
	}
	s3Store, ok := store.(*storage.S3Store)
	if !ok {
		return "", false, nil
	}
	url, err := s3Store.PresignGetObjectURL(ctx, attachment.ThumbnailStorageKey, directDownloadURLTTL)
	if err != nil {
		return "", false, err
	}
	return url, true, nil
}


func (s *AttachmentService) ListAttachments(ctx context.Context, userID int64) ([]models.Attachment, error) {
	return s.store.ListAttachmentsByCreator(ctx, userID)
}

func (s *AttachmentService) UpdateAttachmentThumbnail(
	ctx context.Context,
	userID int64,
	attachmentID int64,
	input UpdateAttachmentThumbnailInput,
) (models.Attachment, error) {
	attachment, err := s.store.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return models.Attachment{}, err
	}
	if attachment.CreatorID != userID {
		return models.Attachment{}, ErrAttachmentPermissionDenied
	}

	content := strings.TrimSpace(input.Content)
	if content == "" {
		return models.Attachment{}, fmt.Errorf("%w: content cannot be empty", ErrInvalidAttachmentThumbnail)
	}
	data, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return models.Attachment{}, fmt.Errorf("%w: invalid base64 content", ErrInvalidAttachmentThumbnail)
	}
	if len(data) == 0 {
		return models.Attachment{}, fmt.Errorf("%w: content cannot be empty", ErrInvalidAttachmentThumbnail)
	}
	if len(data) > thumbnailUploadMaxSize {
		return models.Attachment{}, fmt.Errorf("%w: content too large", ErrInvalidAttachmentThumbnail)
	}

	thumbnailFilename := sanitizeFilename(input.Filename)
	if thumbnailFilename == "" {
		thumbnailFilename = buildThumbnailFilename(attachment.Filename)
	}
	thumbnailType := strings.TrimSpace(input.Type)
	if thumbnailType == "" {
		thumbnailType = thumbnailContentType
	}

	thumbnailKey := thumbnailStorageKey(attachment.StorageKey)
	if thumbnailKey == "" {
		return models.Attachment{}, fmt.Errorf("invalid attachment storage key")
	}
	store, err := s.storageForType(attachment.StorageType)
	if err != nil {
		return models.Attachment{}, err
	}
	thumbnailSize, err := store.Put(ctx, thumbnailKey, thumbnailType, data)
	if err != nil {
		return models.Attachment{}, fmt.Errorf("store thumbnail: %w", err)
	}
	if thumbnailSize <= 0 {
		return models.Attachment{}, fmt.Errorf("store thumbnail failed")
	}

	if err := s.store.UpdateAttachmentThumbnail(
		ctx,
		attachmentID,
		thumbnailFilename,
		thumbnailType,
		thumbnailSize,
		storageTypeName(store),
		thumbnailKey,
	); err != nil {
		return models.Attachment{}, err
	}

	if input.ThumbnailBlobEncryption != nil {
		if err := s.patchAttachmentThumbnailBlobEncryptionMetadata(
			ctx,
			attachmentID,
			attachment.EncryptionMetadata,
			*input.ThumbnailBlobEncryption,
		); err != nil {
			return models.Attachment{}, err
		}
	}

	return s.store.GetAttachmentByID(ctx, attachmentID)
}


func (s *AttachmentService) DeleteAttachment(ctx context.Context, userID int64, attachmentID int64) error {
	attachment, err := s.store.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return err
	}
	if attachment.CreatorID != userID {
		return sql.ErrNoRows
	}

	refCount, err := s.store.CountAttachmentsByStorageKey(ctx, attachment.StorageKey)
	if err != nil {
		return err
	}
	if refCount <= 1 {
		mainStore, err := s.storageForType(attachment.StorageType)
		if err != nil {
			return err
		}
		if err := mainStore.Delete(ctx, attachment.StorageKey); err != nil {
			return err
		}
		if attachment.ThumbnailStorageKey != "" {
			thumbnailStorageType := attachment.ThumbnailStorageType
			if strings.TrimSpace(thumbnailStorageType) == "" {
				thumbnailStorageType = attachment.StorageType
			}
			if thumbnailStore, thumbnailErr := s.storageForType(thumbnailStorageType); thumbnailErr == nil {
				_ = thumbnailStore.Delete(ctx, attachment.ThumbnailStorageKey)
			}
		}
	}
	return s.store.DeleteAttachment(ctx, attachmentID)
}

func (s *AttachmentService) GetAttachment(ctx context.Context, attachmentID int64) (models.Attachment, error) {
	return s.store.GetAttachmentByID(ctx, attachmentID)
}

func (s *AttachmentService) AttachmentVisibleToUser(ctx context.Context, attachmentID int64, userID int64) (bool, error) {
	return s.store.AttachmentVisibleToUser(ctx, attachmentID, userID)
}

func (s *AttachmentService) OpenAttachmentStream(ctx context.Context, attachment models.Attachment) (io.ReadCloser, error) {
	store, err := s.storageForType(attachment.StorageType)
	if err != nil {
		return nil, err
	}
	return store.Open(ctx, attachment.StorageKey)
}

func (s *AttachmentService) OpenAttachmentRangeStream(ctx context.Context, attachment models.Attachment, start int64, end int64) (io.ReadCloser, error) {
	store, err := s.storageForType(attachment.StorageType)
	if err != nil {
		return nil, err
	}
	return store.OpenRange(ctx, attachment.StorageKey, start, end)
}

func (s *AttachmentService) OpenAttachmentThumbnailStream(ctx context.Context, attachment models.Attachment) (io.ReadCloser, error) {
	if strings.TrimSpace(attachment.ThumbnailStorageKey) == "" {
		return nil, os.ErrNotExist
	}
	thumbnailStorageType := attachment.ThumbnailStorageType
	if strings.TrimSpace(thumbnailStorageType) == "" {
		thumbnailStorageType = attachment.StorageType
	}
	store, err := s.storageForType(thumbnailStorageType)
	if err != nil {
		return nil, err
	}
	return store.Open(ctx, attachment.ThumbnailStorageKey)
}

func (s *AttachmentService) OpenAttachment(ctx context.Context, attachmentID int64) (models.Attachment, io.ReadCloser, error) {
	attachment, err := s.GetAttachment(ctx, attachmentID)
	if err != nil {
		return models.Attachment{}, nil, err
	}
	rc, err := s.OpenAttachmentStream(ctx, attachment)
	if err != nil {
		return models.Attachment{}, nil, err
	}
	return attachment, rc, nil
}

func (s *AttachmentService) OpenAttachmentRange(ctx context.Context, attachmentID int64, start int64, end int64) (models.Attachment, io.ReadCloser, error) {
	attachment, err := s.GetAttachment(ctx, attachmentID)
	if err != nil {
		return models.Attachment{}, nil, err
	}
	rc, err := s.OpenAttachmentRangeStream(ctx, attachment, start, end)
	if err != nil {
		return models.Attachment{}, nil, err
	}
	return attachment, rc, nil
}


func (s *AttachmentService) attachToMemo(ctx context.Context, memoID int64, attachment models.Attachment) error {
	attachedMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memoID})
	if err != nil {
		return err
	}
	attachments := make([]store.AttachmentBinding, 0, len(attachedMap[memoID])+1)
	seen := make(map[int64]struct{}, len(attachedMap[memoID])+1)
	for _, item := range attachedMap[memoID] {
		if _, ok := seen[item.ID]; ok {
			continue
		}
		attachments = append(attachments, store.AttachmentBinding{
			AttachmentID:                  item.ID,
			AssociationEncryptionMetadata: item.AssociationEncryptionMetadata,
		})
		seen[item.ID] = struct{}{}
	}
	if _, ok := seen[attachment.ID]; !ok {
		attachments = append(attachments, store.AttachmentBinding{
			AttachmentID:                  attachment.ID,
			AssociationEncryptionMetadata: strings.TrimSpace(attachment.EncryptionMetadata),
		})
	}
	return s.store.SetMemoAttachments(ctx, memoID, attachments)
}

func (s *AttachmentService) patchAttachmentThumbnailBlobEncryptionMetadata(
	ctx context.Context,
	attachmentID int64,
	attachmentEncryptionMetadata string,
	thumbnailBlobEncryption string,
) error {
	trimmedAttachmentMetadata := strings.TrimSpace(attachmentEncryptionMetadata)
	trimmedThumbnailBlobEncryption := strings.TrimSpace(thumbnailBlobEncryption)
	if trimmedAttachmentMetadata == "" {
		if trimmedThumbnailBlobEncryption != "" {
			return fmt.Errorf("%w: thumbnailBlobEncryption requires encrypted attachment metadata", ErrInvalidAttachmentThumbnail)
		}
		return nil
	}

	updatedAttachmentMetadata, err := patchThumbnailBlobEncryptionMetadata(
		trimmedAttachmentMetadata,
		trimmedThumbnailBlobEncryption,
	)
	if err != nil {
		return err
	}
	if err := s.store.UpdateAttachmentEncryptionMetadata(ctx, attachmentID, updatedAttachmentMetadata); err != nil {
		return err
	}

	memoAssociations, err := s.store.ListMemoAttachmentAssociationMetadataByAttachmentID(ctx, attachmentID)
	if err != nil {
		return err
	}
	for _, association := range memoAssociations {
		trimmedAssociationMetadata := strings.TrimSpace(association.AssociationEncryptionMetadata)
		if trimmedAssociationMetadata == "" {
			continue
		}
		updatedAssociationMetadata, err := patchThumbnailBlobEncryptionMetadata(
			trimmedAssociationMetadata,
			trimmedThumbnailBlobEncryption,
		)
		if err != nil {
			return err
		}
		if err := s.store.UpdateMemoAttachmentAssociationEncryptionMetadata(
			ctx,
			association.MemoID,
			association.AttachmentID,
			updatedAssociationMetadata,
		); err != nil {
			return err
		}
	}

	groupAssociations, err := s.store.ListGroupMessageAttachmentAssociationMetadataByAttachmentID(ctx, attachmentID)
	if err != nil {
		return err
	}
	for _, association := range groupAssociations {
		trimmedAssociationMetadata := strings.TrimSpace(association.AssociationEncryptionMetadata)
		if trimmedAssociationMetadata == "" {
			continue
		}
		updatedAssociationMetadata, err := patchThumbnailBlobEncryptionMetadata(
			trimmedAssociationMetadata,
			trimmedThumbnailBlobEncryption,
		)
		if err != nil {
			return err
		}
		if err := s.store.UpdateGroupMessageAttachmentAssociationEncryptionMetadata(
			ctx,
			association.MessageID,
			association.AttachmentID,
			updatedAssociationMetadata,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *AttachmentService) newAttachmentStorageKey(ctx context.Context, userID int64, filename string) (string, error) {
	for i := 0; i < attachmentStorageKeyTries; i++ {
		nanoID, err := generateNanoID(attachmentNanoIDLength)
		if err != nil {
			return "", err
		}
		key := buildAttachmentStorageKey(userID, nanoID)
		count, err := s.store.CountAttachmentsByStorageKey(ctx, key)
		if err != nil {
			return "", err
		}
		if count == 0 {
			return key, nil
		}
	}
	return "", fmt.Errorf("failed to allocate unique attachment storage key")
}


func (s *AttachmentService) hashTempFileSHA256(path string) (string, error) {
	f, tempRoot, err := s.openTempFileForRead(path)
	if err != nil {
		return "", fmt.Errorf("open upload temp file for hash: %w", err)
	}
	defer f.Close()
	defer tempRoot.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", fmt.Errorf("hash upload temp file: %w", err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (s *AttachmentService) openTempFileForRead(path string) (*os.File, *os.Root, error) {
	relPath, err := s.tempRelativePath(path)
	if err != nil {
		return nil, nil, err
	}
	tempRoot, err := os.OpenRoot(filepath.Clean(strings.TrimSpace(s.tempDir)))
	if err != nil {
		return nil, nil, err
	}
	f, err := tempRoot.Open(relPath)
	if err != nil {
		_ = tempRoot.Close()
		return nil, nil, err
	}
	return f, tempRoot, nil
}

func (s *AttachmentService) tempRelativePath(path string) (string, error) {
	baseDir := filepath.Clean(strings.TrimSpace(s.tempDir))
	targetPath := filepath.Clean(strings.TrimSpace(path))
	if baseDir == "" || targetPath == "" {
		return "", fmt.Errorf("invalid temp file path")
	}
	relPath, err := filepath.Rel(baseDir, targetPath)
	if err != nil {
		return "", fmt.Errorf("resolve temp file path: %w", err)
	}
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" || filepath.IsAbs(relPath) || strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("invalid temp file path traversal")
	}
	return relPath, nil
}


func (s *AttachmentService) listContiguousMultipartParts(
	ctx context.Context,
	info multipartSessionInfo,
) ([]storage.S3UploadedPart, int64, int32, error) {
	s3Store := s.defaultS3Store()
	if s3Store == nil {
		return nil, 0, 0, fmt.Errorf("multipart upload session requires s3 storage")
	}
	uploadedParts, err := s3Store.ListMultipartUploadedParts(ctx, info.StorageKey, info.MultipartUploadID)
	if err != nil {
		return nil, 0, 0, err
	}
	sort.Slice(uploadedParts, func(i, j int) bool {
		return uploadedParts[i].PartNumber < uploadedParts[j].PartNumber
	})

	contiguous := make([]storage.S3UploadedPart, 0, len(uploadedParts))
	expectedPart := int32(1)
	var totalSize int64
	for _, part := range uploadedParts {
		if part.PartNumber != expectedPart {
			break
		}
		if part.Size <= 0 || strings.TrimSpace(part.ETag) == "" {
			break
		}
		contiguous = append(contiguous, part)
		totalSize += part.Size
		expectedPart++
	}
	return contiguous, totalSize, expectedPart, nil
}

func (s *AttachmentService) CleanupOrphanFiles(ctx context.Context) (StorageCleanupResult, error) {
	unattachedAttachments, err := s.store.ListUnattachedAttachments(ctx)
	if err != nil {
		return StorageCleanupResult{}, err
	}
	if len(unattachedAttachments) > 0 {
		attachmentIDs := make([]int64, 0, len(unattachedAttachments))
		for _, attachment := range unattachedAttachments {
			attachmentIDs = append(attachmentIDs, attachment.ID)
		}
		if err := s.store.DeleteAttachmentsByIDs(ctx, attachmentIDs); err != nil {
			return StorageCleanupResult{}, err
		}
	}

	referencedByType := make(map[string]map[string]struct{})
	addReference := func(storeType string, key string) {
		normalizedType := storage.NormalizeType(storeType)
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			return
		}
		items := referencedByType[normalizedType]
		if items == nil {
			items = make(map[string]struct{})
			referencedByType[normalizedType] = items
		}
		items[trimmedKey] = struct{}{}
	}

	attachments, err := s.store.ListAllAttachments(ctx)
	if err != nil {
		return StorageCleanupResult{}, err
	}
	for _, attachment := range attachments {
		addReference(attachment.StorageType, attachment.StorageKey)
		thumbnailType := attachment.ThumbnailStorageType
		if strings.TrimSpace(thumbnailType) == "" {
			thumbnailType = attachment.StorageType
		}
		addReference(thumbnailType, attachment.ThumbnailStorageKey)
	}

	users, err := s.store.ListUserAvatarStorageRefs(ctx)
	if err != nil {
		return StorageCleanupResult{}, err
	}
	for _, user := range users {
		addReference(user.AvatarStorageType, avatarStorageKey(user.ID))
	}

	var result StorageCleanupResult
	var firstErr error
	for _, backend := range s.storageRouter.Stores() {
		storeType := storage.NormalizeType(backend.Type())
		keys, listErr := backend.ListKeys(ctx, "")
		if listErr != nil {
			if firstErr == nil {
				firstErr = listErr
			}
			continue
		}
		referenced := referencedByType[storeType]
		for _, key := range keys {
			trimmedKey := strings.TrimSpace(key)
			if !isManagedStorageKey(trimmedKey) {
				continue
			}
			result.ScannedKeys++
			if _, exists := referenced[trimmedKey]; exists {
				continue
			}
			if deleteErr := backend.Delete(ctx, trimmedKey); deleteErr != nil {
				result.FailedKeys++
				if firstErr == nil {
					firstErr = deleteErr
				}
				continue
			}
			result.DeletedKeys++
		}
	}
	return result, firstErr
}


func (s *AttachmentService) defaultStorage() storage.Store {
	if s.storageRouter == nil {
		return nil
	}
	return s.storageRouter.DefaultStore()
}

func (s *AttachmentService) defaultStorageType() string {
	if s.storageRouter == nil {
		return storage.TypeLocal
	}
	return s.storageRouter.DefaultType()
}

func (s *AttachmentService) storageForType(storeType string) (storage.Store, error) {
	if s.storageRouter == nil {
		return nil, fmt.Errorf("storage is not configured")
	}
	resolved, ok := s.storageRouter.StoreForType(storeType)
	if !ok {
		return nil, fmt.Errorf("storage type %s is not configured", storage.NormalizeType(storeType))
	}
	return resolved, nil
}

func (s *AttachmentService) defaultS3Store() *storage.S3Store {
	store := s.defaultStorage()
	s3Store, _ := store.(*storage.S3Store)
	return s3Store
}
