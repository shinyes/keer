package service

import (
	"context"
	"crypto/rand"
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
	"strconv"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type AttachmentService struct {
	store   *store.SQLStore
	storage storage.Store
	tempDir string
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

func NewAttachmentService(s *store.SQLStore, fileStorage storage.Store) *AttachmentService {
	tempDir := filepath.Join(os.TempDir(), "keer", "upload_sessions")
	return &AttachmentService{
		store:   s,
		storage: fileStorage,
		tempDir: tempDir,
	}
}

type CreateAttachmentInput struct {
	Filename string
	Type     string
	Content  string
	MemoName *string
}

type CreateAttachmentUploadSessionInput struct {
	Filename  string
	Type      string
	Size      int64
	MemoName  *string
	Thumbnail *CreateAttachmentUploadSessionThumbnailInput
}

type CreateAttachmentUploadSessionThumbnailInput struct {
	Filename string
	Type     string
	Content  string
}

var (
	ErrUploadSessionNotFound  = errors.New("upload session not found")
	ErrUploadOffsetMismatch   = errors.New("upload offset mismatch")
	ErrUploadExceedsTotalSize = errors.New("upload exceeds total size")
	ErrUploadNotComplete      = errors.New("upload not complete")
	ErrUploadChunkUnsupported = errors.New("upload chunk is not supported for this session")
	ErrMultipartPartInvalid   = errors.New("multipart upload part is invalid")
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
		size, err = s.storage.Put(ctx, storageKey, contentType, data)
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
		storageTypeName(s.storage),
		storageKey,
	)
	if err != nil {
		if uploaded {
			_ = s.storage.Delete(ctx, storageKey)
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
		if err := s.attachToMemo(ctx, *memoID, attachment.ID); err != nil {
			return models.Attachment{}, err
		}
	}

	return attachment, nil
}

func (s *AttachmentService) CreateAttachmentUploadSession(ctx context.Context, userID int64, input CreateAttachmentUploadSessionInput) (models.AttachmentUploadSession, error) {
	_ = s.CleanupExpiredUploadSessions(ctx)

	filename := sanitizeFilename(input.Filename)
	if filename == "" {
		return models.AttachmentUploadSession{}, fmt.Errorf("filename cannot be empty")
	}
	contentType := strings.TrimSpace(input.Type)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if input.Size <= 0 {
		return models.AttachmentUploadSession{}, fmt.Errorf("size must be positive")
	}

	thumbnailFilename := ""
	thumbnailType := ""
	thumbnailData := []byte(nil)
	if input.Thumbnail != nil {
		thumbnailFilename = sanitizeFilename(input.Thumbnail.Filename)
		if thumbnailFilename == "" {
			thumbnailFilename = buildThumbnailFilename(filename)
		}
		thumbnailType = strings.TrimSpace(input.Thumbnail.Type)
		if thumbnailType == "" {
			thumbnailType = thumbnailContentType
		}
		thumbnailPayload := strings.TrimSpace(input.Thumbnail.Content)
		if thumbnailPayload == "" {
			return models.AttachmentUploadSession{}, fmt.Errorf("thumbnail content cannot be empty")
		}
		decoded, err := base64.StdEncoding.DecodeString(thumbnailPayload)
		if err != nil {
			return models.AttachmentUploadSession{}, fmt.Errorf("invalid thumbnail base64 content")
		}
		if len(decoded) == 0 {
			return models.AttachmentUploadSession{}, fmt.Errorf("thumbnail content cannot be empty")
		}
		if len(decoded) > thumbnailUploadMaxSize {
			return models.AttachmentUploadSession{}, fmt.Errorf("thumbnail content too large")
		}
		thumbnailData = decoded
	}

	var memoName *string
	if input.MemoName != nil {
		trimmed := strings.TrimSpace(*input.MemoName)
		if trimmed != "" {
			id, err := parseMemoID(trimmed)
			if err != nil {
				return models.AttachmentUploadSession{}, err
			}
			if _, err := s.store.GetMemoByIDAndCreator(ctx, id, userID); err != nil {
				return models.AttachmentUploadSession{}, err
			}
			memoName = &trimmed
		}
	}

	uploadID, err := generateNanoID(24)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}

	thumbnailTempPath := ""
	if len(thumbnailData) > 0 {
		if err := os.MkdirAll(s.tempDir, 0o755); err != nil {
			return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp dir: %w", err)
		}
		thumbnailTempPath = filepath.Join(s.tempDir, uploadID+".thumb")
		if err := os.WriteFile(thumbnailTempPath, thumbnailData, 0o644); err != nil {
			return models.AttachmentUploadSession{}, fmt.Errorf("create upload thumbnail temp file: %w", err)
		}
	}

	if s3Store, ok := s.storage.(*storage.S3Store); ok {
		storageKey, err := s.newAttachmentStorageKey(ctx, userID, filename)
		if err != nil {
			if thumbnailTempPath != "" {
				_ = os.Remove(thumbnailTempPath)
			}
			return models.AttachmentUploadSession{}, err
		}
		tempPath := encodeDirectSessionPath(storageKey)
		if multipartUploadID, multipartErr := s3Store.CreateMultipartUpload(ctx, storageKey, contentType); multipartErr == nil {
			tempPath = encodeMultipartSessionPath(storageKey, multipartUploadID, s3MultipartPartSizeBytes)
		} else if !errors.Is(multipartErr, storage.ErrS3MultipartUnsupported) {
			if thumbnailTempPath != "" {
				_ = os.Remove(thumbnailTempPath)
			}
			return models.AttachmentUploadSession{}, multipartErr
		}
		now := time.Now().UTC()
		session, err := s.store.CreateAttachmentUploadSession(ctx, models.AttachmentUploadSession{
			ID:                uploadID,
			CreatorID:         userID,
			Filename:          filename,
			Type:              contentType,
			Size:              input.Size,
			MemoName:          memoName,
			TempPath:          tempPath,
			ThumbnailFilename: thumbnailFilename,
			ThumbnailType:     thumbnailType,
			ThumbnailTempPath: thumbnailTempPath,
			ReceivedSize:      0,
			CreateTime:        now,
			UpdateTime:        now,
		})
		if err != nil {
			if thumbnailTempPath != "" {
				_ = os.Remove(thumbnailTempPath)
			}
			return models.AttachmentUploadSession{}, err
		}
		return session, nil
	}

	if err := os.MkdirAll(s.tempDir, 0o755); err != nil {
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp dir: %w", err)
	}
	tempPath := filepath.Join(s.tempDir, uploadID+".part")
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp file: %w", err)
	}
	_ = tempFile.Close()

	now := time.Now().UTC()
	session, err := s.store.CreateAttachmentUploadSession(ctx, models.AttachmentUploadSession{
		ID:                uploadID,
		CreatorID:         userID,
		Filename:          filename,
		Type:              contentType,
		Size:              input.Size,
		MemoName:          memoName,
		TempPath:          tempPath,
		ThumbnailFilename: thumbnailFilename,
		ThumbnailType:     thumbnailType,
		ThumbnailTempPath: thumbnailTempPath,
		ReceivedSize:      0,
		CreateTime:        now,
		UpdateTime:        now,
	})
	if err != nil {
		_ = os.Remove(tempPath)
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, err
	}
	return session, nil
}

func (s *AttachmentService) CleanupExpiredUploadSessions(ctx context.Context) error {
	cutoff := time.Now().UTC().Add(-uploadSessionTTL)
	var firstErr error

	for {
		sessions, err := s.store.ListAttachmentUploadSessionsUpdatedBefore(ctx, cutoff, uploadSessionCleanupBatch)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if len(sessions) == 0 {
			break
		}

		for _, session := range sessions {
			if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if multipart, ok := decodeMultipartSessionPath(session.TempPath); ok {
				if s3Store, s3OK := s.storage.(*storage.S3Store); s3OK {
					_ = s3Store.AbortMultipartUpload(ctx, multipart.StorageKey, multipart.MultipartUploadID)
				}
			} else if storageKey, direct := decodeDirectSessionPath(session.TempPath); direct {
				_ = s.storage.Delete(ctx, storageKey)
			} else {
				_ = os.Remove(session.TempPath)
			}
			if session.ThumbnailTempPath != "" {
				_ = os.Remove(session.ThumbnailTempPath)
			}
		}

		if len(sessions) < uploadSessionCleanupBatch {
			break
		}
	}

	return firstErr
}

func (s *AttachmentService) GetAttachmentUploadSession(ctx context.Context, userID int64, uploadID string) (models.AttachmentUploadSession, error) {
	session, err := s.store.GetAttachmentUploadSessionByID(ctx, strings.TrimSpace(uploadID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.AttachmentUploadSession{}, ErrUploadSessionNotFound
		}
		return models.AttachmentUploadSession{}, err
	}
	if session.CreatorID != userID {
		return models.AttachmentUploadSession{}, sql.ErrNoRows
	}
	return session, nil
}

func (s *AttachmentService) GetDirectUploadSession(ctx context.Context, session models.AttachmentUploadSession) (*DirectUploadSession, error) {
	storageKey, ok := decodeDirectSessionPath(session.TempPath)
	if !ok {
		return nil, nil
	}
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
		return nil, nil
	}
	uploadURL, err := s3Store.PresignPutObjectURL(ctx, storageKey, session.Type, directUploadURLTTL)
	if err != nil {
		return nil, err
	}
	return &DirectUploadSession{
		UploadURL: uploadURL,
		Method:    "PUT",
	}, nil
}

func (s *AttachmentService) IsDirectUploadSession(session models.AttachmentUploadSession) bool {
	_, ok := decodeDirectSessionPath(session.TempPath)
	return ok
}

func (s *AttachmentService) GetMultipartUploadPartSession(session models.AttachmentUploadSession) (*MultipartUploadPartSession, error) {
	multipart, ok := decodeMultipartSessionPath(session.TempPath)
	if !ok {
		return nil, nil
	}
	if multipart.PartSize <= 0 {
		return nil, fmt.Errorf("invalid multipart upload session part size")
	}
	return &MultipartUploadPartSession{
		PartSize: multipart.PartSize,
	}, nil
}

func (s *AttachmentService) GetAttachmentUploadSessionProgress(ctx context.Context, session models.AttachmentUploadSession) (int64, error) {
	if multipart, ok := decodeMultipartSessionPath(session.TempPath); ok {
		parts, _, _, err := s.listContiguousMultipartParts(ctx, multipart)
		if err != nil {
			return 0, err
		}
		return sumUploadedPartSizes(parts), nil
	}
	return session.ReceivedSize, nil
}

func (s *AttachmentService) CreateMultipartPartUploadURL(
	ctx context.Context,
	session models.AttachmentUploadSession,
	expectedOffset int64,
	requestedPartNumber int32,
	requestedSize int64,
) (*MultipartPartUploadURL, error) {
	multipart, ok := decodeMultipartSessionPath(session.TempPath)
	if !ok {
		return nil, nil
	}
	if requestedPartNumber <= 0 {
		return nil, ErrMultipartPartInvalid
	}
	if requestedSize <= 0 {
		return nil, ErrMultipartPartInvalid
	}
	if multipart.PartSize <= 0 {
		return nil, ErrMultipartPartInvalid
	}
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
		return nil, fmt.Errorf("multipart upload session requires s3 storage")
	}

	parts, currentOffset, nextPartNumber, err := s.listContiguousMultipartParts(ctx, multipart)
	if err != nil {
		return nil, err
	}
	_ = parts
	if expectedOffset != currentOffset {
		return nil, &UploadOffsetMismatchError{CurrentOffset: currentOffset}
	}
	if requestedPartNumber != nextPartNumber {
		return nil, &UploadOffsetMismatchError{CurrentOffset: currentOffset}
	}

	remaining := session.Size - currentOffset
	if remaining <= 0 {
		return nil, ErrUploadNotComplete
	}
	maxPartSize := multipart.PartSize
	if maxPartSize > remaining {
		maxPartSize = remaining
	}
	if requestedSize > maxPartSize {
		return nil, ErrUploadExceedsTotalSize
	}

	uploadURL, err := s3Store.PresignUploadPartURL(
		ctx,
		multipart.StorageKey,
		multipart.MultipartUploadID,
		requestedPartNumber,
		multipartUploadURLTTL,
	)
	if err != nil {
		return nil, err
	}

	return &MultipartPartUploadURL{
		UploadURL:  uploadURL,
		Method:     "PUT",
		PartNumber: requestedPartNumber,
		Offset:     currentOffset,
		Size:       requestedSize,
	}, nil
}

func (s *AttachmentService) PresignAttachmentURL(ctx context.Context, attachment models.Attachment) (string, bool, error) {
	if !strings.EqualFold(strings.TrimSpace(attachment.StorageType), "S3") {
		return "", false, nil
	}
	s3Store, ok := s.storage.(*storage.S3Store)
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
	if !strings.EqualFold(strings.TrimSpace(attachment.ThumbnailStorageType), "S3") &&
		!strings.EqualFold(strings.TrimSpace(attachment.StorageType), "S3") {
		return "", false, nil
	}
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
		return "", false, nil
	}
	url, err := s3Store.PresignGetObjectURL(ctx, attachment.ThumbnailStorageKey, directDownloadURLTTL)
	if err != nil {
		return "", false, err
	}
	return url, true, nil
}

func (s *AttachmentService) AppendAttachmentUploadChunk(ctx context.Context, userID int64, uploadID string, expectedOffset int64, chunk []byte) (models.AttachmentUploadSession, error) {
	session, err := s.GetAttachmentUploadSession(ctx, userID, uploadID)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	if _, multipart := decodeMultipartSessionPath(session.TempPath); multipart {
		return models.AttachmentUploadSession{}, ErrUploadChunkUnsupported
	}
	if _, direct := decodeDirectSessionPath(session.TempPath); direct {
		return models.AttachmentUploadSession{}, ErrUploadChunkUnsupported
	}
	if expectedOffset != session.ReceivedSize {
		return models.AttachmentUploadSession{}, &UploadOffsetMismatchError{CurrentOffset: session.ReceivedSize}
	}

	remaining := session.Size - session.ReceivedSize
	if int64(len(chunk)) > remaining {
		return models.AttachmentUploadSession{}, ErrUploadExceedsTotalSize
	}

	file, err := os.OpenFile(session.TempPath, os.O_WRONLY, 0o644)
	if err != nil {
		return models.AttachmentUploadSession{}, fmt.Errorf("open upload temp file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(session.ReceivedSize, io.SeekStart); err != nil {
		return models.AttachmentUploadSession{}, fmt.Errorf("seek upload temp file: %w", err)
	}
	if _, err := file.Write(chunk); err != nil {
		return models.AttachmentUploadSession{}, fmt.Errorf("write upload chunk: %w", err)
	}

	newOffset := session.ReceivedSize + int64(len(chunk))
	if err := s.store.UpdateAttachmentUploadSessionOffset(ctx, session.ID, session.ReceivedSize, newOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			latest, latestErr := s.store.GetAttachmentUploadSessionByID(ctx, session.ID)
			if latestErr != nil {
				return models.AttachmentUploadSession{}, latestErr
			}
			return models.AttachmentUploadSession{}, &UploadOffsetMismatchError{CurrentOffset: latest.ReceivedSize}
		}
		return models.AttachmentUploadSession{}, err
	}
	return s.store.GetAttachmentUploadSessionByID(ctx, session.ID)
}

func (s *AttachmentService) CancelAttachmentUploadSession(ctx context.Context, userID int64, uploadID string) error {
	session, err := s.GetAttachmentUploadSession(ctx, userID, uploadID)
	if err != nil {
		return err
	}
	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return err
	}
	if multipart, ok := decodeMultipartSessionPath(session.TempPath); ok {
		if s3Store, s3OK := s.storage.(*storage.S3Store); s3OK {
			_ = s3Store.AbortMultipartUpload(ctx, multipart.StorageKey, multipart.MultipartUploadID)
		}
	} else if storageKey, direct := decodeDirectSessionPath(session.TempPath); direct {
		_ = s.storage.Delete(ctx, storageKey)
	} else {
		_ = os.Remove(session.TempPath)
	}
	if session.ThumbnailTempPath != "" {
		_ = os.Remove(session.ThumbnailTempPath)
	}
	return nil
}

func (s *AttachmentService) CompleteAttachmentUploadSession(ctx context.Context, userID int64, uploadID string) (models.Attachment, error) {
	session, err := s.GetAttachmentUploadSession(ctx, userID, uploadID)
	if err != nil {
		return models.Attachment{}, err
	}
	if multipart, ok := decodeMultipartSessionPath(session.TempPath); ok {
		return s.completeMultipartAttachmentUploadSession(ctx, userID, session, multipart)
	}
	if storageKey, direct := decodeDirectSessionPath(session.TempPath); direct {
		return s.completeDirectAttachmentUploadSession(ctx, userID, session, storageKey)
	}
	if session.ReceivedSize != session.Size {
		return models.Attachment{}, ErrUploadNotComplete
	}

	contentHash, err := hashFileSHA256(session.TempPath)
	if err != nil {
		return models.Attachment{}, err
	}

	existing, found, err := s.store.FindAttachmentByContentHash(ctx, userID, contentHash)
	if err != nil {
		return models.Attachment{}, err
	}

	var attachment models.Attachment
	if found {
		attachment, err = s.store.CreateAttachment(
			ctx,
			userID,
			session.Filename,
			"",
			session.Type,
			existing.Size,
			contentHash,
			existing.StorageType,
			existing.StorageKey,
		)
		if err != nil {
			return models.Attachment{}, err
		}
		if existing.ThumbnailStorageKey != "" && existing.ThumbnailSize > 0 {
			s.copyThumbnailMetadataFromExisting(ctx, attachment.ID, existing)
		} else if session.ThumbnailTempPath != "" {
			s.ensureThumbnailFromUploadSession(
				ctx,
				attachment,
				session.ThumbnailType,
				session.ThumbnailFilename,
				session.ThumbnailTempPath,
			)
		}
	} else {
		storageKey, err := s.newAttachmentStorageKey(ctx, userID, session.Filename)
		if err != nil {
			return models.Attachment{}, err
		}
		file, err := os.Open(session.TempPath)
		if err != nil {
			return models.Attachment{}, fmt.Errorf("open upload temp file: %w", err)
		}
		size, uploadErr := s.storage.PutStream(ctx, storageKey, session.Type, file, session.Size)
		_ = file.Close()
		if uploadErr != nil {
			return models.Attachment{}, uploadErr
		}
		attachment, err = s.store.CreateAttachment(
			ctx,
			userID,
			session.Filename,
			"",
			session.Type,
			size,
			contentHash,
			storageTypeName(s.storage),
			storageKey,
		)
		if err != nil {
			_ = s.storage.Delete(ctx, storageKey)
			return models.Attachment{}, err
		}
		if session.ThumbnailTempPath != "" {
			s.ensureThumbnailFromUploadSession(
				ctx,
				attachment,
				session.ThumbnailType,
				session.ThumbnailFilename,
				session.ThumbnailTempPath,
			)
		} else {
			s.ensureThumbnailFromFile(ctx, attachment, session.Type, session.Filename, session.TempPath)
		}
	}
	if refreshed, refreshErr := s.store.GetAttachmentByID(ctx, attachment.ID); refreshErr == nil {
		attachment = refreshed
	}

	if session.MemoName != nil {
		memoID, err := parseMemoID(*session.MemoName)
		if err != nil {
			return models.Attachment{}, err
		}
		if err := s.attachToMemo(ctx, memoID, attachment.ID); err != nil {
			return models.Attachment{}, err
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, err
	}
	_ = os.Remove(session.TempPath)
	if session.ThumbnailTempPath != "" {
		_ = os.Remove(session.ThumbnailTempPath)
	}
	return attachment, nil
}

func (s *AttachmentService) completeDirectAttachmentUploadSession(
	ctx context.Context,
	userID int64,
	session models.AttachmentUploadSession,
	storageKey string,
) (models.Attachment, error) {
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
		return models.Attachment{}, fmt.Errorf("direct upload session requires s3 storage")
	}

	size, err := s3Store.HeadSize(ctx, storageKey)
	if err != nil || size <= 0 {
		return models.Attachment{}, ErrUploadNotComplete
	}
	if size != session.Size {
		return models.Attachment{}, ErrUploadNotComplete
	}

	contentHash := hashDirectUploadReference(userID, session.ID, storageKey, size)
	attachment, err := s.store.CreateAttachment(
		ctx,
		userID,
		session.Filename,
		"",
		session.Type,
		size,
		contentHash,
		storageTypeName(s.storage),
		storageKey,
	)
	if err != nil {
		_ = s.storage.Delete(ctx, storageKey)
		return models.Attachment{}, err
	}
	if session.ThumbnailTempPath != "" {
		s.ensureThumbnailFromUploadSession(
			ctx,
			attachment,
			session.ThumbnailType,
			session.ThumbnailFilename,
			session.ThumbnailTempPath,
		)
	}
	if refreshed, refreshErr := s.store.GetAttachmentByID(ctx, attachment.ID); refreshErr == nil {
		attachment = refreshed
	}

	if session.MemoName != nil {
		memoID, err := parseMemoID(*session.MemoName)
		if err != nil {
			return models.Attachment{}, err
		}
		if err := s.attachToMemo(ctx, memoID, attachment.ID); err != nil {
			return models.Attachment{}, err
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, err
	}
	if session.ThumbnailTempPath != "" {
		_ = os.Remove(session.ThumbnailTempPath)
	}
	return attachment, nil
}

func (s *AttachmentService) completeMultipartAttachmentUploadSession(
	ctx context.Context,
	userID int64,
	session models.AttachmentUploadSession,
	multipart multipartSessionInfo,
) (models.Attachment, error) {
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
		return models.Attachment{}, fmt.Errorf("multipart upload session requires s3 storage")
	}

	parts, uploadedSize, _, err := s.listContiguousMultipartParts(ctx, multipart)
	if err != nil {
		return models.Attachment{}, err
	}
	if uploadedSize != session.Size {
		return models.Attachment{}, ErrUploadNotComplete
	}
	if err := s3Store.CompleteMultipartUpload(ctx, multipart.StorageKey, multipart.MultipartUploadID, parts); err != nil {
		return models.Attachment{}, err
	}

	contentHash := hashMultipartUploadReference(userID, session.ID, multipart.StorageKey, uploadedSize, parts)
	attachment, err := s.store.CreateAttachment(
		ctx,
		userID,
		session.Filename,
		"",
		session.Type,
		uploadedSize,
		contentHash,
		storageTypeName(s.storage),
		multipart.StorageKey,
	)
	if err != nil {
		_ = s.storage.Delete(ctx, multipart.StorageKey)
		return models.Attachment{}, err
	}

	if session.ThumbnailTempPath != "" {
		s.ensureThumbnailFromUploadSession(
			ctx,
			attachment,
			session.ThumbnailType,
			session.ThumbnailFilename,
			session.ThumbnailTempPath,
		)
	}
	if refreshed, refreshErr := s.store.GetAttachmentByID(ctx, attachment.ID); refreshErr == nil {
		attachment = refreshed
	}

	if session.MemoName != nil {
		memoID, err := parseMemoID(*session.MemoName)
		if err != nil {
			return models.Attachment{}, err
		}
		if err := s.attachToMemo(ctx, memoID, attachment.ID); err != nil {
			return models.Attachment{}, err
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, err
	}
	if session.ThumbnailTempPath != "" {
		_ = os.Remove(session.ThumbnailTempPath)
	}
	return attachment, nil
}

func (s *AttachmentService) ListAttachments(ctx context.Context, userID int64) ([]models.Attachment, error) {
	return s.store.ListAttachmentsByCreator(ctx, userID)
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
		if err := s.storage.Delete(ctx, attachment.StorageKey); err != nil {
			return err
		}
		if attachment.ThumbnailStorageKey != "" {
			_ = s.storage.Delete(ctx, attachment.ThumbnailStorageKey)
		}
	}
	return s.store.DeleteAttachment(ctx, attachmentID)
}

func (s *AttachmentService) GetAttachment(ctx context.Context, attachmentID int64) (models.Attachment, error) {
	return s.store.GetAttachmentByID(ctx, attachmentID)
}

func (s *AttachmentService) OpenAttachmentStream(ctx context.Context, attachment models.Attachment) (io.ReadCloser, error) {
	return s.storage.Open(ctx, attachment.StorageKey)
}

func (s *AttachmentService) OpenAttachmentRangeStream(ctx context.Context, attachment models.Attachment, start int64, end int64) (io.ReadCloser, error) {
	return s.storage.OpenRange(ctx, attachment.StorageKey, start, end)
}

func (s *AttachmentService) OpenAttachmentThumbnailStream(ctx context.Context, attachment models.Attachment) (io.ReadCloser, error) {
	if strings.TrimSpace(attachment.ThumbnailStorageKey) == "" {
		return nil, os.ErrNotExist
	}
	return s.storage.Open(ctx, attachment.ThumbnailStorageKey)
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

func parseMemoID(name string) (int64, error) {
	raw := strings.TrimSpace(name)
	if raw == "" {
		return 0, fmt.Errorf("invalid memo name")
	}
	raw = strings.SplitN(raw, "|", 2)[0]
	raw = strings.Trim(raw, "/")
	if idx := strings.LastIndex(raw, "/"); idx >= 0 {
		raw = raw[idx+1:]
	}
	if raw == "" {
		return 0, fmt.Errorf("invalid memo name")
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memo id")
	}
	return id, nil
}

func sanitizeFilename(filename string) string {
	filename = strings.TrimSpace(filename)
	filename = filepath.Base(filename)
	if filename == "." || filename == ".." {
		return ""
	}
	filename = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, filename)
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return ""
	}
	return filename
}

func hashAttachmentContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *AttachmentService) attachToMemo(ctx context.Context, memoID int64, attachmentID int64) error {
	attachedMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memoID})
	if err != nil {
		return err
	}
	attachmentIDs := make([]int64, 0, len(attachedMap[memoID])+1)
	seen := make(map[int64]struct{}, len(attachedMap[memoID])+1)
	for _, item := range attachedMap[memoID] {
		if _, ok := seen[item.ID]; ok {
			continue
		}
		attachmentIDs = append(attachmentIDs, item.ID)
		seen[item.ID] = struct{}{}
	}
	if _, ok := seen[attachmentID]; !ok {
		attachmentIDs = append(attachmentIDs, attachmentID)
	}
	return s.store.SetMemoAttachments(ctx, memoID, attachmentIDs)
}

func (s *AttachmentService) newAttachmentStorageKey(ctx context.Context, userID int64, filename string) (string, error) {
	for i := 0; i < attachmentStorageKeyTries; i++ {
		nanoID, err := generateNanoID(attachmentNanoIDLength)
		if err != nil {
			return "", err
		}
		key := buildAttachmentStorageKey(userID, nanoID, filename)
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

func buildAttachmentStorageKey(userID int64, nanoID string, filename string) string {
	return fmt.Sprintf("attachments/%d/%s_%s", userID, nanoID, filename)
}

func generateNanoID(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("invalid nano id length")
	}
	alphabet := attachmentNanoIDAlphabet
	buf := make([]byte, length)
	randBytes := make([]byte, length)
	if _, err := rand.Read(randBytes); err != nil {
		return "", fmt.Errorf("generate nano id: %w", err)
	}
	for i := 0; i < length; i++ {
		buf[i] = alphabet[int(randBytes[i])%len(alphabet)]
	}
	return string(buf), nil
}

func storageTypeName(s storage.Store) string {
	switch s.(type) {
	case *storage.S3Store:
		return "S3"
	default:
		return "LOCAL"
	}
}

func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open upload temp file for hash: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", fmt.Errorf("hash upload temp file: %w", err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func encodeDirectSessionPath(storageKey string) string {
	return directSessionPathPrefix + strings.TrimSpace(storageKey)
}

func encodeMultipartSessionPath(storageKey string, multipartUploadID string, partSize int64) string {
	encodedStorageKey := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(storageKey)))
	encodedMultipartUploadID := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(multipartUploadID)))
	return fmt.Sprintf(
		"%s%s.%s.%d",
		multipartSessionPathPrefix,
		encodedStorageKey,
		encodedMultipartUploadID,
		partSize,
	)
}

func decodeDirectSessionPath(tempPath string) (string, bool) {
	raw := strings.TrimSpace(tempPath)
	if !strings.HasPrefix(raw, directSessionPathPrefix) {
		return "", false
	}
	key := strings.TrimSpace(strings.TrimPrefix(raw, directSessionPathPrefix))
	if key == "" {
		return "", false
	}
	return key, true
}

func decodeMultipartSessionPath(tempPath string) (multipartSessionInfo, bool) {
	raw := strings.TrimSpace(tempPath)
	if !strings.HasPrefix(raw, multipartSessionPathPrefix) {
		return multipartSessionInfo{}, false
	}
	payload := strings.TrimSpace(strings.TrimPrefix(raw, multipartSessionPathPrefix))
	if payload == "" {
		return multipartSessionInfo{}, false
	}

	if decoded, ok := decodeMultipartSessionPathEncoded(payload); ok {
		return decoded, true
	}

	return decodeMultipartSessionPathLegacy(payload)
}

func decodeMultipartSessionPathEncoded(payload string) (multipartSessionInfo, bool) {
	parts := strings.Split(payload, ".")
	if len(parts) != 3 {
		return multipartSessionInfo{}, false
	}
	storageKeyBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(parts[0]))
	if err != nil {
		return multipartSessionInfo{}, false
	}
	multipartUploadIDBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(parts[1]))
	if err != nil {
		return multipartSessionInfo{}, false
	}
	storageKey := strings.TrimSpace(string(storageKeyBytes))
	multipartUploadID := strings.TrimSpace(string(multipartUploadIDBytes))
	partSize, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil || partSize <= 0 {
		return multipartSessionInfo{}, false
	}
	if storageKey == "" || multipartUploadID == "" {
		return multipartSessionInfo{}, false
	}
	return multipartSessionInfo{
		StorageKey:        storageKey,
		MultipartUploadID: multipartUploadID,
		PartSize:          partSize,
	}, true
}

func decodeMultipartSessionPathLegacy(payload string) (multipartSessionInfo, bool) {
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		return multipartSessionInfo{}, false
	}
	storageKey := strings.TrimSpace(parts[0])
	multipartUploadID := strings.TrimSpace(parts[1])
	partSize, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil || partSize <= 0 {
		return multipartSessionInfo{}, false
	}
	if storageKey == "" || multipartUploadID == "" {
		return multipartSessionInfo{}, false
	}
	return multipartSessionInfo{
		StorageKey:        storageKey,
		MultipartUploadID: multipartUploadID,
		PartSize:          partSize,
	}, true
}

func hashDirectUploadReference(userID int64, uploadID string, storageKey string, size int64) string {
	raw := fmt.Sprintf("s3-direct|%d|%s|%s|%d", userID, uploadID, storageKey, size)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func hashMultipartUploadReference(
	userID int64,
	uploadID string,
	storageKey string,
	size int64,
	parts []storage.S3UploadedPart,
) string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("s3-multipart|%d|%s|%s|%d", userID, uploadID, storageKey, size))
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("|%d:%d:%s", part.PartNumber, part.Size, part.ETag))
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

func (s *AttachmentService) listContiguousMultipartParts(
	ctx context.Context,
	info multipartSessionInfo,
) ([]storage.S3UploadedPart, int64, int32, error) {
	s3Store, ok := s.storage.(*storage.S3Store)
	if !ok {
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

func sumUploadedPartSizes(parts []storage.S3UploadedPart) int64 {
	var total int64
	for _, part := range parts {
		total += part.Size
	}
	return total
}
