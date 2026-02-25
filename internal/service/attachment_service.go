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
	attachmentNanoIDLength    = 8
	attachmentStorageKeyTries = 8
	attachmentNanoIDAlphabet  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	uploadSessionTTL          = 24 * time.Hour
	uploadSessionCleanupBatch = 200
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
)

type UploadOffsetMismatchError struct {
	CurrentOffset int64
}

func (e *UploadOffsetMismatchError) Error() string {
	return fmt.Sprintf("upload offset mismatch current=%d", e.CurrentOffset)
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

	if err := os.MkdirAll(s.tempDir, 0o755); err != nil {
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp dir: %w", err)
	}
	uploadID, err := generateNanoID(24)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	tempPath := filepath.Join(s.tempDir, uploadID+".part")
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp file: %w", err)
	}
	_ = tempFile.Close()

	thumbnailTempPath := ""
	if len(thumbnailData) > 0 {
		thumbnailTempPath = filepath.Join(s.tempDir, uploadID+".thumb")
		if err := os.WriteFile(thumbnailTempPath, thumbnailData, 0o644); err != nil {
			_ = os.Remove(tempPath)
			return models.AttachmentUploadSession{}, fmt.Errorf("create upload thumbnail temp file: %w", err)
		}
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
			_ = os.Remove(session.TempPath)
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

func (s *AttachmentService) AppendAttachmentUploadChunk(ctx context.Context, userID int64, uploadID string, expectedOffset int64, chunk []byte) (models.AttachmentUploadSession, error) {
	session, err := s.GetAttachmentUploadSession(ctx, userID, uploadID)
	if err != nil {
		return models.AttachmentUploadSession{}, err
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
	_ = os.Remove(session.TempPath)
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
