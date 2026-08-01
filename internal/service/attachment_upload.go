package service

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
		if err := os.MkdirAll(s.tempDir, 0o750); err != nil {
			return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp dir: %w", err)
		}
		thumbnailTempPath = filepath.Join(s.tempDir, uploadID+".thumb")
		if err := os.WriteFile(thumbnailTempPath, thumbnailData, 0o600); err != nil {
			return models.AttachmentUploadSession{}, fmt.Errorf("create upload thumbnail temp file: %w", err)
		}
	}

	if s3Store := s.defaultS3Store(); s3Store != nil {
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
			ID:                 uploadID,
			CreatorID:          userID,
			Filename:           filename,
			Type:               contentType,
			Size:               input.Size,
			EncryptionMetadata: strings.TrimSpace(input.EncryptionMetadata),
			MemoName:           memoName,
			TempPath:           tempPath,
			ThumbnailFilename:  thumbnailFilename,
			ThumbnailType:      thumbnailType,
			ThumbnailTempPath:  thumbnailTempPath,
			ReceivedSize:       0,
			CreateTime:         now,
			UpdateTime:         now,
		})
		if err != nil {
			if thumbnailTempPath != "" {
				_ = os.Remove(thumbnailTempPath)
			}
			return models.AttachmentUploadSession{}, err
		}
		return session, nil
	}

	if err := os.MkdirAll(s.tempDir, 0o750); err != nil {
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp dir: %w", err)
	}
	tempPath := filepath.Join(s.tempDir, uploadID+".part")
	tempRoot, err := os.OpenRoot(s.tempDir)
	if err != nil {
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, fmt.Errorf("open upload temp dir root: %w", err)
	}
	tempFile, err := tempRoot.OpenFile(uploadID+".part", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		_ = tempRoot.Close()
		if thumbnailTempPath != "" {
			_ = os.Remove(thumbnailTempPath)
		}
		return models.AttachmentUploadSession{}, fmt.Errorf("create upload temp file: %w", err)
	}
	_ = tempFile.Close()
	_ = tempRoot.Close()

	now := time.Now().UTC()
	session, err := s.store.CreateAttachmentUploadSession(ctx, models.AttachmentUploadSession{
		ID:                 uploadID,
		CreatorID:          userID,
		Filename:           filename,
		Type:               contentType,
		Size:               input.Size,
		EncryptionMetadata: strings.TrimSpace(input.EncryptionMetadata),
		MemoName:           memoName,
		TempPath:           tempPath,
		ThumbnailFilename:  thumbnailFilename,
		ThumbnailType:      thumbnailType,
		ThumbnailTempPath:  thumbnailTempPath,
		ReceivedSize:       0,
		CreateTime:         now,
		UpdateTime:         now,
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
				if s3Store := s.defaultS3Store(); s3Store != nil {
					_ = s3Store.AbortMultipartUpload(ctx, multipart.StorageKey, multipart.MultipartUploadID)
				}
			} else if storageKey, direct := decodeDirectSessionPath(session.TempPath); direct {
				if store := s.defaultStorage(); store != nil {
					_ = store.Delete(ctx, storageKey)
				}
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
	s3Store := s.defaultS3Store()
	if s3Store == nil {
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
	s3Store := s.defaultS3Store()
	if s3Store == nil {
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

	file, err := os.OpenFile(session.TempPath, os.O_WRONLY, 0o600)
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
		if s3Store := s.defaultS3Store(); s3Store != nil {
			_ = s3Store.AbortMultipartUpload(ctx, multipart.StorageKey, multipart.MultipartUploadID)
		}
	} else if storageKey, direct := decodeDirectSessionPath(session.TempPath); direct {
		if store := s.defaultStorage(); store != nil {
			_ = store.Delete(ctx, storageKey)
		}
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

	contentHash, err := s.hashTempFileSHA256(session.TempPath)
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
			session.EncryptionMetadata,
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
		file, tempRoot, err := s.openTempFileForRead(session.TempPath)
		if err != nil {
			return models.Attachment{}, fmt.Errorf("open upload temp file: %w", err)
		}
		defaultStorage := s.defaultStorage()
		if defaultStorage == nil {
			_ = file.Close()
			_ = tempRoot.Close()
			return models.Attachment{}, fmt.Errorf("attachment storage is not configured")
		}
		size, uploadErr := defaultStorage.PutStream(ctx, storageKey, session.Type, file, session.Size)
		_ = file.Close()
		_ = tempRoot.Close()
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
			session.EncryptionMetadata,
			s.defaultStorageType(),
			storageKey,
		)
		if err != nil {
			_ = defaultStorage.Delete(ctx, storageKey)
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
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
		if err := s.attachToMemo(ctx, memoID, attachment); err != nil {
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
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
	s3Store := s.defaultS3Store()
	if s3Store == nil {
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
		session.EncryptionMetadata,
		s.defaultStorageType(),
		storageKey,
	)
	if err != nil {
		_ = s3Store.Delete(ctx, storageKey)
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
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
		if err := s.attachToMemo(ctx, memoID, attachment); err != nil {
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
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
	s3Store := s.defaultS3Store()
	if s3Store == nil {
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
		session.EncryptionMetadata,
		s.defaultStorageType(),
		multipart.StorageKey,
	)
	if err != nil {
		_ = s3Store.Delete(ctx, multipart.StorageKey)
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
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
		if err := s.attachToMemo(ctx, memoID, attachment); err != nil {
			return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
		}
	}

	if err := s.store.DeleteAttachmentUploadSessionByID(ctx, session.ID); err != nil {
		return models.Attachment{}, s.rollbackCreatedAttachment(ctx, userID, attachment.ID, err)
	}
	if session.ThumbnailTempPath != "" {
		_ = os.Remove(session.ThumbnailTempPath)
	}
	return attachment, nil
}

func (s *AttachmentService) rollbackCreatedAttachment(ctx context.Context, userID int64, attachmentID int64, cause error) error {
	if attachmentID <= 0 || cause == nil {
		return cause
	}
	if cleanupErr := s.DeleteAttachment(ctx, userID, attachmentID); cleanupErr != nil && !errors.Is(cleanupErr, sql.ErrNoRows) {
		return fmt.Errorf("%w (rollback attachment cleanup failed: %v)", cause, cleanupErr)
	}
	return cause
}
