package service

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
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
}

func NewAttachmentService(s *store.SQLStore, fileStorage storage.Store) *AttachmentService {
	return &AttachmentService{
		store:   s,
		storage: fileStorage,
	}
}

type CreateAttachmentInput struct {
	Filename string
	Type     string
	Content  string
	MemoName *string
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

	storageKey := fmt.Sprintf("attachments/%d/%d/%s", userID, time.Now().UTC().UnixNano(), filename)
	size, err := s.storage.Put(ctx, storageKey, contentType, data)
	if err != nil {
		return models.Attachment{}, err
	}

	attachment, err := s.store.CreateAttachment(
		ctx,
		userID,
		filename,
		"",
		contentType,
		size,
		storageTypeName(s.storage),
		storageKey,
	)
	if err != nil {
		_ = s.storage.Delete(ctx, storageKey)
		return models.Attachment{}, err
	}

	if memoID != nil {
		attachedMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{*memoID})
		if err != nil {
			return models.Attachment{}, err
		}
		attachmentIDs := make([]int64, 0, len(attachedMap[*memoID])+1)
		for _, item := range attachedMap[*memoID] {
			attachmentIDs = append(attachmentIDs, item.ID)
		}
		attachmentIDs = append(attachmentIDs, attachment.ID)
		if err := s.store.SetMemoAttachments(ctx, *memoID, attachmentIDs); err != nil {
			return models.Attachment{}, err
		}
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
	if err := s.storage.Delete(ctx, attachment.StorageKey); err != nil {
		return err
	}
	return s.store.DeleteAttachment(ctx, attachmentID)
}

func (s *AttachmentService) OpenAttachment(ctx context.Context, attachmentID int64) (models.Attachment, io.ReadCloser, error) {
	attachment, err := s.store.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return models.Attachment{}, nil, err
	}
	rc, err := s.storage.Open(ctx, attachment.StorageKey)
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
	if filename == "." {
		return ""
	}
	return filename
}

func storageTypeName(s storage.Store) string {
	switch s.(type) {
	case *storage.S3Store:
		return "S3"
	default:
		return "LOCAL"
	}
}
