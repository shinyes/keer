package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type AttachmentService struct {
	store   *store.SQLStore
	storage storage.Store
}

const (
	attachmentNanoIDLength    = 8
	attachmentStorageKeyTries = 8
	attachmentNanoIDAlphabet  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

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

	if memoID != nil {
		if err := s.attachToMemo(ctx, *memoID, attachment.ID); err != nil {
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

	refCount, err := s.store.CountAttachmentsByStorageKey(ctx, attachment.StorageKey)
	if err != nil {
		return err
	}
	if refCount <= 1 {
		if err := s.storage.Delete(ctx, attachment.StorageKey); err != nil {
			return err
		}
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
