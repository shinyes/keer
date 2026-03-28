package service

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type memoryCleanupStore struct {
	storeType string
	objects   map[string][]byte
}

func newMemoryCleanupStore(storeType string) *memoryCleanupStore {
	return &memoryCleanupStore{
		storeType: storeType,
		objects:   make(map[string][]byte),
	}
}

func (s *memoryCleanupStore) Put(_ context.Context, key string, _ string, data []byte) (int64, error) {
	copied := make([]byte, len(data))
	copy(copied, data)
	s.objects[key] = copied
	return int64(len(copied)), nil
}

func (s *memoryCleanupStore) PutStream(_ context.Context, key string, _ string, reader io.Reader, _ int64) (int64, error) {
	payload, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	return s.Put(context.Background(), key, "", payload)
}

func (s *memoryCleanupStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	payload, ok := s.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (s *memoryCleanupStore) OpenRange(ctx context.Context, key string, _, _ int64) (io.ReadCloser, error) {
	return s.Open(ctx, key)
}

func (s *memoryCleanupStore) Delete(_ context.Context, key string) error {
	delete(s.objects, key)
	return nil
}

func (s *memoryCleanupStore) ListKeys(_ context.Context, prefix string) ([]string, error) {
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if prefix == "" || strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (s *memoryCleanupStore) Type() string {
	return s.storeType
}

func TestCleanupOrphanFiles_LocalStoreRemovesOnlyUnreferencedManagedKeys(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, storage.NewRouter(storage.TypeLocal, localStore))
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "cleanup-local-user")

	referencedKey := "attachments/1/referenced.bin"
	thumbnailKey := "attachments/1/referenced.thumb.bin"
	avatarKey := avatarStorageKey(user.ID)
	orphanAttachmentKey := "attachments/1/orphan.bin"
	orphanThumbnailKey := "attachments/1/orphan.thumb.bin"
	orphanAvatarKey := "avatars/orphan.bin"

	referencedAttachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"referenced.bin",
		"",
		"application/octet-stream",
		4,
		"hash-local",
		"",
		storage.TypeLocal,
		referencedKey,
	)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	if err := services.store.UpdateAttachmentThumbnail(
		ctx,
		referencedAttachment.ID,
		"referenced.thumb.bin",
		"image/jpeg",
		4,
		storage.TypeLocal,
		thumbnailKey,
	); err != nil {
		t.Fatalf("UpdateAttachmentThumbnail() error = %v", err)
	}
	memo, err := services.store.CreateMemo(
		ctx,
		user.ID,
		"memo-content",
		"",
		models.VisibilityPrivate,
		models.MemoStateNormal,
		false,
		models.MemoPayload{},
		nowUTC(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}
	if err := services.store.SetMemoAttachments(ctx, memo.ID, []store.AttachmentBinding{
		{AttachmentID: referencedAttachment.ID},
	}); err != nil {
		t.Fatalf("SetMemoAttachments() error = %v", err)
	}

	orphanAttachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"orphan.bin",
		"",
		"application/octet-stream",
		4,
		"hash-orphan-local",
		"",
		storage.TypeLocal,
		orphanAttachmentKey,
	)
	if err != nil {
		t.Fatalf("CreateAttachment() orphan error = %v", err)
	}
	if err := services.store.UpdateAttachmentThumbnail(
		ctx,
		orphanAttachment.ID,
		"orphan.thumb.bin",
		"image/jpeg",
		4,
		storage.TypeLocal,
		orphanThumbnailKey,
	); err != nil {
		t.Fatalf("UpdateAttachmentThumbnail() orphan error = %v", err)
	}
	if _, err := services.store.UpdateUserAvatar(ctx, user.ID, avatarPublicURL(user.ID), storage.TypeLocal); err != nil {
		t.Fatalf("UpdateUserAvatar() error = %v", err)
	}

	if _, err := localStore.Put(ctx, referencedKey, "application/octet-stream", []byte("main")); err != nil {
		t.Fatalf("Put referenced attachment error = %v", err)
	}
	if _, err := localStore.Put(ctx, thumbnailKey, "image/jpeg", []byte("thumb")); err != nil {
		t.Fatalf("Put referenced thumbnail error = %v", err)
	}
	if _, err := localStore.Put(ctx, avatarKey, "image/png", []byte("avatar")); err != nil {
		t.Fatalf("Put referenced avatar error = %v", err)
	}
	if _, err := localStore.Put(ctx, orphanAttachmentKey, "application/octet-stream", []byte("orphan")); err != nil {
		t.Fatalf("Put orphan attachment error = %v", err)
	}
	if _, err := localStore.Put(ctx, orphanThumbnailKey, "image/jpeg", []byte("orphan-thumb")); err != nil {
		t.Fatalf("Put orphan thumbnail error = %v", err)
	}
	if _, err := localStore.Put(ctx, orphanAvatarKey, "image/png", []byte("orphan")); err != nil {
		t.Fatalf("Put orphan avatar error = %v", err)
	}
	if _, err := localStore.Put(ctx, "misc/not-managed.txt", "text/plain", []byte("keep")); err != nil {
		t.Fatalf("Put unmanaged key error = %v", err)
	}

	result, err := attachmentService.CleanupOrphanFiles(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphanFiles() error = %v", err)
	}
	if result.ScannedKeys != 6 {
		t.Fatalf("expected scanned=6, got %d", result.ScannedKeys)
	}
	if result.DeletedKeys != 3 {
		t.Fatalf("expected deleted=3, got %d", result.DeletedKeys)
	}
	if result.FailedKeys != 0 {
		t.Fatalf("expected failed=0, got %d", result.FailedKeys)
	}

	assertStoreHasKey(t, localStore, referencedKey)
	assertStoreHasKey(t, localStore, thumbnailKey)
	assertStoreHasKey(t, localStore, avatarKey)
	assertStoreMissingKey(t, localStore, orphanAttachmentKey)
	assertStoreMissingKey(t, localStore, orphanThumbnailKey)
	assertStoreMissingKey(t, localStore, orphanAvatarKey)
	assertStoreHasKey(t, localStore, "misc/not-managed.txt")

	if _, err := services.store.GetAttachmentByID(ctx, orphanAttachment.ID); err != sql.ErrNoRows {
		t.Fatalf("expected orphan attachment row deleted, got err=%v", err)
	}
	if _, err := services.store.GetAttachmentByID(ctx, referencedAttachment.ID); err != nil {
		t.Fatalf("expected referenced attachment row retained, got err=%v", err)
	}
}

func TestCleanupOrphanFiles_MixedStoresPreservesReferencedKeysAcrossBackends(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	s3Store := newMemoryCleanupStore(storage.TypeS3)
	attachmentService := NewAttachmentService(
		services.store,
		storage.NewRouter(storage.TypeLocal, localStore, s3Store),
	)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "cleanup-mixed-user")

	localAttachmentKey := "attachments/1/local.bin"
	s3ThumbnailKey := "attachments/1/local.thumb.bin"
	s3AvatarKey := avatarStorageKey(user.ID)
	localOrphanKey := "attachments/1/orphan-local.bin"
	s3OrphanThumbnailKey := "attachments/1/orphan-s3-thumb.bin"
	s3OrphanKey := "avatars/orphan-s3.bin"

	referencedAttachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"local.bin",
		"",
		"application/octet-stream",
		4,
		"hash-mixed",
		"",
		storage.TypeLocal,
		localAttachmentKey,
	)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	if err := services.store.UpdateAttachmentThumbnail(
		ctx,
		referencedAttachment.ID,
		"local.thumb.bin",
		"image/jpeg",
		4,
		storage.TypeS3,
		s3ThumbnailKey,
	); err != nil {
		t.Fatalf("UpdateAttachmentThumbnail() error = %v", err)
	}
	memo, err := services.store.CreateMemo(
		ctx,
		user.ID,
		"memo-content",
		"",
		models.VisibilityPrivate,
		models.MemoStateNormal,
		false,
		models.MemoPayload{},
		nowUTC(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}
	if err := services.store.SetMemoAttachments(ctx, memo.ID, []store.AttachmentBinding{
		{AttachmentID: referencedAttachment.ID},
	}); err != nil {
		t.Fatalf("SetMemoAttachments() error = %v", err)
	}

	orphanAttachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"orphan-local.bin",
		"",
		"application/octet-stream",
		4,
		"hash-mixed-orphan",
		"",
		storage.TypeLocal,
		localOrphanKey,
	)
	if err != nil {
		t.Fatalf("CreateAttachment() orphan error = %v", err)
	}
	if err := services.store.UpdateAttachmentThumbnail(
		ctx,
		orphanAttachment.ID,
		"orphan-s3-thumb.bin",
		"image/jpeg",
		4,
		storage.TypeS3,
		s3OrphanThumbnailKey,
	); err != nil {
		t.Fatalf("UpdateAttachmentThumbnail() orphan error = %v", err)
	}
	if _, err := services.store.UpdateUserAvatar(ctx, user.ID, avatarPublicURL(user.ID), storage.TypeS3); err != nil {
		t.Fatalf("UpdateUserAvatar() error = %v", err)
	}

	if _, err := localStore.Put(ctx, localAttachmentKey, "application/octet-stream", []byte("main")); err != nil {
		t.Fatalf("Put local referenced attachment error = %v", err)
	}
	if _, err := localStore.Put(ctx, localOrphanKey, "application/octet-stream", []byte("orphan")); err != nil {
		t.Fatalf("Put local orphan error = %v", err)
	}
	if _, err := s3Store.Put(ctx, s3ThumbnailKey, "image/jpeg", []byte("thumb")); err != nil {
		t.Fatalf("Put s3 referenced thumbnail error = %v", err)
	}
	if _, err := s3Store.Put(ctx, s3AvatarKey, "image/png", []byte("avatar")); err != nil {
		t.Fatalf("Put s3 referenced avatar error = %v", err)
	}
	if _, err := s3Store.Put(ctx, s3OrphanThumbnailKey, "image/jpeg", []byte("orphan-thumb")); err != nil {
		t.Fatalf("Put s3 orphan thumbnail error = %v", err)
	}
	if _, err := s3Store.Put(ctx, s3OrphanKey, "image/png", []byte("orphan")); err != nil {
		t.Fatalf("Put s3 orphan error = %v", err)
	}

	result, err := attachmentService.CleanupOrphanFiles(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphanFiles() error = %v", err)
	}
	if result.ScannedKeys != 6 {
		t.Fatalf("expected scanned=6, got %d", result.ScannedKeys)
	}
	if result.DeletedKeys != 3 {
		t.Fatalf("expected deleted=3, got %d", result.DeletedKeys)
	}
	if result.FailedKeys != 0 {
		t.Fatalf("expected failed=0, got %d", result.FailedKeys)
	}

	assertStoreHasKey(t, localStore, localAttachmentKey)
	assertStoreMissingKey(t, localStore, localOrphanKey)
	assertStoreHasKey(t, s3Store, s3ThumbnailKey)
	assertStoreHasKey(t, s3Store, s3AvatarKey)
	assertStoreMissingKey(t, s3Store, s3OrphanThumbnailKey)
	assertStoreMissingKey(t, s3Store, s3OrphanKey)

	if _, err := services.store.GetAttachmentByID(ctx, orphanAttachment.ID); err != sql.ErrNoRows {
		t.Fatalf("expected orphan attachment row deleted, got err=%v", err)
	}
	if _, err := services.store.GetAttachmentByID(ctx, referencedAttachment.ID); err != nil {
		t.Fatalf("expected referenced attachment row retained, got err=%v", err)
	}
}

func TestCleanupOrphanFiles_PreservesGroupReferencedAttachment(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, storage.NewRouter(storage.TypeLocal, localStore))
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "cleanup-group-user")

	groupAttachmentKey := "attachments/1/group.bin"
	groupThumbnailKey := "attachments/1/group.thumb.bin"
	groupAttachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"group.bin",
		"",
		"application/octet-stream",
		4,
		"hash-group",
		"",
		storage.TypeLocal,
		groupAttachmentKey,
	)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	if err := services.store.UpdateAttachmentThumbnail(
		ctx,
		groupAttachment.ID,
		"group.thumb.bin",
		"image/jpeg",
		4,
		storage.TypeLocal,
		groupThumbnailKey,
	); err != nil {
		t.Fatalf("UpdateAttachmentThumbnail() error = %v", err)
	}

	group, err := services.store.CreateGroup(ctx, user.ID, "cleanup-group", "desc")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := services.store.CreateGroupMessage(
		ctx,
		group.ID,
		user.ID,
		"hello",
		"",
		nil,
		[]store.AttachmentBinding{{AttachmentID: groupAttachment.ID}},
	); err != nil {
		t.Fatalf("CreateGroupMessage() error = %v", err)
	}

	if _, err := localStore.Put(ctx, groupAttachmentKey, "application/octet-stream", []byte("group")); err != nil {
		t.Fatalf("Put group attachment error = %v", err)
	}
	if _, err := localStore.Put(ctx, groupThumbnailKey, "image/jpeg", []byte("group-thumb")); err != nil {
		t.Fatalf("Put group thumbnail error = %v", err)
	}

	result, err := attachmentService.CleanupOrphanFiles(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphanFiles() error = %v", err)
	}
	if result.DeletedKeys != 0 {
		t.Fatalf("expected no deleted keys for group referenced attachment, got %d", result.DeletedKeys)
	}
	assertStoreHasKey(t, localStore, groupAttachmentKey)
	assertStoreHasKey(t, localStore, groupThumbnailKey)
	if _, err := services.store.GetAttachmentByID(ctx, groupAttachment.ID); err != nil {
		t.Fatalf("expected group referenced attachment row retained, got err=%v", err)
	}
}

func nowUTC() time.Time {
	return time.Now().UTC()
}

func assertStoreHasKey(t *testing.T, store storage.Store, key string) {
	t.Helper()
	reader, err := store.Open(context.Background(), key)
	if err != nil {
		t.Fatalf("expected store key %q to exist, got error %v", key, err)
	}
	_ = reader.Close()
}

func assertStoreMissingKey(t *testing.T, store storage.Store, key string) {
	t.Helper()
	reader, err := store.Open(context.Background(), key)
	if err == nil {
		_ = reader.Close()
		t.Fatalf("expected store key %q to be missing", key)
	}
}
