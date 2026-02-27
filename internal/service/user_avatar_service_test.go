package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"testing"

	"github.com/shinyes/keer/internal/storage"
)

type memoryAvatarStore struct {
	putErr    error
	deleteErr error
	objects   map[string][]byte
}

func newMemoryAvatarStore() *memoryAvatarStore {
	return &memoryAvatarStore{
		objects: make(map[string][]byte),
	}
}

func (s *memoryAvatarStore) Put(_ context.Context, key string, _ string, data []byte) (int64, error) {
	if s.putErr != nil {
		return 0, s.putErr
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	s.objects[key] = copied
	return int64(len(copied)), nil
}

func (s *memoryAvatarStore) PutStream(_ context.Context, key string, _ string, reader io.Reader, _ int64) (int64, error) {
	payload, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	return s.Put(context.Background(), key, "", payload)
}

func (s *memoryAvatarStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	payload, ok := s.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (s *memoryAvatarStore) OpenRange(ctx context.Context, key string, _, _ int64) (io.ReadCloser, error) {
	return s.Open(ctx, key)
}

func (s *memoryAvatarStore) Delete(_ context.Context, key string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.objects, key)
	return nil
}

var _ storage.Store = (*memoryAvatarStore)(nil)

func TestUpdateUserAvatarThumbnail_StoresAvatarToDedicatedPath(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	avatarStore := newMemoryAvatarStore()
	userService.SetAvatarStorage(avatarStore)
	ctx := context.Background()

	user, err := services.store.CreateUser(ctx, "avatarcase01", "avatarcase01", "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	content := encodeBase64(makePNG(t, 256, 256))
	updated, err := userService.UpdateUserAvatarThumbnail(ctx, user.ID, content, "image/png")
	if err != nil {
		t.Fatalf("UpdateUserAvatarThumbnail() error = %v", err)
	}

	key := avatarStorageKey(user.ID)
	if _, ok := avatarStore.objects[key]; !ok {
		t.Fatalf("expected avatar stored at key %q", key)
	}
	if updated.AvatarURL != avatarPublicURL(user.ID) {
		t.Fatalf("unexpected avatar url: %q", updated.AvatarURL)
	}
}

func TestUpdateUserAvatarThumbnail_WriteFailureDoesNotUpdateAvatarURL(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	avatarStore := newMemoryAvatarStore()
	avatarStore.putErr = errors.New("disk full")
	userService.SetAvatarStorage(avatarStore)
	ctx := context.Background()

	user, err := services.store.CreateUser(ctx, "avatarcase02", "avatarcase02", "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	content := encodeBase64(makePNG(t, 128, 128))
	if _, err := userService.UpdateUserAvatarThumbnail(ctx, user.ID, content, "image/png"); err == nil {
		t.Fatalf("expected update failure when avatar store put fails")
	}

	refreshed, err := services.store.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("GetUserByID() error = %v", err)
	}
	if refreshed.AvatarURL != "" {
		t.Fatalf("expected avatar_url unchanged on write failure, got %q", refreshed.AvatarURL)
	}
}

func TestUpdateUserAvatarThumbnail_RejectsLargeDimensions(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	avatarStore := newMemoryAvatarStore()
	userService.SetAvatarStorage(avatarStore)
	ctx := context.Background()

	user, err := services.store.CreateUser(ctx, "avatarcase03", "avatarcase03", "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	content := encodeBase64(makePNG(t, avatarMaxDimension+1, 16))
	if _, err := userService.UpdateUserAvatarThumbnail(ctx, user.ID, content, "image/png"); err == nil {
		t.Fatalf("expected validation error for oversized dimensions")
	}
	if len(avatarStore.objects) != 0 {
		t.Fatalf("expected no avatar file written when validation fails")
	}
}

func TestClearUserAvatar_DeleteFailureDoesNotUpdateAvatarURL(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	avatarStore := newMemoryAvatarStore()
	userService.SetAvatarStorage(avatarStore)
	ctx := context.Background()

	user, err := services.store.CreateUser(ctx, "avatarcase04", "avatarcase04", "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if _, err := services.store.UpdateUserAvatar(ctx, user.ID, avatarPublicURL(user.ID)); err != nil {
		t.Fatalf("UpdateUserAvatar() error = %v", err)
	}
	avatarStore.deleteErr = errors.New("delete failed")

	if _, err := userService.ClearUserAvatar(ctx, user.ID); err == nil {
		t.Fatalf("expected clear failure when avatar deletion fails")
	}
	refreshed, err := services.store.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("GetUserByID() error = %v", err)
	}
	if refreshed.AvatarURL == "" {
		t.Fatalf("expected avatar_url unchanged when delete fails")
	}
}

func makePNG(t *testing.T, width int, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: 12, G: 120, B: 230, A: 255})
		}
	}
	var buffer bytes.Buffer
	if err := png.Encode(&buffer, img); err != nil {
		t.Fatalf("png encode failed: %v", err)
	}
	return buffer.Bytes()
}

func encodeBase64(payload []byte) string {
	return base64.StdEncoding.EncodeToString(payload)
}
