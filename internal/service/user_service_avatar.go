package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"image"
	"io"
	"net/http"
	"strings"
	"sync"
)

func (s *UserService) UpdateUserAvatar(ctx context.Context, userID int64, avatarURL string) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		return s.store.UpdateUserAvatar(ctx, userID, strings.TrimSpace(avatarURL), "")
	})
}

func (s *UserService) UpdateUserAvatarThumbnail(ctx context.Context, userID int64, contentBase64 string, declaredType string) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		store := s.defaultAvatarStore()
		if store == nil {
			return models.User{}, fmt.Errorf("avatar storage is not configured")
		}
		content, err := decodeBase64Payload(contentBase64)
		if err != nil {
			return models.User{}, fmt.Errorf("invalid avatar content: %w", err)
		}
		if err := validateAvatarImage(content, declaredType); err != nil {
			return models.User{}, err
		}

		thumbnailData, err := buildThumbnailJPEG(bytes.NewReader(content))
		if err != nil || len(thumbnailData) == 0 {
			return models.User{}, fmt.Errorf("invalid avatar image")
		}

		if _, err := store.Put(ctx, avatarStorageKey(userID), thumbnailContentType, thumbnailData); err != nil {
			return models.User{}, fmt.Errorf("store avatar: %w", err)
		}
		return s.store.UpdateUserAvatar(ctx, userID, avatarPublicURL(userID), s.defaultAvatarStorageType())
	})
}

func (s *UserService) ClearUserAvatar(ctx context.Context, userID int64) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		user, err := s.store.GetUserByID(ctx, userID)
		if err != nil {
			return models.User{}, err
		}
		if store, ok := s.avatarStoreForType(user.AvatarStorageType); ok {
			if err := store.Delete(ctx, avatarStorageKey(userID)); err != nil {
				return models.User{}, fmt.Errorf("delete avatar: %w", err)
			}
		}
		return s.store.UpdateUserAvatar(ctx, userID, "", "")
	})
}

func (s *UserService) OpenUserAvatarStream(ctx context.Context, userID int64) (io.ReadCloser, error) {
	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	store, ok := s.avatarStoreForType(user.AvatarStorageType)
	if !ok {
		return nil, fmt.Errorf("avatar storage is not configured")
	}
	return store.Open(ctx, avatarStorageKey(userID))
}

func (s *UserService) PresignUserAvatarURL(ctx context.Context, userID int64) (string, bool, error) {
	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return "", false, err
	}
	store, ok := s.avatarStoreForType(user.AvatarStorageType)
	if !ok {
		return "", false, nil
	}
	s3Store, ok := store.(*storage.S3Store)
	if !ok {
		return "", false, nil
	}
	url, err := s3Store.PresignGetObjectURL(ctx, avatarStorageKey(userID), directDownloadURLTTL)
	if err != nil {
		return "", false, err
	}
	return url, true, nil
}

func avatarStorageKey(userID int64) string {
	return fmt.Sprintf("avatars/%d", userID)
}

func avatarPublicURL(userID int64) string {
	return fmt.Sprintf("/file/avatars/%d", userID)
}

func decodeBase64Payload(content string) ([]byte, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil, fmt.Errorf("empty content")
	}
	if comma := strings.Index(trimmed, ","); comma > 0 && strings.Contains(strings.ToLower(trimmed[:comma]), "base64") {
		trimmed = strings.TrimSpace(trimmed[comma+1:])
	}
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		return decoded, nil
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(trimmed); err == nil {
		return decoded, nil
	}
	return nil, fmt.Errorf("decode base64 failed")
}

func validateAvatarImage(content []byte, declaredType string) error {
	if len(content) == 0 {
		return fmt.Errorf("avatar content is empty")
	}
	if len(content) > avatarMaxSourceBytes {
		return fmt.Errorf("avatar content too large")
	}

	normalizedDeclaredType := strings.ToLower(strings.TrimSpace(declaredType))
	if normalizedDeclaredType != "" && !strings.HasPrefix(normalizedDeclaredType, "image/") {
		return fmt.Errorf("avatar type must be image")
	}

	detectedType := strings.ToLower(http.DetectContentType(content))
	if !strings.HasPrefix(detectedType, "image/") {
		return fmt.Errorf("avatar content must be image")
	}
	if normalizedDeclaredType != "" && normalizedDeclaredType != detectedType {
		return fmt.Errorf("avatar type mismatch")
	}

	config, _, err := image.DecodeConfig(bytes.NewReader(content))
	if err != nil {
		return fmt.Errorf("invalid avatar image")
	}
	if config.Width <= 0 || config.Height <= 0 {
		return fmt.Errorf("invalid avatar dimensions")
	}
	if config.Width > avatarMaxDimension || config.Height > avatarMaxDimension {
		return fmt.Errorf("avatar dimensions exceed limit")
	}
	if int64(config.Width)*int64(config.Height) > avatarMaxPixels {
		return fmt.Errorf("avatar pixel count exceed limit")
	}
	return nil
}

func (s *UserService) withUserAvatarLock(userID int64, fn func() (models.User, error)) (models.User, error) {
	lockValue, _ := s.avatarLocks.LoadOrStore(userID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func (s *UserService) defaultAvatarStore() storage.Store {
	if s.avatarStorage == nil {
		return nil
	}
	return s.avatarStorage.DefaultStore()
}

func (s *UserService) defaultAvatarStorageType() string {
	if s.avatarStorage == nil {
		return storage.TypeLocal
	}
	return s.avatarStorage.DefaultType()
}

func (s *UserService) avatarStoreForType(storeType string) (storage.Store, bool) {
	if s.avatarStorage == nil {
		return nil, false
	}
	return s.avatarStorage.StoreForType(storeType)
}
