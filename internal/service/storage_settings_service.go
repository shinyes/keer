package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/store"
)

const (
	settingKeyStorageBackend    = "storage_backend"
	settingKeyStorageS3Endpoint = "storage_s3_endpoint"
	settingKeyStorageS3Region   = "storage_s3_region"
	settingKeyStorageS3Bucket   = "storage_s3_bucket"
	settingKeyStorageS3KeyID    = "storage_s3_access_key_id"
	settingKeyStorageS3Secret   = "storage_s3_access_key_secret"
	settingKeyStorageS3Path     = "storage_s3_use_path_style"
)

type StorageSettings struct {
	Backend config.StorageBackend
	S3      config.S3Config
}

type StorageSettingsService struct {
	store *store.SQLStore
}

func NewStorageSettingsService(s *store.SQLStore) *StorageSettingsService {
	return &StorageSettingsService{store: s}
}

func (s *StorageSettingsService) Resolve(ctx context.Context) (StorageSettings, error) {
	backend, err := s.resolveBackend(ctx)
	if err != nil {
		return StorageSettings{}, err
	}

	resolved := StorageSettings{
		Backend: backend,
	}
	if backend == config.StorageBackendLocal {
		return resolved, nil
	}

	s3Cfg, err := s.resolveS3Config(ctx)
	if err != nil {
		return StorageSettings{}, err
	}
	resolved.S3 = s3Cfg
	return resolved, nil
}

func (s *StorageSettingsService) SetLocal(ctx context.Context) error {
	return s.store.UpsertSetting(ctx, settingKeyStorageBackend, string(config.StorageBackendLocal))
}

func (s *StorageSettingsService) SetS3(ctx context.Context, cfg config.S3Config) error {
	normalized := config.S3Config{
		Endpoint:     strings.TrimSpace(cfg.Endpoint),
		Region:       strings.TrimSpace(cfg.Region),
		Bucket:       strings.TrimSpace(cfg.Bucket),
		AccessKeyID:  strings.TrimSpace(cfg.AccessKeyID),
		AccessSecret: strings.TrimSpace(cfg.AccessSecret),
		UsePathStyle: cfg.UsePathStyle,
	}
	if err := normalized.Validate(); err != nil {
		return err
	}

	settings := []struct {
		key   string
		value string
	}{
		{settingKeyStorageS3Endpoint, normalized.Endpoint},
		{settingKeyStorageS3Region, normalized.Region},
		{settingKeyStorageS3Bucket, normalized.Bucket},
		{settingKeyStorageS3KeyID, normalized.AccessKeyID},
		{settingKeyStorageS3Secret, normalized.AccessSecret},
		{settingKeyStorageS3Path, strconv.FormatBool(normalized.UsePathStyle)},
	}
	for _, item := range settings {
		if err := s.store.UpsertSetting(ctx, item.key, item.value); err != nil {
			return err
		}
	}
	return s.store.UpsertSetting(ctx, settingKeyStorageBackend, string(config.StorageBackendS3))
}

func (s *StorageSettingsService) resolveBackend(ctx context.Context) (config.StorageBackend, error) {
	raw, err := s.store.GetSetting(ctx, settingKeyStorageBackend)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		if err := s.store.UpsertSetting(ctx, settingKeyStorageBackend, string(config.StorageBackendLocal)); err != nil {
			return "", err
		}
		return config.StorageBackendLocal, nil
	}

	backend := config.StorageBackend(strings.ToLower(strings.TrimSpace(raw)))
	switch backend {
	case config.StorageBackendLocal, config.StorageBackendS3:
		return backend, nil
	default:
		return "", fmt.Errorf("unsupported storage backend %q in setting %s", raw, settingKeyStorageBackend)
	}
}

func (s *StorageSettingsService) resolveS3Config(ctx context.Context) (config.S3Config, error) {
	endpoint, err := s.getRequiredSetting(ctx, settingKeyStorageS3Endpoint)
	if err != nil {
		return config.S3Config{}, err
	}
	region, err := s.getRequiredSetting(ctx, settingKeyStorageS3Region)
	if err != nil {
		return config.S3Config{}, err
	}
	bucket, err := s.getRequiredSetting(ctx, settingKeyStorageS3Bucket)
	if err != nil {
		return config.S3Config{}, err
	}
	accessKeyID, err := s.getRequiredSetting(ctx, settingKeyStorageS3KeyID)
	if err != nil {
		return config.S3Config{}, err
	}
	accessSecret, err := s.getRequiredSetting(ctx, settingKeyStorageS3Secret)
	if err != nil {
		return config.S3Config{}, err
	}
	usePathStyle, err := s.getBoolSetting(ctx, settingKeyStorageS3Path, true)
	if err != nil {
		return config.S3Config{}, err
	}

	cfg := config.S3Config{
		Endpoint:     endpoint,
		Region:       region,
		Bucket:       bucket,
		AccessKeyID:  accessKeyID,
		AccessSecret: accessSecret,
		UsePathStyle: usePathStyle,
	}
	if err := cfg.Validate(); err != nil {
		return config.S3Config{}, err
	}
	return cfg, nil
}

func (s *StorageSettingsService) getRequiredSetting(ctx context.Context, key string) (string, error) {
	raw, err := s.store.GetSetting(ctx, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("setting %s is required when storage backend is s3", key)
		}
		return "", err
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("setting %s is required when storage backend is s3", key)
	}
	return value, nil
}

func (s *StorageSettingsService) getBoolSetting(ctx context.Context, key string, fallback bool) (bool, error) {
	raw, err := s.store.GetSetting(ctx, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fallback, nil
		}
		return fallback, err
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback, nil
	}
	parsed, parseErr := strconv.ParseBool(value)
	if parseErr != nil {
		return fallback, fmt.Errorf("invalid bool in setting %s: %q", key, raw)
	}
	return parsed, nil
}
