package service

import (
	"context"
	"strings"
	"testing"

	"github.com/shinyes/keer/internal/config"
)

func TestStorageSettingsResolveDefaultLocal(t *testing.T) {
	services := setupTestServices(t)
	storageService := NewStorageSettingsService(services.store)
	ctx := context.Background()

	resolved, err := storageService.Resolve(ctx)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Backend != config.StorageBackendLocal {
		t.Fatalf("expected local backend, got %s", resolved.Backend)
	}

	rawBackend, err := services.store.GetSetting(ctx, settingKeyStorageBackend)
	if err != nil {
		t.Fatalf("GetSetting(storage_backend) error = %v", err)
	}
	if strings.TrimSpace(rawBackend) != string(config.StorageBackendLocal) {
		t.Fatalf("expected persisted storage_backend=local, got %q", rawBackend)
	}
}

func TestStorageSettingsSetS3AndResolve(t *testing.T) {
	services := setupTestServices(t)
	storageService := NewStorageSettingsService(services.store)
	ctx := context.Background()

	want := config.S3Config{
		Endpoint:     "https://s3.example.com",
		Region:       "auto",
		Bucket:       "memos",
		AccessKeyID:  "test-access-key-id",
		AccessSecret: "test-access-key-secret",
		UsePathStyle: true,
	}
	if err := storageService.SetS3(ctx, want); err != nil {
		t.Fatalf("SetS3() error = %v", err)
	}

	resolved, err := storageService.Resolve(ctx)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Backend != config.StorageBackendS3 {
		t.Fatalf("expected s3 backend, got %s", resolved.Backend)
	}
	if resolved.S3 != want {
		t.Fatalf("resolved s3 config mismatch: got %+v want %+v", resolved.S3, want)
	}
}

func TestStorageSettingsSetLocal(t *testing.T) {
	services := setupTestServices(t)
	storageService := NewStorageSettingsService(services.store)
	ctx := context.Background()

	if err := storageService.SetS3(ctx, config.S3Config{
		Endpoint:     "https://s3.example.com",
		Region:       "auto",
		Bucket:       "memos",
		AccessKeyID:  "test-access-key-id",
		AccessSecret: "test-access-key-secret",
		UsePathStyle: true,
	}); err != nil {
		t.Fatalf("SetS3() error = %v", err)
	}

	if err := storageService.SetLocal(ctx); err != nil {
		t.Fatalf("SetLocal() error = %v", err)
	}

	resolved, err := storageService.Resolve(ctx)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Backend != config.StorageBackendLocal {
		t.Fatalf("expected local backend, got %s", resolved.Backend)
	}
}

func TestStorageSettingsResolveS3MissingField(t *testing.T) {
	services := setupTestServices(t)
	storageService := NewStorageSettingsService(services.store)
	ctx := context.Background()

	if err := services.store.UpsertSetting(ctx, settingKeyStorageBackend, string(config.StorageBackendS3)); err != nil {
		t.Fatalf("UpsertSetting(storage_backend=s3) error = %v", err)
	}
	if err := services.store.UpsertSetting(ctx, settingKeyStorageS3Region, "auto"); err != nil {
		t.Fatalf("UpsertSetting(storage_s3_region) error = %v", err)
	}
	if err := services.store.UpsertSetting(ctx, settingKeyStorageS3Bucket, "memos"); err != nil {
		t.Fatalf("UpsertSetting(storage_s3_bucket) error = %v", err)
	}
	if err := services.store.UpsertSetting(ctx, settingKeyStorageS3KeyID, "id"); err != nil {
		t.Fatalf("UpsertSetting(storage_s3_access_key_id) error = %v", err)
	}
	if err := services.store.UpsertSetting(ctx, settingKeyStorageS3Secret, "secret"); err != nil {
		t.Fatalf("UpsertSetting(storage_s3_access_key_secret) error = %v", err)
	}

	_, err := storageService.Resolve(ctx)
	if err == nil {
		t.Fatalf("expected Resolve() error when endpoint is missing")
	}
	if !strings.Contains(err.Error(), settingKeyStorageS3Endpoint) {
		t.Fatalf("expected missing endpoint error, got %v", err)
	}
}
