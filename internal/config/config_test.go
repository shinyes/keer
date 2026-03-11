package config

import "testing"

func TestLoad_DefaultLocalStorage(t *testing.T) {
	t.Setenv("STORAGE_BACKEND", "")
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_REGION", "")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("S3_ACCESS_KEY_ID", "")
	t.Setenv("S3_ACCESS_KEY_SECRET", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Storage != StorageBackendLocal {
		t.Fatalf("expected local storage, got %q", cfg.Storage)
	}
}

func TestLoad_S3StorageFromEnv(t *testing.T) {
	t.Setenv("STORAGE_BACKEND", "s3")
	t.Setenv("S3_ENDPOINT", "https://s3.example.com")
	t.Setenv("S3_REGION", "auto")
	t.Setenv("S3_BUCKET", "keer")
	t.Setenv("S3_ACCESS_KEY_ID", "test-id")
	t.Setenv("S3_ACCESS_KEY_SECRET", "test-secret")
	t.Setenv("S3_USE_PATH_STYLE", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Storage != StorageBackendS3 {
		t.Fatalf("expected s3 storage, got %q", cfg.Storage)
	}
	if cfg.S3.Endpoint != "https://s3.example.com" || cfg.S3.Region != "auto" || cfg.S3.Bucket != "keer" {
		t.Fatalf("unexpected s3 config: %+v", cfg.S3)
	}
	if cfg.S3.UsePathStyle {
		t.Fatalf("expected path style false")
	}
}

func TestLoad_InvalidStorageBackend(t *testing.T) {
	t.Setenv("STORAGE_BACKEND", "bad")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid storage backend")
	}
}

func TestLoad_S3StorageMissingField(t *testing.T) {
	t.Setenv("STORAGE_BACKEND", "s3")
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_REGION", "auto")
	t.Setenv("S3_BUCKET", "keer")
	t.Setenv("S3_ACCESS_KEY_ID", "test-id")
	t.Setenv("S3_ACCESS_KEY_SECRET", "test-secret")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for incomplete s3 config")
	}
}
