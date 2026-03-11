package app

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/store"
)

func TestPurgeLegacyStorageSettings(t *testing.T) {
	tempDir := t.TempDir()
	sqliteDB, err := db.OpenSQLite(filepath.Join(tempDir, "keer.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer sqliteDB.Close() //nolint:errcheck

	if err := db.Migrate(sqliteDB); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	sqlStore := store.New(sqliteDB)
	ctx := context.Background()
	legacyKeys := []string{
		"storage_backend",
		"storage_s3_endpoint",
		"storage_s3_region",
		"storage_s3_bucket",
		"storage_s3_access_key_id",
		"storage_s3_access_key_secret",
		"storage_s3_use_path_style",
	}
	for _, key := range legacyKeys {
		if err := sqlStore.UpsertSetting(ctx, key, "legacy-value"); err != nil {
			t.Fatalf("UpsertSetting(%q) error = %v", key, err)
		}
	}
	if err := sqlStore.UpsertSetting(ctx, "allow_registration", "true"); err != nil {
		t.Fatalf("UpsertSetting(allow_registration) error = %v", err)
	}

	if err := purgeLegacyStorageSettings(ctx, sqlStore); err != nil {
		t.Fatalf("purgeLegacyStorageSettings() error = %v", err)
	}

	for _, key := range legacyKeys {
		_, err := sqlStore.GetSetting(ctx, key)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected %q to be removed, got err=%v", key, err)
		}
	}

	allowRegistration, err := sqlStore.GetSetting(ctx, "allow_registration")
	if err != nil {
		t.Fatalf("GetSetting(allow_registration) error = %v", err)
	}
	if allowRegistration != "true" {
		t.Fatalf("unexpected allow_registration value %q", allowRegistration)
	}
}
