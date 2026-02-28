package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type testServices struct {
	store       *store.SQLStore
	memoService *MemoService
}

func setupTestServices(t *testing.T) testServices {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	sqliteDB, err := db.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteDB.Close()
	})
	if err := db.Migrate(sqliteDB); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	sqlStore := store.New(sqliteDB)
	return testServices{
		store:       sqlStore,
		memoService: NewMemoService(sqlStore),
	}
}

func mustCreateUser(t *testing.T, s *store.SQLStore, username string) models.User {
	t.Helper()
	user, err := s.CreateUser(context.Background(), username, username, "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	return user
}
