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

const testJWTSecret = "test-jwt-secret"

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

func newTestUserService(t *testing.T, sqlStore *store.SQLStore) *UserService {
	t.Helper()
	userService := NewUserService(sqlStore)
	if err := userService.ConfigureAuth(testJWTSecret, 0, 0); err != nil {
		t.Fatalf("ConfigureAuth() error = %v", err)
	}
	return userService
}

func mustCreateUser(t *testing.T, s *store.SQLStore, username string) models.User {
	t.Helper()
	user, err := s.CreateUser(context.Background(), username, "USER")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	return user
}

func mustAddFriend(t *testing.T, s *store.SQLStore, userID int64, friendID int64) {
	t.Helper()
	if err := s.AddFriend(context.Background(), userID, friendID); err != nil {
		t.Fatalf("AddFriend() error = %v", err)
	}
}
