package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestCreateUser_FirstUserIsAdmin(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	user, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username:    "alice01",
		DisplayName: "Alice",
		Password:    "pass-123",
	}, false)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if user.Username != "alice01" {
		t.Fatalf("unexpected username: %s", user.Username)
	}
	if user.Role != "ADMIN" {
		t.Fatalf("expected ADMIN for first user, got %s", user.Role)
	}
	if user.PasswordHash == "" {
		t.Fatalf("expected non-empty password hash")
	}
}

func TestCreateUser_DuplicateUsername(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "bob01", Password: "pass-123"}, true); err != nil {
		t.Fatalf("first CreateUser() error = %v", err)
	}
	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "bob01", Password: "pass-123"}, true); !errors.Is(err, ErrUsernameAlreadyExists) {
		t.Fatalf("expected ErrUsernameAlreadyExists, got %v", err)
	}
}

func TestCreateUser_InvalidUsername(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "ab", Password: "pass-123"}, true); !errors.Is(err, ErrInvalidUsername) {
		t.Fatalf("expected ErrInvalidUsername for short username, got %v", err)
	}
	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "_abc", Password: "pass-123"}, true); !errors.Is(err, ErrInvalidUsername) {
		t.Fatalf("expected ErrInvalidUsername for leading underscore, got %v", err)
	}
}

func TestCreateUser_RegistrationDisabledForSecondUser(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	admin, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "owner01", Password: "pass-123"}, true)
	if err != nil {
		t.Fatalf("first CreateUser() error = %v", err)
	}
	if admin.Role != "ADMIN" {
		t.Fatalf("expected first user role ADMIN, got %s", admin.Role)
	}

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "user02", Password: "pass-123"}, false); !errors.Is(err, ErrRegistrationDisabled) {
		t.Fatalf("expected ErrRegistrationDisabled, got %v", err)
	}
}

func TestCreateUser_ValidateOnlyDoesNotPersist(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	user, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username:     "preview01",
		DisplayName:  "Preview",
		Password:     "pass-123",
		ValidateOnly: true,
	}, true)
	if err != nil {
		t.Fatalf("CreateUser(validate only) error = %v", err)
	}
	if user.ID != 0 {
		t.Fatalf("expected user ID to be 0 in validateOnly mode, got %d", user.ID)
	}

	_, err = services.store.GetUserByUsername(ctx, "preview01")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected preview user not persisted, got err=%v", err)
	}
}

func TestCreateUser_AdminCanAssignAdminRole(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	admin, err := userService.CreateUser(ctx, nil, CreateUserInput{Username: "root01", Password: "pass-123"}, true)
	if err != nil {
		t.Fatalf("create first user error = %v", err)
	}
	if admin.Role != "ADMIN" {
		t.Fatalf("expected first user role ADMIN, got %s", admin.Role)
	}

	user, err := userService.CreateUser(ctx, &admin, CreateUserInput{
		Username: "admin02",
		Role:     "ADMIN",
		Password: "pass-123",
	}, false)
	if err != nil {
		t.Fatalf("admin create user error = %v", err)
	}
	if user.Role != "ADMIN" {
		t.Fatalf("expected assigned ADMIN role, got %s", user.Role)
	}
}

func TestCreateUser_EmptyPassword(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "nopass01",
		Password: "",
	}, true); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("expected ErrInvalidPassword, got %v", err)
	}
}

func TestSignInWithPassword_Success(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "signin01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	user, token, err := userService.SignInWithPassword(ctx, "signin01", "pass-123")
	if err != nil {
		t.Fatalf("SignInWithPassword() error = %v", err)
	}
	if token == "" {
		t.Fatalf("expected non-empty token")
	}
	if user.ID != created.ID {
		t.Fatalf("expected signed-in user ID=%d, got %d", created.ID, user.ID)
	}

	authUser, err := userService.AuthenticateToken(ctx, token)
	if err != nil {
		t.Fatalf("AuthenticateToken() error = %v", err)
	}
	if authUser.ID != created.ID {
		t.Fatalf("expected authenticated user ID=%d, got %d", created.ID, authUser.ID)
	}
}

func TestSignInWithPassword_InvalidCredentials(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "signin02",
		Password: "pass-123",
	}, true); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	if _, _, err := userService.SignInWithPassword(ctx, "signin02", "wrong-pass"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
	if _, _, err := userService.SignInWithPassword(ctx, "not-exists", "pass-123"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for not exists user, got %v", err)
	}
}
