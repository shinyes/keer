package service

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"
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

func TestResolveAllowRegistration(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	allow, err := userService.ResolveAllowRegistration(ctx, true)
	if err != nil {
		t.Fatalf("ResolveAllowRegistration(default=true) error = %v", err)
	}
	if !allow {
		t.Fatalf("expected true when no setting and fallback=true")
	}

	if err := userService.SetAllowRegistration(ctx, false); err != nil {
		t.Fatalf("SetAllowRegistration(false) error = %v", err)
	}
	allow, err = userService.ResolveAllowRegistration(ctx, true)
	if err != nil {
		t.Fatalf("ResolveAllowRegistration(after false) error = %v", err)
	}
	if allow {
		t.Fatalf("expected false from persisted setting")
	}

	if err := userService.SetAllowRegistration(ctx, true); err != nil {
		t.Fatalf("SetAllowRegistration(true) error = %v", err)
	}
	allow, err = userService.ResolveAllowRegistration(ctx, false)
	if err != nil {
		t.Fatalf("ResolveAllowRegistration(after true) error = %v", err)
	}
	if !allow {
		t.Fatalf("expected true from persisted setting")
	}
}

func TestCreateAccessTokenForUser(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "token01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	user, token, err := userService.CreateAccessTokenForUser(ctx, created.Username, "cli token")
	if err != nil {
		t.Fatalf("CreateAccessTokenForUser(username) error = %v", err)
	}
	if token == "" {
		t.Fatalf("expected non-empty token")
	}
	if user.ID != created.ID {
		t.Fatalf("expected user ID %d, got %d", created.ID, user.ID)
	}

	authUser, err := userService.AuthenticateToken(ctx, token)
	if err != nil {
		t.Fatalf("AuthenticateToken() error = %v", err)
	}
	if authUser.ID != created.ID {
		t.Fatalf("expected authenticated user ID %d, got %d", created.ID, authUser.ID)
	}

	_, token2, err := userService.CreateAccessTokenForUser(ctx, strconv.FormatInt(created.ID, 10), "")
	if err != nil {
		t.Fatalf("CreateAccessTokenForUser(id) error = %v", err)
	}
	if token2 == "" {
		t.Fatalf("expected non-empty token2")
	}
}

func TestCreateAccessTokenForUserWithExpiry(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "token02",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	expiresAt := time.Now().UTC().Add(2 * time.Hour)
	user, token, err := userService.CreateAccessTokenForUserWithExpiry(ctx, created.Username, "expiring token", &expiresAt)
	if err != nil {
		t.Fatalf("CreateAccessTokenForUserWithExpiry() error = %v", err)
	}
	if token == "" {
		t.Fatalf("expected non-empty token")
	}
	if user.ID != created.ID {
		t.Fatalf("expected user ID %d, got %d", created.ID, user.ID)
	}

	_, pat, err := services.store.GetUserByToken(ctx, token)
	if err != nil {
		t.Fatalf("GetUserByToken() error = %v", err)
	}
	if pat.ExpiresAt == nil {
		t.Fatalf("expected expires_at to be set")
	}
	if !pat.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("expected expires_at in future, got %s", pat.ExpiresAt.UTC().Format(time.RFC3339))
	}
}

func TestCreateAccessTokenForUserWithExpiry_InvalidPastTime(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "token03",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	expiresAt := time.Now().UTC().Add(-1 * time.Hour)
	if _, _, err := userService.CreateAccessTokenForUserWithExpiry(ctx, created.Username, "expired", &expiresAt); !errors.Is(err, ErrInvalidTokenExpiry) {
		t.Fatalf("expected ErrInvalidTokenExpiry, got %v", err)
	}
}

func TestListAccessTokensForUser(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "token-list01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	if _, _, err := userService.CreateAccessTokenForUser(ctx, created.Username, "list-token-1"); err != nil {
		t.Fatalf("CreateAccessTokenForUser(token1) error = %v", err)
	}
	if _, _, err := userService.CreateAccessTokenForUser(ctx, created.Username, "list-token-2"); err != nil {
		t.Fatalf("CreateAccessTokenForUser(token2) error = %v", err)
	}

	user, tokens, err := userService.ListAccessTokensForUser(ctx, created.Username)
	if err != nil {
		t.Fatalf("ListAccessTokensForUser() error = %v", err)
	}
	if user.ID != created.ID {
		t.Fatalf("expected user ID %d, got %d", created.ID, user.ID)
	}
	if len(tokens) < 2 {
		t.Fatalf("expected at least 2 tokens, got %d", len(tokens))
	}
	for _, token := range tokens {
		if token.UserID != created.ID {
			t.Fatalf("expected token user_id=%d, got %d", created.ID, token.UserID)
		}
		if token.TokenPrefix == "" {
			t.Fatalf("expected non-empty token prefix")
		}
	}
}

func TestRevokeAccessTokenByID(t *testing.T) {
	services := setupTestServices(t)
	userService := NewUserService(services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "token-revoke01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	_, rawToken, err := userService.CreateAccessTokenForUser(ctx, created.Username, "revoke-me")
	if err != nil {
		t.Fatalf("CreateAccessTokenForUser() error = %v", err)
	}
	_, tokenRecord, err := services.store.GetUserByToken(ctx, rawToken)
	if err != nil {
		t.Fatalf("GetUserByToken() error = %v", err)
	}

	revoked, err := userService.RevokeAccessTokenByID(ctx, tokenRecord.ID)
	if err != nil {
		t.Fatalf("RevokeAccessTokenByID() error = %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatalf("expected revoked_at to be set")
	}

	if _, err := userService.AuthenticateToken(ctx, rawToken); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected revoked token authentication to fail with sql.ErrNoRows, got %v", err)
	}

	if _, err := userService.RevokeAccessTokenByID(ctx, tokenRecord.ID); !errors.Is(err, ErrTokenAlreadyRevoked) {
		t.Fatalf("expected ErrTokenAlreadyRevoked on second revoke, got %v", err)
	}
}
