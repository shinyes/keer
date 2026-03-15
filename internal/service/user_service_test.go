package service

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConfigureAuth_RejectsEmptyJWTSecret(t *testing.T) {
	userService := NewUserService(nil)

	if err := userService.ConfigureAuth("", 0, 0); err == nil {
		t.Fatal("expected ConfigureAuth() to reject empty JWT secret")
	}
}

func TestConfigureAuth_RejectsPlaceholderJWTSecret(t *testing.T) {
	userService := NewUserService(nil)

	if err := userService.ConfigureAuth("change-me-in-production", 0, 0); err == nil {
		t.Fatal("expected ConfigureAuth() to reject placeholder JWT secret")
	}
}

func TestCreateUser_FirstUserIsAdmin(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	user, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "alice01",
		Password: "pass-123",
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	user, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username:     "preview01",
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "signin01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	user, tokens, err := userService.SignInWithPassword(ctx, "signin01", "pass-123")
	if err != nil {
		t.Fatalf("SignInWithPassword() error = %v", err)
	}
	if tokens.AccessToken == "" {
		t.Fatalf("expected non-empty access token")
	}
	if tokens.RefreshToken == "" {
		t.Fatalf("expected non-empty refresh token")
	}
	if user.ID != created.ID {
		t.Fatalf("expected signed-in user ID=%d, got %d", created.ID, user.ID)
	}

	authUser, err := userService.AuthenticateAccessToken(ctx, tokens.AccessToken)
	if err != nil {
		t.Fatalf("AuthenticateAccessToken() error = %v", err)
	}
	if authUser.ID != created.ID {
		t.Fatalf("expected authenticated user ID=%d, got %d", created.ID, authUser.ID)
	}
}

func TestSignInWithPassword_InvalidCredentials(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
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

func TestRefreshSession_RotatesRefreshToken(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	created, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "refresh01",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	_, tokens, err := userService.SignInWithPassword(ctx, "refresh01", "pass-123")
	if err != nil {
		t.Fatalf("SignInWithPassword() error = %v", err)
	}

	refreshedUser, refreshedTokens, err := userService.RefreshSession(ctx, tokens.RefreshToken)
	if err != nil {
		t.Fatalf("RefreshSession() error = %v", err)
	}
	if refreshedUser.ID != created.ID {
		t.Fatalf("expected refreshed user ID=%d, got %d", created.ID, refreshedUser.ID)
	}
	if refreshedTokens.AccessToken == "" || refreshedTokens.RefreshToken == "" {
		t.Fatalf("expected rotated access/refresh tokens")
	}
	if refreshedTokens.RefreshToken == tokens.RefreshToken {
		t.Fatalf("expected refresh token rotation")
	}

	if _, err := userService.AuthenticateAccessToken(ctx, refreshedTokens.AccessToken); err != nil {
		t.Fatalf("AuthenticateAccessToken(refreshed) error = %v", err)
	}
	if _, _, err := userService.RefreshSession(ctx, tokens.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected ErrInvalidRefreshToken for rotated refresh token, got %v", err)
	}
}

func TestRefreshSession_SameRefreshTokenOnlyOneConcurrentRequestSucceeds(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	if _, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "refresh-concurrent",
		Password: "pass-123",
	}, true); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	_, tokens, err := userService.SignInWithPassword(ctx, "refresh-concurrent", "pass-123")
	if err != nil {
		t.Fatalf("SignInWithPassword() error = %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	invalidCount := 0
	var unexpectedErr error

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, rotated, refreshErr := userService.RefreshSession(ctx, tokens.RefreshToken)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case refreshErr == nil:
				successCount++
				if rotated.RefreshToken == "" || rotated.AccessToken == "" {
					unexpectedErr = errors.New("expected rotated tokens to be populated")
				}
			case errors.Is(refreshErr, ErrInvalidRefreshToken):
				invalidCount++
			default:
				unexpectedErr = refreshErr
			}
		}()
	}

	close(start)
	wg.Wait()

	if unexpectedErr != nil {
		t.Fatalf("unexpected concurrent refresh error: %v", unexpectedErr)
	}
	if successCount != 1 || invalidCount != 1 {
		t.Fatalf("expected exactly one success and one invalid refresh, got success=%d invalid=%d", successCount, invalidCount)
	}
}

func TestCreateAccessTokenForUser(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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
	userService := newTestUserService(t, services.store)
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

func TestListUserChanges_UsesIncrementalWindowAndIdentifierForms(t *testing.T) {
	services := setupTestServices(t)
	userService := newTestUserService(t, services.store)
	ctx := context.Background()

	first, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "user-change-1",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser(first) error = %v", err)
	}
	second, err := userService.CreateUser(ctx, nil, CreateUserInput{
		Username: "user-change-2",
		Password: "pass-123",
	}, true)
	if err != nil {
		t.Fatalf("CreateUser(second) error = %v", err)
	}

	since := time.Now().UTC().Add(-1 * time.Second)
	if _, err := userService.UpdateUserAvatar(ctx, first.ID, "/file/avatars/first"); err != nil {
		t.Fatalf("UpdateUserAvatar(first) error = %v", err)
	}
	if _, err := userService.UpdateUserAvatar(ctx, second.ID, "/file/avatars/second"); err != nil {
		t.Fatalf("UpdateUserAvatar(second) error = %v", err)
	}
	anchor := time.Now().UTC()

	changes, err := userService.ListUserChanges(
		ctx,
		[]string{
			strconv.FormatInt(first.ID, 10),
			"users/" + strconv.FormatInt(second.ID, 10),
			"users/" + strconv.FormatInt(first.ID, 10),
			"missing-user",
		},
		since,
		anchor,
	)
	if err != nil {
		t.Fatalf("ListUserChanges() error = %v", err)
	}
	if len(changes.Users) != 2 {
		t.Fatalf("expected 2 changed users, got %d", len(changes.Users))
	}
	if changes.SyncAnchor.IsZero() {
		t.Fatalf("expected non-zero sync anchor")
	}
	if changes.Users[0].ID >= changes.Users[1].ID && changes.Users[0].UpdateTime.Equal(changes.Users[1].UpdateTime) {
		t.Fatalf("expected deterministic ordering by id when update time equal")
	}

	emptyWindow, err := userService.ListUserChanges(
		ctx,
		[]string{strconv.FormatInt(first.ID, 10)},
		anchor.Add(time.Minute),
		anchor,
	)
	if err != nil {
		t.Fatalf("ListUserChanges(empty window) error = %v", err)
	}
	if len(emptyWindow.Users) != 0 {
		t.Fatalf("expected empty changes when since is after anchor, got %d", len(emptyWindow.Users))
	}
}
