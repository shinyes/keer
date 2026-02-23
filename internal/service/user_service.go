package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type UserService struct {
	store *store.SQLStore
}

var (
	ErrInvalidUsername       = errors.New("invalid username")
	ErrInvalidDisplayName    = errors.New("invalid display name")
	ErrInvalidPassword       = errors.New("invalid password")
	ErrInvalidCredentials    = errors.New("invalid credentials")
	ErrInvalidRole           = errors.New("invalid role")
	ErrUsernameAlreadyExists = errors.New("username already exists")
	ErrTokenAlreadyExists    = errors.New("access token already exists")
	ErrTokenAlreadyRevoked   = errors.New("access token already revoked")
	ErrInvalidTokenExpiry    = errors.New("invalid token expiry")
	ErrRegistrationDisabled  = errors.New("registration is disabled")
	usernamePattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{2,31}$`)
)

const settingKeyAllowRegistration = "allow_registration"

type CreateUserInput struct {
	Username     string
	DisplayName  string
	Password     string
	Role         string
	ValidateOnly bool
}

func NewUserService(s *store.SQLStore) *UserService {
	return &UserService{store: s}
}

func (s *UserService) GetUser(ctx context.Context, userID int64) (models.User, error) {
	return s.store.GetUserByID(ctx, userID)
}

func (s *UserService) GetUserByIdentifier(ctx context.Context, identifier string) (models.User, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return models.User{}, sql.ErrNoRows
	}
	if userID, err := strconv.ParseInt(identifier, 10, 64); err == nil {
		return s.store.GetUserByID(ctx, userID)
	}
	return s.store.GetUserByUsername(ctx, normalizeUsername(identifier))
}

func (s *UserService) AuthenticateToken(ctx context.Context, rawToken string) (models.User, error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return models.User{}, sql.ErrNoRows
	}
	user, token, err := s.store.GetUserByToken(ctx, rawToken)
	if err != nil {
		return models.User{}, err
	}
	_ = s.store.TouchPersonalAccessToken(ctx, token.ID)
	return user, nil
}

func (s *UserService) EnsureBootstrap(ctx context.Context, username string, rawToken string) error {
	username = normalizeUsername(username)
	rawToken = strings.TrimSpace(rawToken)
	if username == "" || rawToken == "" {
		return nil
	}

	user, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		user, err = s.store.CreateUser(ctx, username, username, "HOST")
		if err != nil {
			return fmt.Errorf("create bootstrap user: %w", err)
		}
	}

	if _, _, err := s.store.GetUserByToken(ctx, rawToken); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if _, err := s.store.CreatePersonalAccessToken(ctx, user.ID, rawToken, "bootstrap token"); err != nil {
		return fmt.Errorf("create bootstrap token: %w", err)
	}
	return nil
}

func (s *UserService) CreateUser(ctx context.Context, creator *models.User, input CreateUserInput, allowRegistration bool) (models.User, error) {
	username := normalizeUsername(input.Username)
	displayName := strings.TrimSpace(input.DisplayName)
	password := strings.TrimSpace(input.Password)
	role := normalizeUserRole(input.Role)

	if !usernamePattern.MatchString(username) {
		return models.User{}, ErrInvalidUsername
	}
	if displayName == "" {
		displayName = username
	}
	if len([]rune(displayName)) > 64 {
		return models.User{}, ErrInvalidDisplayName
	}
	if password == "" {
		return models.User{}, ErrInvalidPassword
	}
	if role == "" && strings.TrimSpace(input.Role) != "" && !strings.EqualFold(strings.TrimSpace(input.Role), "ROLE_UNSPECIFIED") {
		return models.User{}, ErrInvalidRole
	}

	totalUsers, err := s.store.CountUsers(ctx)
	if err != nil {
		return models.User{}, err
	}
	isFirstUser := totalUsers == 0
	isSuperUser := creator != nil && isSuperUserRole(creator.Role)
	if !isFirstUser && !allowRegistration && !isSuperUser {
		return models.User{}, ErrRegistrationDisabled
	}

	roleToAssign := "USER"
	if isFirstUser {
		roleToAssign = "ADMIN"
	} else if isSuperUser && role != "" {
		roleToAssign = role
	}

	if input.ValidateOnly {
		return models.User{
			Username:          username,
			DisplayName:       displayName,
			Role:              roleToAssign,
			DefaultVisibility: models.VisibilityPrivate,
		}, nil
	}

	if _, err := s.store.GetUserByUsername(ctx, username); err == nil {
		return models.User{}, ErrUsernameAlreadyExists
	} else if !errors.Is(err, sql.ErrNoRows) {
		return models.User{}, err
	}

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return models.User{}, fmt.Errorf("hash password: %w", err)
	}

	user, err := s.store.CreateUserWithProfile(ctx, username, displayName, string(passwordHash), roleToAssign)
	if err != nil {
		if isUniqueConstraintErr(err) {
			return models.User{}, ErrUsernameAlreadyExists
		}
		return models.User{}, err
	}
	return user, nil
}

func (s *UserService) ResolveAllowRegistration(ctx context.Context, fallback bool) (bool, error) {
	raw, err := s.store.GetSetting(ctx, settingKeyAllowRegistration)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fallback, nil
		}
		return fallback, err
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return fallback, nil
	}
}

func (s *UserService) SetAllowRegistration(ctx context.Context, allow bool) error {
	value := "false"
	if allow {
		value = "true"
	}
	return s.store.UpsertSetting(ctx, settingKeyAllowRegistration, value)
}

func (s *UserService) CreateAccessTokenForUser(ctx context.Context, identifier string, description string) (models.User, string, error) {
	return s.CreateAccessTokenForUserWithExpiry(ctx, identifier, description, nil)
}

func (s *UserService) CreateAccessTokenForUserWithExpiry(ctx context.Context, identifier string, description string, expiresAt *time.Time) (models.User, string, error) {
	user, err := s.GetUserByIdentifier(ctx, identifier)
	if err != nil {
		return models.User{}, "", err
	}
	description = strings.TrimSpace(description)
	if description == "" {
		description = "admin generated token"
	}
	token, err := s.createAccessToken(ctx, user.ID, description, expiresAt)
	if err != nil {
		return models.User{}, "", err
	}
	return user, token, nil
}

func (s *UserService) ListAccessTokensForUser(ctx context.Context, identifier string) (models.User, []models.PersonalAccessToken, error) {
	user, err := s.GetUserByIdentifier(ctx, identifier)
	if err != nil {
		return models.User{}, nil, err
	}
	tokens, err := s.store.ListPersonalAccessTokensByUserID(ctx, user.ID)
	if err != nil {
		return models.User{}, nil, err
	}
	return user, tokens, nil
}

func (s *UserService) RevokeAccessTokenByID(ctx context.Context, tokenID int64) (models.PersonalAccessToken, error) {
	token, err := s.store.GetPersonalAccessTokenByID(ctx, tokenID)
	if err != nil {
		return models.PersonalAccessToken{}, err
	}
	if token.RevokedAt != nil {
		return token, ErrTokenAlreadyRevoked
	}
	if err := s.store.RevokePersonalAccessToken(ctx, tokenID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return token, ErrTokenAlreadyRevoked
		}
		return models.PersonalAccessToken{}, err
	}
	return s.store.GetPersonalAccessTokenByID(ctx, tokenID)
}

func (s *UserService) SignInWithPassword(ctx context.Context, username string, password string) (models.User, string, error) {
	username = normalizeUsername(username)
	if username == "" || password == "" {
		return models.User{}, "", ErrInvalidCredentials
	}

	user, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, "", ErrInvalidCredentials
		}
		return models.User{}, "", err
	}
	if user.PasswordHash == "" {
		return models.User{}, "", ErrInvalidCredentials
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return models.User{}, "", ErrInvalidCredentials
	}

	token, err := s.createAccessToken(ctx, user.ID, "signin token", nil)
	if err != nil {
		return models.User{}, "", err
	}
	return user, token, nil
}

func isUniqueConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") || strings.Contains(msg, "constraint failed")
}

func normalizeUsername(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (s *UserService) createAccessToken(ctx context.Context, userID int64, description string, expiresAt *time.Time) (string, error) {
	var normalizedExpiresAt *time.Time
	if expiresAt != nil {
		expires := expiresAt.UTC()
		if !expires.After(time.Now().UTC()) {
			return "", ErrInvalidTokenExpiry
		}
		normalizedExpiresAt = &expires
	}

	for i := 0; i < 5; i++ {
		token, err := generateAccessToken()
		if err != nil {
			return "", err
		}
		if _, err := s.store.CreatePersonalAccessTokenWithExpiry(ctx, userID, token, description, normalizedExpiresAt); err == nil {
			return token, nil
		} else if !isUniqueConstraintErr(err) {
			return "", err
		}
	}
	return "", ErrTokenAlreadyExists
}

func generateAccessToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate access token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func normalizeUserRole(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "ADMIN":
		return "ADMIN"
	case "USER":
		return "USER"
	default:
		return ""
	}
}

func isSuperUserRole(role string) bool {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "HOST", "ADMIN":
		return true
	default:
		return false
	}
}
