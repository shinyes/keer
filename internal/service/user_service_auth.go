package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
	"golang.org/x/crypto/bcrypt"
	"strings"
	"time"
)

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

func (s *UserService) CreateUser(ctx context.Context, creator *models.User, input CreateUserInput, allowRegistration bool) (models.User, error) {
	username := normalizeUsername(input.Username)
	password := strings.TrimSpace(input.Password)
	role := normalizeUserRole(input.Role)
	isSuperUser := creator != nil && isSuperUserRole(creator.Role)

	if !usernamePattern.MatchString(username) {
		return models.User{}, ErrInvalidUsername
	}
	if password == "" {
		return models.User{}, ErrInvalidPassword
	}
	if role == "" && strings.TrimSpace(input.Role) != "" && !strings.EqualFold(strings.TrimSpace(input.Role), "ROLE_UNSPECIFIED") {
		return models.User{}, ErrInvalidRole
	}

	if input.ValidateOnly {
		totalUsers, err := s.store.CountUsers(ctx)
		if err != nil {
			return models.User{}, err
		}
		roleToAssign := "USER"
		if totalUsers == 0 {
			roleToAssign = "ADMIN"
		} else if isSuperUser && role != "" {
			roleToAssign = role
		}
		if totalUsers > 0 && !allowRegistration && !isSuperUser {
			return models.User{}, ErrRegistrationDisabled
		}
		return models.User{
			Username:          username,
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

	user, err := s.store.CreateUserWithProfileSubjectToRegistration(
		ctx,
		username,
		string(passwordHash),
		role,
		allowRegistration,
		isSuperUser,
	)
	if err != nil {
		if errors.Is(err, store.ErrRegistrationNotAllowed) {
			return models.User{}, ErrRegistrationDisabled
		}
		if isUniqueConstraintErr(err) {
			return models.User{}, ErrUsernameAlreadyExists
		}
		return models.User{}, err
	}
	return user, nil
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

func (s *UserService) SignInWithPassword(ctx context.Context, username string, password string) (models.User, SessionTokens, error) {
	username = normalizeUsername(username)
	if username == "" || password == "" {
		return models.User{}, SessionTokens{}, ErrInvalidCredentials
	}

	user, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, SessionTokens{}, ErrInvalidCredentials
		}
		return models.User{}, SessionTokens{}, err
	}
	if user.PasswordHash == "" {
		return models.User{}, SessionTokens{}, ErrInvalidCredentials
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return models.User{}, SessionTokens{}, ErrInvalidCredentials
	}

	tokens, err := s.issueSessionTokens(ctx, user.ID)
	if err != nil {
		return models.User{}, SessionTokens{}, err
	}
	return user, tokens, nil
}

func isUniqueConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") || strings.Contains(msg, "constraint failed")
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
