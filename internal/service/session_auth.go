package service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

type SessionTokens struct {
	AccessToken           string
	AccessTokenExpiresAt  time.Time
	RefreshToken          string
	RefreshTokenExpiresAt time.Time
}

type accessTokenClaims struct {
	Subject   string `json:"sub"`
	TokenType string `json:"tokenType"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

const placeholderJWTSecret = "change-me-in-production"

func (s *UserService) ConfigureAuth(jwtSecret string, accessTokenTTL time.Duration, refreshTokenTTL time.Duration) error {
	trimmedSecret := strings.TrimSpace(jwtSecret)
	switch trimmedSecret {
	case "":
		return fmt.Errorf("jwt secret is required")
	case placeholderJWTSecret:
		return fmt.Errorf("jwt secret must not use placeholder value %q", placeholderJWTSecret)
	}
	s.jwtSecret = []byte(trimmedSecret)
	if accessTokenTTL > 0 {
		s.accessTokenTTL = accessTokenTTL
	}
	if refreshTokenTTL > 0 {
		s.refreshTokenTTL = refreshTokenTTL
	}
	return nil
}

func (s *UserService) AuthenticateAccessToken(ctx context.Context, rawToken string) (models.User, error) {
	userID, err := s.parseAccessToken(rawToken)
	if err != nil {
		return models.User{}, sql.ErrNoRows
	}
	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

func (s *UserService) RefreshSession(ctx context.Context, rawRefreshToken string) (models.User, SessionTokens, error) {
	rawRefreshToken = strings.TrimSpace(rawRefreshToken)
	if rawRefreshToken == "" {
		return models.User{}, SessionTokens{}, ErrInvalidRefreshToken
	}

	now := time.Now().UTC()
	accessExpiresAt := now.Add(s.accessTokenTTL)
	refreshExpiresAt := now.Add(s.refreshTokenTTL)

	var (
		user           models.User
		nextRefreshRaw string
		rotatedRefresh models.RefreshToken
		err            error
	)
	for i := 0; i < 5; i++ {
		nextRefreshRaw, err = generateSessionRefreshToken()
		if err != nil {
			return models.User{}, SessionTokens{}, err
		}
		user, rotatedRefresh, err = s.store.RotateRefreshToken(ctx, rawRefreshToken, nextRefreshRaw, refreshExpiresAt)
		if err == nil {
			break
		}
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, SessionTokens{}, ErrInvalidRefreshToken
		}
		if !isUniqueConstraintErr(err) {
			return models.User{}, SessionTokens{}, err
		}
	}
	if err != nil {
		return models.User{}, SessionTokens{}, ErrTokenAlreadyExists
	}

	accessToken, err := s.buildAccessToken(user.ID, now, accessExpiresAt)
	if err != nil {
		return models.User{}, SessionTokens{}, err
	}
	return user, SessionTokens{
		AccessToken:           accessToken,
		AccessTokenExpiresAt:  accessExpiresAt,
		RefreshToken:          nextRefreshRaw,
		RefreshTokenExpiresAt: rotatedRefresh.ExpiresAt,
	}, nil
}

func (s *UserService) issueSessionTokens(ctx context.Context, userID int64) (SessionTokens, error) {
	now := time.Now().UTC()
	accessExpiresAt := now.Add(s.accessTokenTTL)
	refreshExpiresAt := now.Add(s.refreshTokenTTL)

	accessToken, err := s.buildAccessToken(userID, now, accessExpiresAt)
	if err != nil {
		return SessionTokens{}, err
	}

	var refreshToken string
	for i := 0; i < 5; i++ {
		candidate, genErr := generateSessionRefreshToken()
		if genErr != nil {
			return SessionTokens{}, genErr
		}
		if _, createErr := s.store.CreateRefreshToken(ctx, userID, candidate, refreshExpiresAt); createErr == nil {
			refreshToken = candidate
			break
		} else if !isUniqueConstraintErr(createErr) {
			return SessionTokens{}, createErr
		}
	}
	if refreshToken == "" {
		return SessionTokens{}, ErrTokenAlreadyExists
	}

	return SessionTokens{
		AccessToken:           accessToken,
		AccessTokenExpiresAt:  accessExpiresAt,
		RefreshToken:          refreshToken,
		RefreshTokenExpiresAt: refreshExpiresAt,
	}, nil
}

func (s *UserService) buildAccessToken(userID int64, issuedAt time.Time, expiresAt time.Time) (string, error) {
	if len(s.jwtSecret) == 0 {
		return "", fmt.Errorf("jwt secret is not configured")
	}

	headerJSON, err := json.Marshal(map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	})
	if err != nil {
		return "", fmt.Errorf("marshal jwt header: %w", err)
	}
	payloadJSON, err := json.Marshal(accessTokenClaims{
		Subject:   strconv.FormatInt(userID, 10),
		TokenType: "access",
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	})
	if err != nil {
		return "", fmt.Errorf("marshal jwt payload: %w", err)
	}

	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	mac := hmac.New(sha256.New, s.jwtSecret)
	if _, err := mac.Write([]byte(signingInput)); err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + signature, nil
}

func (s *UserService) parseAccessToken(rawToken string) (int64, error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return 0, sql.ErrNoRows
	}
	if len(s.jwtSecret) == 0 {
		return 0, sql.ErrNoRows
	}
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return 0, sql.ErrNoRows
	}

	signingInput := parts[0] + "." + parts[1]
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return 0, sql.ErrNoRows
	}
	mac := hmac.New(sha256.New, s.jwtSecret)
	if _, err := mac.Write([]byte(signingInput)); err != nil {
		return 0, sql.ErrNoRows
	}
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return 0, sql.ErrNoRows
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, sql.ErrNoRows
	}
	var header struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return 0, sql.ErrNoRows
	}
	if header.Algorithm != "HS256" || header.Type != "JWT" {
		return 0, sql.ErrNoRows
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, sql.ErrNoRows
	}
	var claims accessTokenClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return 0, sql.ErrNoRows
	}
	if claims.TokenType != "access" || claims.ExpiresAt <= time.Now().UTC().Unix() {
		return 0, sql.ErrNoRows
	}
	userID, err := strconv.ParseInt(strings.TrimSpace(claims.Subject), 10, 64)
	if err != nil || userID <= 0 {
		return 0, sql.ErrNoRows
	}
	return userID, nil
}

func generateSessionRefreshToken() (string, error) {
	buf := make([]byte, 48)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate refresh token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
