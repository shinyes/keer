package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"image"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type UserService struct {
	store           *store.SQLStore
	avatarStorage   *storage.Router
	avatarLocks     sync.Map
	jwtSecret       []byte
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
}

var (
	ErrInvalidUsername        = errors.New("invalid username")
	ErrInvalidPassword        = errors.New("invalid password")
	ErrInvalidCurrentPassword = errors.New("invalid current password")
	ErrInvalidEncryptionKey   = errors.New("invalid encryption key")
	ErrInvalidGeneralSetting  = errors.New("invalid general setting")
	ErrInvalidCredentials     = errors.New("invalid credentials")
	ErrInvalidRefreshToken    = errors.New("invalid refresh token")
	ErrInvalidRole            = errors.New("invalid role")
	ErrUsernameAlreadyExists  = errors.New("username already exists")
	ErrTokenAlreadyExists     = errors.New("access token already exists")
	ErrTokenAlreadyRevoked    = errors.New("access token already revoked")
	ErrInvalidTokenExpiry     = errors.New("invalid token expiry")
	ErrRegistrationDisabled   = errors.New("registration is disabled")
	ErrCannotFriendSelf       = errors.New("cannot add yourself as a friend")
	ErrFriendNotFound         = errors.New("friend not found")
	usernamePattern           = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{2,31}$`)
)

const (
	defaultAccessTokenTTL  = 15 * time.Minute
	defaultRefreshTokenTTL = 30 * 24 * time.Hour
	avatarMaxSourceBytes   = 10 * 1024 * 1024
	avatarMaxDimension     = 4096
	avatarMaxPixels        = 12_000_000
)

type CreateUserInput struct {
	Username     string
	Password     string
	Role         string
	ValidateOnly bool
}

type UserChanges struct {
	Users      []models.User
	SyncAnchor time.Time
}

type UpsertUserEncryptionKeyInput struct {
	Version                  int
	KDFAlgorithm             string
	KDFSalt                  string
	KDFTimeCost              int
	KDFMemoryKiB             int
	KDFParallelism           int
	WrapAlgorithm            string
	WrappedAccountKey        string
	SharingPublicKey         string
	WrappedSharingPrivateKey string
	KeyVersion               int
	Algorithms               string
}

type UpdateUserGeneralSettingsInput struct {
	MemoVisibility  string
	MemoEditGesture string
	MemoColumns     []models.MemoColumnConfig
}

func NewUserService(s *store.SQLStore) *UserService {
	return &UserService{
		store:           s,
		accessTokenTTL:  defaultAccessTokenTTL,
		refreshTokenTTL: defaultRefreshTokenTTL,
	}
}

func (s *UserService) SetAvatarStorageRouter(router *storage.Router) {
	s.avatarStorage = router
}

func (s *UserService) GetUserEncryptionKey(ctx context.Context, userID int64) (models.UserEncryptionKey, error) {
	return s.store.GetUserEncryptionKeyByUserID(ctx, userID)
}

func (s *UserService) UpsertUserEncryptionKey(
	ctx context.Context,
	userID int64,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	encryptionKey, err := normalizeUserEncryptionKeyInput(userID, input)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	return s.store.UpsertUserEncryptionKey(ctx, encryptionKey)
}

func normalizeUserEncryptionKeyInput(
	userID int64,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	input.KDFAlgorithm = strings.TrimSpace(input.KDFAlgorithm)
	input.KDFSalt = strings.TrimSpace(input.KDFSalt)
	input.WrapAlgorithm = strings.TrimSpace(input.WrapAlgorithm)
	input.WrappedAccountKey = strings.TrimSpace(input.WrappedAccountKey)
	input.SharingPublicKey = strings.TrimSpace(input.SharingPublicKey)
	input.WrappedSharingPrivateKey = strings.TrimSpace(input.WrappedSharingPrivateKey)
	input.Algorithms = strings.TrimSpace(input.Algorithms)
	if input.KeyVersion <= 0 {
		input.KeyVersion = 1
	}
	if userID <= 0 ||
		input.Version != accountMasterKeyRecoveryBundleVersion ||
		input.KDFAlgorithm != accountMasterKeyRecoveryKDFAlgorithm ||
		input.KDFSalt == "" ||
		input.KDFTimeCost <= 0 ||
		input.KDFMemoryKiB <= 0 ||
		input.KDFParallelism <= 0 ||
		input.WrapAlgorithm != accountMasterKeyRecoveryWrapAlgorithm ||
		input.WrappedAccountKey == "" {
		return models.UserEncryptionKey{}, ErrInvalidEncryptionKey
	}
	if (input.SharingPublicKey == "") != (input.WrappedSharingPrivateKey == "") {
		return models.UserEncryptionKey{}, ErrInvalidEncryptionKey
	}

	return models.UserEncryptionKey{
		UserID:                   userID,
		Version:                  input.Version,
		KDFAlgorithm:             input.KDFAlgorithm,
		KDFSalt:                  input.KDFSalt,
		KDFTimeCost:              input.KDFTimeCost,
		KDFMemoryKiB:             input.KDFMemoryKiB,
		KDFParallelism:           input.KDFParallelism,
		WrapAlgorithm:            input.WrapAlgorithm,
		WrappedAccountKey:        input.WrappedAccountKey,
		SharingPublicKey:         input.SharingPublicKey,
		WrappedSharingPrivateKey: input.WrappedSharingPrivateKey,
		KeyVersion:               input.KeyVersion,
		Algorithms:               input.Algorithms,
	}, nil
}

const (
	accountMasterKeyRecoveryBundleVersion = 2
	accountMasterKeyRecoveryKDFAlgorithm  = "ARGON2ID"
	accountMasterKeyRecoveryWrapAlgorithm = "AES_GCM"
)

func (s *UserService) ChangePassword(
	ctx context.Context,
	userID int64,
	currentPassword string,
	newPassword string,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	currentPassword = strings.TrimSpace(currentPassword)
	newPassword = strings.TrimSpace(newPassword)
	if userID <= 0 || currentPassword == "" {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}
	if newPassword == "" {
		return models.UserEncryptionKey{}, ErrInvalidPassword
	}

	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	if user.PasswordHash == "" {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(currentPassword)); err != nil {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}

	encryptionKey, err := normalizeUserEncryptionKeyInput(userID, input)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return models.UserEncryptionKey{}, fmt.Errorf("hash password: %w", err)
	}

	return s.store.UpdateUserPasswordHashAndEncryptionKey(ctx, userID, string(passwordHash), encryptionKey)
}

func (s *UserService) GetUserGeneralSettings(ctx context.Context, userID int64) (models.UserGeneralSettings, error) {
	return s.store.GetUserGeneralSettings(ctx, userID)
}

func (s *UserService) UpdateUserGeneralSettings(
	ctx context.Context,
	userID int64,
	input UpdateUserGeneralSettingsInput,
) (models.UserGeneralSettings, error) {
	settings, err := normalizeUserGeneralSettingsInput(userID, input)
	if err != nil {
		return models.UserGeneralSettings{}, err
	}
	return s.store.UpdateUserGeneralSettings(ctx, settings)
}

func normalizeUserGeneralSettingsInput(
	userID int64,
	input UpdateUserGeneralSettingsInput,
) (models.UserGeneralSettings, error) {
	if userID <= 0 {
		return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
	}

	visibility := models.Visibility(strings.TrimSpace(input.MemoVisibility))
	gesture := models.MemoEditGesture(strings.TrimSpace(input.MemoEditGesture))
	if !visibility.IsValid() || !gesture.IsValid() {
		return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
	}

	columns := make([]models.MemoColumnConfig, 0, len(input.MemoColumns))
	seenIDs := make(map[string]struct{}, len(input.MemoColumns))
	for _, column := range input.MemoColumns {
		id := strings.TrimSpace(column.ID)
		name := strings.TrimSpace(column.Name)
		if id == "" || name == "" {
			return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
		}
		if _, exists := seenIDs[id]; exists {
			return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
		}
		seenIDs[id] = struct{}{}
		columns = append(columns, models.MemoColumnConfig{
			ID:              id,
			Name:            name,
			RequiredTags:    normalizeMemoColumnTags(column.RequiredTags),
			VisibleInDrawer: column.VisibleInDrawer,
			PinnedMemoNames: normalizeMemoColumnMemoNames(column.PinnedMemoNames),
		})
	}

	return models.UserGeneralSettings{
		UserID:          userID,
		MemoVisibility:  visibility,
		MemoEditGesture: gesture,
		MemoColumns:     columns,
	}, nil
}

func normalizeMemoColumnTags(tags []string) []string {
	normalized := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, raw := range tags {
		tag := strings.TrimSpace(raw)
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized
}

func normalizeMemoColumnMemoNames(names []string) []string {
	normalized := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func (s *UserService) GetUser(ctx context.Context, userID int64) (models.User, error) {
	return s.store.GetUserByID(ctx, userID)
}

func (s *UserService) GetUserByIdentifier(ctx context.Context, identifier string) (models.User, error) {
	identifier = normalizeUserIdentifier(identifier)
	if identifier == "" {
		return models.User{}, sql.ErrNoRows
	}
	if userID, err := strconv.ParseInt(identifier, 10, 64); err == nil {
		return s.store.GetUserByID(ctx, userID)
	}
	return s.store.GetUserByUsername(ctx, normalizeUsername(identifier))
}

func (s *UserService) ListUserChanges(
	ctx context.Context,
	identifiers []string,
	since time.Time,
	syncAnchor time.Time,
) (UserChanges, error) {
	normalizedAnchor := syncAnchor.UTC()
	if normalizedAnchor.IsZero() {
		normalizedAnchor = time.Now().UTC()
	}
	normalizedSince := since.UTC()
	if normalizedSince.After(normalizedAnchor) {
		normalizedSince = normalizedAnchor
	}

	userIDSet := make(map[int64]struct{}, len(identifiers))
	usernameSet := make(map[string]struct{}, len(identifiers))
	for _, raw := range identifiers {
		identifier := normalizeUserIdentifier(raw)
		if identifier == "" {
			continue
		}
		if userID, err := strconv.ParseInt(identifier, 10, 64); err == nil {
			if userID > 0 {
				userIDSet[userID] = struct{}{}
			}
			continue
		}
		username := normalizeUsername(identifier)
		if username == "" {
			continue
		}
		usernameSet[username] = struct{}{}
	}

	userIDs := make([]int64, 0, len(userIDSet))
	for userID := range userIDSet {
		userIDs = append(userIDs, userID)
	}
	usernames := make([]string, 0, len(usernameSet))
	for username := range usernameSet {
		usernames = append(usernames, username)
	}

	users, err := s.store.ListUsersByIdentifiersUpdatedWithin(
		ctx,
		userIDs,
		usernames,
		normalizedSince,
		normalizedAnchor,
	)
	if err != nil {
		return UserChanges{}, err
	}

	sort.SliceStable(users, func(i, j int) bool {
		left := users[i].UpdateTime.UTC()
		right := users[j].UpdateTime.UTC()
		if left.Equal(right) {
			return users[i].ID < users[j].ID
		}
		return left.Before(right)
	})

	return UserChanges{
		Users:      users,
		SyncAnchor: normalizedAnchor,
	}, nil
}

func (s *UserService) ListFriends(ctx context.Context, userID int64) ([]models.User, error) {
	return s.store.ListFriends(ctx, userID)
}

func (s *UserService) AddFriend(ctx context.Context, userID int64, identifier string) (models.User, error) {
	friend, err := s.GetUserByIdentifier(ctx, identifier)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, ErrFriendNotFound
		}
		return models.User{}, err
	}
	if friend.ID == userID {
		return models.User{}, ErrCannotFriendSelf
	}
	if err := s.store.AddFriend(ctx, userID, friend.ID); err != nil {
		return models.User{}, err
	}
	return friend, nil
}

func (s *UserService) RemoveFriend(ctx context.Context, userID int64, identifier string) error {
	friend, err := s.GetUserByIdentifier(ctx, identifier)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFriendNotFound
		}
		return err
	}
	if friend.ID == userID {
		return ErrCannotFriendSelf
	}
	if err := s.store.RemoveFriend(ctx, userID, friend.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFriendNotFound
		}
		return err
	}
	return nil
}

func (s *UserService) UpdateUserAvatar(ctx context.Context, userID int64, avatarURL string) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		return s.store.UpdateUserAvatar(ctx, userID, strings.TrimSpace(avatarURL), "")
	})
}

func (s *UserService) UpdateUserAvatarThumbnail(ctx context.Context, userID int64, contentBase64 string, declaredType string) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		store := s.defaultAvatarStore()
		if store == nil {
			return models.User{}, fmt.Errorf("avatar storage is not configured")
		}
		content, err := decodeBase64Payload(contentBase64)
		if err != nil {
			return models.User{}, fmt.Errorf("invalid avatar content: %w", err)
		}
		if err := validateAvatarImage(content, declaredType); err != nil {
			return models.User{}, err
		}

		thumbnailData, err := buildThumbnailJPEG(bytes.NewReader(content))
		if err != nil || len(thumbnailData) == 0 {
			return models.User{}, fmt.Errorf("invalid avatar image")
		}

		if _, err := store.Put(ctx, avatarStorageKey(userID), thumbnailContentType, thumbnailData); err != nil {
			return models.User{}, fmt.Errorf("store avatar: %w", err)
		}
		return s.store.UpdateUserAvatar(ctx, userID, avatarPublicURL(userID), s.defaultAvatarStorageType())
	})
}

func (s *UserService) ClearUserAvatar(ctx context.Context, userID int64) (models.User, error) {
	return s.withUserAvatarLock(userID, func() (models.User, error) {
		user, err := s.store.GetUserByID(ctx, userID)
		if err != nil {
			return models.User{}, err
		}
		if store, ok := s.avatarStoreForType(user.AvatarStorageType); ok {
			if err := store.Delete(ctx, avatarStorageKey(userID)); err != nil {
				return models.User{}, fmt.Errorf("delete avatar: %w", err)
			}
		}
		return s.store.UpdateUserAvatar(ctx, userID, "", "")
	})
}

func (s *UserService) OpenUserAvatarStream(ctx context.Context, userID int64) (io.ReadCloser, error) {
	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	store, ok := s.avatarStoreForType(user.AvatarStorageType)
	if !ok {
		return nil, fmt.Errorf("avatar storage is not configured")
	}
	return store.Open(ctx, avatarStorageKey(userID))
}

func (s *UserService) PresignUserAvatarURL(ctx context.Context, userID int64) (string, bool, error) {
	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return "", false, err
	}
	store, ok := s.avatarStoreForType(user.AvatarStorageType)
	if !ok {
		return "", false, nil
	}
	s3Store, ok := store.(*storage.S3Store)
	if !ok {
		return "", false, nil
	}
	url, err := s3Store.PresignGetObjectURL(ctx, avatarStorageKey(userID), directDownloadURLTTL)
	if err != nil {
		return "", false, err
	}
	return url, true, nil
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

func normalizeUsername(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizeUserIdentifier(raw string) string {
	identifier := strings.TrimSpace(raw)
	identifier = strings.TrimPrefix(identifier, "users/")
	identifier = strings.TrimPrefix(identifier, "users\\")
	if pipe := strings.Index(identifier, "|"); pipe >= 0 {
		identifier = identifier[:pipe]
	}
	return strings.TrimSpace(identifier)
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

func avatarStorageKey(userID int64) string {
	return fmt.Sprintf("avatars/%d", userID)
}

func avatarPublicURL(userID int64) string {
	return fmt.Sprintf("/file/avatars/%d", userID)
}

func decodeBase64Payload(content string) ([]byte, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil, fmt.Errorf("empty content")
	}
	if comma := strings.Index(trimmed, ","); comma > 0 && strings.Contains(strings.ToLower(trimmed[:comma]), "base64") {
		trimmed = strings.TrimSpace(trimmed[comma+1:])
	}
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		return decoded, nil
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(trimmed); err == nil {
		return decoded, nil
	}
	return nil, fmt.Errorf("decode base64 failed")
}

func validateAvatarImage(content []byte, declaredType string) error {
	if len(content) == 0 {
		return fmt.Errorf("avatar content is empty")
	}
	if len(content) > avatarMaxSourceBytes {
		return fmt.Errorf("avatar content too large")
	}

	normalizedDeclaredType := strings.ToLower(strings.TrimSpace(declaredType))
	if normalizedDeclaredType != "" && !strings.HasPrefix(normalizedDeclaredType, "image/") {
		return fmt.Errorf("avatar type must be image")
	}

	detectedType := strings.ToLower(http.DetectContentType(content))
	if !strings.HasPrefix(detectedType, "image/") {
		return fmt.Errorf("avatar content must be image")
	}
	if normalizedDeclaredType != "" && normalizedDeclaredType != detectedType {
		return fmt.Errorf("avatar type mismatch")
	}

	config, _, err := image.DecodeConfig(bytes.NewReader(content))
	if err != nil {
		return fmt.Errorf("invalid avatar image")
	}
	if config.Width <= 0 || config.Height <= 0 {
		return fmt.Errorf("invalid avatar dimensions")
	}
	if config.Width > avatarMaxDimension || config.Height > avatarMaxDimension {
		return fmt.Errorf("avatar dimensions exceed limit")
	}
	if int64(config.Width)*int64(config.Height) > avatarMaxPixels {
		return fmt.Errorf("avatar pixel count exceed limit")
	}
	return nil
}

func (s *UserService) withUserAvatarLock(userID int64, fn func() (models.User, error)) (models.User, error) {
	lockValue, _ := s.avatarLocks.LoadOrStore(userID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func (s *UserService) defaultAvatarStore() storage.Store {
	if s.avatarStorage == nil {
		return nil
	}
	return s.avatarStorage.DefaultStore()
}

func (s *UserService) defaultAvatarStorageType() string {
	if s.avatarStorage == nil {
		return storage.TypeLocal
	}
	return s.avatarStorage.DefaultType()
}

func (s *UserService) avatarStoreForType(storeType string) (storage.Store, bool) {
	if s.avatarStorage == nil {
		return nil, false
	}
	return s.avatarStorage.StoreForType(storeType)
}
