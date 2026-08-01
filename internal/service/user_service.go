package service

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	MemoVisibility       string
	MemoEditGesture      string
	MemoColumns          []models.MemoColumnConfig
	ExploreDrawerEntries []models.ExploreDrawerEntryConfig
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

