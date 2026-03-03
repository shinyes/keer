package store

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) CreateUser(ctx context.Context, username string, displayName string, role string) (models.User, error) {
	return s.CreateUserWithProfile(ctx, username, displayName, "", role)
}

func (s *SQLStore) CreateUserWithProfile(ctx context.Context, username string, displayName string, passwordHash string, role string) (models.User, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO users (username, display_name, avatar_url, password_hash, role, default_visibility, create_time, update_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		username,
		displayName,
		"",
		passwordHash,
		role,
		models.VisibilityPrivate,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.User{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return models.User{}, err
	}
	return s.GetUserByID(ctx, id)
}

func (s *SQLStore) GetUserByID(ctx context.Context, id int64) (models.User, error) {
	var user models.User
	var defaultVisibility string
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, username, display_name, avatar_url, password_hash, role, default_visibility, create_time, update_time
		FROM users
		WHERE id = ?`,
		id,
	).Scan(
		&user.ID,
		&user.Username,
		&user.DisplayName,
		&user.AvatarURL,
		&user.PasswordHash,
		&user.Role,
		&defaultVisibility,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.User{}, err
	}
	user.DefaultVisibility = models.Visibility(defaultVisibility)
	user.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.User{}, err
	}
	user.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

func (s *SQLStore) GetUserByUsername(ctx context.Context, username string) (models.User, error) {
	var user models.User
	var defaultVisibility string
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, username, display_name, avatar_url, password_hash, role, default_visibility, create_time, update_time
		FROM users
		WHERE username = ? COLLATE NOCASE`,
		username,
	).Scan(
		&user.ID,
		&user.Username,
		&user.DisplayName,
		&user.AvatarURL,
		&user.PasswordHash,
		&user.Role,
		&defaultVisibility,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.User{}, err
	}
	user.DefaultVisibility = models.Visibility(defaultVisibility)
	user.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.User{}, err
	}
	user.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

func (s *SQLStore) ListUsersByIdentifiersUpdatedWithin(
	ctx context.Context,
	userIDs []int64,
	usernames []string,
	updatedAfter time.Time,
	updatedBeforeOrEqual time.Time,
) ([]models.User, error) {
	if len(userIDs) == 0 && len(usernames) == 0 {
		return []models.User{}, nil
	}

	query := `SELECT id, username, display_name, avatar_url, password_hash, role, default_visibility, create_time, update_time
		FROM users
		WHERE (`
	args := make([]any, 0, len(userIDs)+len(usernames)+2)
	conditions := make([]string, 0, 2)

	if len(userIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(userIDs)), ",")
		conditions = append(conditions, `id IN (`+placeholders+`)`)
		for _, userID := range userIDs {
			args = append(args, userID)
		}
	}
	if len(usernames) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(usernames)), ",")
		conditions = append(conditions, `username COLLATE NOCASE IN (`+placeholders+`)`)
		for _, username := range usernames {
			args = append(args, username)
		}
	}

	query += strings.Join(conditions, " OR ")
	query += `)
		AND update_time > ?
		AND update_time <= ?
		ORDER BY update_time ASC, id ASC`
	args = append(args, updatedAfter.UTC().Format(time.RFC3339Nano))
	args = append(args, updatedBeforeOrEqual.UTC().Format(time.RFC3339Nano))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]models.User, 0, len(userIDs)+len(usernames))
	for rows.Next() {
		var user models.User
		var defaultVisibility string
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&user.ID,
			&user.Username,
			&user.DisplayName,
			&user.AvatarURL,
			&user.PasswordHash,
			&user.Role,
			&defaultVisibility,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		user.DefaultVisibility = models.Visibility(defaultVisibility)
		user.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		user.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return users, nil
}

func (s *SQLStore) CreatePersonalAccessToken(ctx context.Context, userID int64, rawToken string, description string) (models.PersonalAccessToken, error) {
	return s.CreatePersonalAccessTokenWithExpiry(ctx, userID, rawToken, description, nil)
}

func (s *SQLStore) CreatePersonalAccessTokenWithExpiry(ctx context.Context, userID int64, rawToken string, description string, expiresAt *time.Time) (models.PersonalAccessToken, error) {
	now := time.Now().UTC()
	tokenHash := HashToken(rawToken)
	tokenPrefix := rawToken
	if len(tokenPrefix) > 8 {
		tokenPrefix = tokenPrefix[:8]
	}
	var expiresValue any
	if expiresAt != nil {
		expiresValue = expiresAt.UTC().Format(time.RFC3339Nano)
	}
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO personal_access_tokens (user_id, token_prefix, token_hash, description, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		userID,
		tokenPrefix,
		tokenHash,
		description,
		now.Format(time.RFC3339Nano),
		expiresValue,
	)
	if err != nil {
		return models.PersonalAccessToken{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return models.PersonalAccessToken{}, err
	}
	return s.GetPersonalAccessTokenByID(ctx, id)
}

func (s *SQLStore) GetPersonalAccessTokenByID(ctx context.Context, id int64) (models.PersonalAccessToken, error) {
	var token models.PersonalAccessToken
	var createdAt string
	var lastUsedAt sql.NullString
	var expiresAt sql.NullString
	var revokedAt sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, user_id, token_prefix, token_hash, description, created_at, last_used_at, expires_at, revoked_at
		FROM personal_access_tokens WHERE id = ?`,
		id,
	).Scan(
		&token.ID,
		&token.UserID,
		&token.TokenPrefix,
		&token.TokenHash,
		&token.Description,
		&createdAt,
		&lastUsedAt,
		&expiresAt,
		&revokedAt,
	)
	if err != nil {
		return models.PersonalAccessToken{}, err
	}
	var errParse error
	token.CreatedAt, errParse = parseTime(createdAt)
	if errParse != nil {
		return models.PersonalAccessToken{}, errParse
	}
	token.LastUsedAt, errParse = parseNullableTime(lastUsedAt)
	if errParse != nil {
		return models.PersonalAccessToken{}, errParse
	}
	token.ExpiresAt, errParse = parseNullableTime(expiresAt)
	if errParse != nil {
		return models.PersonalAccessToken{}, errParse
	}
	token.RevokedAt, errParse = parseNullableTime(revokedAt)
	if errParse != nil {
		return models.PersonalAccessToken{}, errParse
	}
	return token, nil
}

func (s *SQLStore) ListPersonalAccessTokensByUserID(ctx context.Context, userID int64) ([]models.PersonalAccessToken, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, user_id, token_prefix, token_hash, description, created_at, last_used_at, expires_at, revoked_at
		FROM personal_access_tokens
		WHERE user_id = ?
		ORDER BY created_at DESC, id DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.PersonalAccessToken, 0)
	for rows.Next() {
		var token models.PersonalAccessToken
		var createdAt string
		var lastUsedAt sql.NullString
		var expiresAt sql.NullString
		var revokedAt sql.NullString
		if err := rows.Scan(
			&token.ID,
			&token.UserID,
			&token.TokenPrefix,
			&token.TokenHash,
			&token.Description,
			&createdAt,
			&lastUsedAt,
			&expiresAt,
			&revokedAt,
		); err != nil {
			return nil, err
		}
		var parseErr error
		token.CreatedAt, parseErr = parseTime(createdAt)
		if parseErr != nil {
			return nil, parseErr
		}
		token.LastUsedAt, parseErr = parseNullableTime(lastUsedAt)
		if parseErr != nil {
			return nil, parseErr
		}
		token.ExpiresAt, parseErr = parseNullableTime(expiresAt)
		if parseErr != nil {
			return nil, parseErr
		}
		token.RevokedAt, parseErr = parseNullableTime(revokedAt)
		if parseErr != nil {
			return nil, parseErr
		}
		result = append(result, token)
	}
	return result, rows.Err()
}

func (s *SQLStore) RevokePersonalAccessToken(ctx context.Context, tokenID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE personal_access_tokens
		SET revoked_at = ?
		WHERE id = ? AND revoked_at IS NULL`,
		time.Now().UTC().Format(time.RFC3339Nano),
		tokenID,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLStore) GetUserByToken(ctx context.Context, rawToken string) (models.User, models.PersonalAccessToken, error) {
	tokenHash := HashToken(rawToken)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var user models.User
	var token models.PersonalAccessToken
	var defaultVisibility string
	var userCreateTime string
	var userUpdateTime string
	var tokenCreateTime string
	var lastUsedAt sql.NullString
	var expiresAt sql.NullString
	var revokedAt sql.NullString

	err := s.db.QueryRowContext(
		ctx,
		`SELECT
			u.id, u.username, u.display_name, u.avatar_url, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time,
			t.id, t.user_id, t.token_prefix, t.token_hash, t.description, t.created_at, t.last_used_at, t.expires_at, t.revoked_at
		FROM personal_access_tokens t
		JOIN users u ON u.id = t.user_id
		WHERE t.token_hash = ?
			AND t.revoked_at IS NULL
			AND (t.expires_at IS NULL OR t.expires_at > ?)`,
		tokenHash,
		now,
	).Scan(
		&user.ID,
		&user.Username,
		&user.DisplayName,
		&user.AvatarURL,
		&user.PasswordHash,
		&user.Role,
		&defaultVisibility,
		&userCreateTime,
		&userUpdateTime,
		&token.ID,
		&token.UserID,
		&token.TokenPrefix,
		&token.TokenHash,
		&token.Description,
		&tokenCreateTime,
		&lastUsedAt,
		&expiresAt,
		&revokedAt,
	)
	if err != nil {
		return models.User{}, models.PersonalAccessToken{}, err
	}

	user.DefaultVisibility = models.Visibility(defaultVisibility)
	var errParse error
	user.CreateTime, errParse = parseTime(userCreateTime)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	user.UpdateTime, errParse = parseTime(userUpdateTime)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	token.CreatedAt, errParse = parseTime(tokenCreateTime)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	token.LastUsedAt, errParse = parseNullableTime(lastUsedAt)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	token.ExpiresAt, errParse = parseNullableTime(expiresAt)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	token.RevokedAt, errParse = parseNullableTime(revokedAt)
	if errParse != nil {
		return models.User{}, models.PersonalAccessToken{}, errParse
	}
	return user, token, nil
}

func (s *SQLStore) UpdateUserAvatar(ctx context.Context, userID int64, avatarURL string) (models.User, error) {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE users
		SET avatar_url = ?, update_time = ?
		WHERE id = ?`,
		avatarURL,
		time.Now().UTC().Format(time.RFC3339Nano),
		userID,
	)
	if err != nil {
		return models.User{}, err
	}
	return s.GetUserByID(ctx, userID)
}

func (s *SQLStore) CountUsers(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM users`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLStore) TouchPersonalAccessToken(ctx context.Context, tokenID int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE personal_access_tokens SET last_used_at = ? WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		tokenID,
	)
	return err
}
