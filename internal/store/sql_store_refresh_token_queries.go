package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) CreateRefreshToken(ctx context.Context, userID int64, rawToken string, expiresAt time.Time) (models.RefreshToken, error) {
	now := time.Now().UTC()
	tokenHash := HashToken(rawToken)
	tokenPrefix := rawToken
	if len(tokenPrefix) > 8 {
		tokenPrefix = tokenPrefix[:8]
	}
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO refresh_tokens (user_id, token_prefix, token_hash, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`,
		userID,
		tokenPrefix,
		tokenHash,
		now.Format(time.RFC3339Nano),
		expiresAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.RefreshToken{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return models.RefreshToken{}, err
	}
	return s.GetRefreshTokenByID(ctx, id)
}

func (s *SQLStore) GetRefreshTokenByID(ctx context.Context, id int64) (models.RefreshToken, error) {
	var token models.RefreshToken
	var createdAt string
	var lastUsedAt sql.NullString
	var expiresAt string
	var revokedAt sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, user_id, token_prefix, token_hash, created_at, last_used_at, expires_at, revoked_at
		FROM refresh_tokens
		WHERE id = ?`,
		id,
	).Scan(
		&token.ID,
		&token.UserID,
		&token.TokenPrefix,
		&token.TokenHash,
		&createdAt,
		&lastUsedAt,
		&expiresAt,
		&revokedAt,
	)
	if err != nil {
		return models.RefreshToken{}, err
	}
	var parseErr error
	token.CreatedAt, parseErr = parseTime(createdAt)
	if parseErr != nil {
		return models.RefreshToken{}, parseErr
	}
	token.LastUsedAt, parseErr = parseNullableTime(lastUsedAt)
	if parseErr != nil {
		return models.RefreshToken{}, parseErr
	}
	token.ExpiresAt, parseErr = parseTime(expiresAt)
	if parseErr != nil {
		return models.RefreshToken{}, parseErr
	}
	token.RevokedAt, parseErr = parseNullableTime(revokedAt)
	if parseErr != nil {
		return models.RefreshToken{}, parseErr
	}
	return token, nil
}

func (s *SQLStore) GetUserByRefreshToken(ctx context.Context, rawToken string) (models.User, models.RefreshToken, error) {
	tokenHash := HashToken(rawToken)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var user models.User
	var token models.RefreshToken
	var defaultVisibility string
	var userCreateTime string
	var userUpdateTime string
	var tokenCreateTime string
	var lastUsedAt sql.NullString
	var expiresAt string
	var revokedAt sql.NullString

	err := s.db.QueryRowContext(
		ctx,
		`SELECT
			u.id, u.username, u.avatar_url, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time,
			t.id, t.user_id, t.token_prefix, t.token_hash, t.created_at, t.last_used_at, t.expires_at, t.revoked_at
		FROM refresh_tokens t
		JOIN users u ON u.id = t.user_id
		WHERE t.token_hash = ?
			AND t.revoked_at IS NULL
			AND t.expires_at > ?`,
		tokenHash,
		now,
	).Scan(
		&user.ID,
		&user.Username,
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
		&tokenCreateTime,
		&lastUsedAt,
		&expiresAt,
		&revokedAt,
	)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}

	user.DefaultVisibility = models.Visibility(defaultVisibility)
	var parseErr error
	user.CreateTime, parseErr = parseTime(userCreateTime)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	user.UpdateTime, parseErr = parseTime(userUpdateTime)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	token.CreatedAt, parseErr = parseTime(tokenCreateTime)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	token.LastUsedAt, parseErr = parseNullableTime(lastUsedAt)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	token.ExpiresAt, parseErr = parseTime(expiresAt)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	token.RevokedAt, parseErr = parseNullableTime(revokedAt)
	if parseErr != nil {
		return models.User{}, models.RefreshToken{}, parseErr
	}
	return user, token, nil
}

func (s *SQLStore) RotateRefreshToken(
	ctx context.Context,
	rawToken string,
	newRawToken string,
	expiresAt time.Time,
) (models.User, models.RefreshToken, error) {
	now := time.Now().UTC()
	oldTokenHash := HashToken(rawToken)
	newTokenHash := HashToken(newRawToken)
	newTokenPrefix := newRawToken
	if len(newTokenPrefix) > 8 {
		newTokenPrefix = newTokenPrefix[:8]
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	var user models.User
	var oldTokenID int64
	var defaultVisibility string
	var userCreateTime string
	var userUpdateTime string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT
			u.id, u.username, u.avatar_url, u.avatar_storage_type, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time,
			t.id
		FROM refresh_tokens t
		JOIN users u ON u.id = t.user_id
		WHERE t.token_hash = ?
			AND t.revoked_at IS NULL
			AND t.expires_at > ?`,
		oldTokenHash,
		now.Format(time.RFC3339Nano),
	).Scan(
		&user.ID,
		&user.Username,
		&user.AvatarURL,
		&user.AvatarStorageType,
		&user.PasswordHash,
		&user.Role,
		&defaultVisibility,
		&userCreateTime,
		&userUpdateTime,
		&oldTokenID,
	); err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	user.DefaultVisibility = models.Visibility(defaultVisibility)
	user.CreateTime, err = parseTime(userCreateTime)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	user.UpdateTime, err = parseTime(userUpdateTime)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE refresh_tokens
		SET last_used_at = ?, revoked_at = ?
		WHERE id = ? AND revoked_at IS NULL`,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		oldTokenID,
	)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	if affected == 0 {
		return models.User{}, models.RefreshToken{}, sql.ErrNoRows
	}

	res, err = tx.ExecContext(
		ctx,
		`INSERT INTO refresh_tokens (user_id, token_prefix, token_hash, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`,
		user.ID,
		newTokenPrefix,
		newTokenHash,
		now.Format(time.RFC3339Nano),
		expiresAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	newTokenID, err := res.LastInsertId()
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}

	if err := tx.Commit(); err != nil {
		return models.User{}, models.RefreshToken{}, err
	}

	newToken, err := s.GetRefreshTokenByID(ctx, newTokenID)
	if err != nil {
		return models.User{}, models.RefreshToken{}, err
	}
	return user, newToken, nil
}

func (s *SQLStore) TouchRefreshToken(ctx context.Context, tokenID int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE refresh_tokens SET last_used_at = ? WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		tokenID,
	)
	return err
}

func (s *SQLStore) RevokeRefreshToken(ctx context.Context, tokenID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE refresh_tokens
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
