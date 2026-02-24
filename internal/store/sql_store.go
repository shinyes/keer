package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

type SQLStore struct {
	db *sql.DB
}

func New(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

func (s *SQLStore) DB() *sql.DB {
	return s.db
}

type MemoUpdate struct {
	Content     *string
	Visibility  *models.Visibility
	State       *models.MemoState
	Pinned      *bool
	DisplayTime *time.Time
	Payload     *models.MemoPayload
}

func (s *SQLStore) CreateUser(ctx context.Context, username string, displayName string, role string) (models.User, error) {
	return s.CreateUserWithProfile(ctx, username, displayName, "", role)
}

func (s *SQLStore) CreateUserWithProfile(ctx context.Context, username string, displayName string, passwordHash string, role string) (models.User, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO users (username, display_name, password_hash, role, default_visibility, create_time, update_time)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		username,
		displayName,
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
		`SELECT id, username, display_name, password_hash, role, default_visibility, create_time, update_time
		FROM users
		WHERE id = ?`,
		id,
	).Scan(
		&user.ID,
		&user.Username,
		&user.DisplayName,
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
		`SELECT id, username, display_name, password_hash, role, default_visibility, create_time, update_time
		FROM users
		WHERE username = ? COLLATE NOCASE`,
		username,
	).Scan(
		&user.ID,
		&user.Username,
		&user.DisplayName,
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
			u.id, u.username, u.display_name, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time,
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

func (s *SQLStore) CreateMemo(ctx context.Context, creatorID int64, content string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, displayTime time.Time) (models.Memo, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return models.Memo{}, err
	}
	now := time.Now().UTC()
	pinnedInt := 0
	if pinned {
		pinnedInt = 1
	}
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO memos (creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		content,
		visibility,
		state,
		pinnedInt,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		displayTime.UTC().Format(time.RFC3339Nano),
		string(payloadJSON),
	)
	if err != nil {
		return models.Memo{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return models.Memo{}, err
	}
	return s.GetMemoByID(ctx, id)
}

func (s *SQLStore) CreateMemoWithAttachments(ctx context.Context, creatorID int64, content string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, displayTime time.Time, attachmentIDs []int64) (models.Memo, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return models.Memo{}, err
	}
	now := time.Now().UTC()
	pinnedInt := 0
	if pinned {
		pinnedInt = 1
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Memo{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO memos (creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		content,
		visibility,
		state,
		pinnedInt,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		displayTime.UTC().Format(time.RFC3339Nano),
		string(payloadJSON),
	)
	if err != nil {
		return models.Memo{}, err
	}
	memoID, err := res.LastInsertId()
	if err != nil {
		return models.Memo{}, err
	}
	if err := setMemoAttachmentsInTx(ctx, tx, memoID, attachmentIDs); err != nil {
		return models.Memo{}, err
	}
	if err := tx.Commit(); err != nil {
		return models.Memo{}, err
	}
	return s.GetMemoByID(ctx, memoID)
}

func (s *SQLStore) GetMemoByID(ctx context.Context, id int64) (models.Memo, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json
		FROM memos
		WHERE id = ?`,
		id,
	)
	return scanMemo(row)
}

func (s *SQLStore) UpdateMemo(ctx context.Context, memoID int64, update MemoUpdate) (models.Memo, error) {
	assignments := make([]string, 0, 8)
	args := make([]any, 0, 8)

	if update.Content != nil {
		assignments = append(assignments, "content = ?")
		args = append(args, *update.Content)
	}
	if update.Visibility != nil {
		assignments = append(assignments, "visibility = ?")
		args = append(args, *update.Visibility)
	}
	if update.State != nil {
		assignments = append(assignments, "state = ?")
		args = append(args, *update.State)
	}
	if update.Pinned != nil {
		pinnedInt := 0
		if *update.Pinned {
			pinnedInt = 1
		}
		assignments = append(assignments, "pinned = ?")
		args = append(args, pinnedInt)
	}
	if update.DisplayTime != nil {
		assignments = append(assignments, "display_time = ?")
		args = append(args, update.DisplayTime.UTC().Format(time.RFC3339Nano))
	}
	if update.Payload != nil {
		payloadJSON, err := json.Marshal(*update.Payload)
		if err != nil {
			return models.Memo{}, err
		}
		assignments = append(assignments, "payload_json = ?")
		args = append(args, string(payloadJSON))
	}

	assignments = append(assignments, "update_time = ?")
	args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
	args = append(args, memoID)

	query := fmt.Sprintf(`UPDATE memos SET %s WHERE id = ?`, strings.Join(assignments, ", "))
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return models.Memo{}, err
	}
	return s.GetMemoByID(ctx, memoID)
}

func (s *SQLStore) UpdateMemoWithAttachments(ctx context.Context, memoID int64, update MemoUpdate, attachmentIDs *[]int64) (models.Memo, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Memo{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	assignments := make([]string, 0, 8)
	args := make([]any, 0, 8)

	if update.Content != nil {
		assignments = append(assignments, "content = ?")
		args = append(args, *update.Content)
	}
	if update.Visibility != nil {
		assignments = append(assignments, "visibility = ?")
		args = append(args, *update.Visibility)
	}
	if update.State != nil {
		assignments = append(assignments, "state = ?")
		args = append(args, *update.State)
	}
	if update.Pinned != nil {
		pinnedInt := 0
		if *update.Pinned {
			pinnedInt = 1
		}
		assignments = append(assignments, "pinned = ?")
		args = append(args, pinnedInt)
	}
	if update.DisplayTime != nil {
		assignments = append(assignments, "display_time = ?")
		args = append(args, update.DisplayTime.UTC().Format(time.RFC3339Nano))
	}
	if update.Payload != nil {
		payloadJSON, err := json.Marshal(*update.Payload)
		if err != nil {
			return models.Memo{}, err
		}
		assignments = append(assignments, "payload_json = ?")
		args = append(args, string(payloadJSON))
	}

	assignments = append(assignments, "update_time = ?")
	args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
	args = append(args, memoID)

	query := fmt.Sprintf(`UPDATE memos SET %s WHERE id = ?`, strings.Join(assignments, ", "))
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return models.Memo{}, err
	}

	if attachmentIDs != nil {
		if err := setMemoAttachmentsInTx(ctx, tx, memoID, *attachmentIDs); err != nil {
			return models.Memo{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return models.Memo{}, err
	}
	return s.GetMemoByID(ctx, memoID)
}

func (s *SQLStore) DeleteMemo(ctx context.Context, memoID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM memos WHERE id = ?`, memoID)
	return err
}

func (s *SQLStore) ListVisibleMemos(ctx context.Context, viewerID int64, state *models.MemoState, prefilter MemoSQLPrefilter, limit int, offset int) ([]models.Memo, error) {
	if prefilter.Unsatisfiable {
		return []models.Memo{}, nil
	}

	query := `SELECT m.id, m.creator_id, m.content, m.visibility, m.state, m.pinned, m.create_time, m.update_time, m.display_time, m.payload_json
		FROM memos m
		WHERE (m.creator_id = ? OR m.visibility IN ('PUBLIC', 'PROTECTED'))`
	args := []any{viewerID}

	if state != nil {
		query += ` AND m.state = ?`
		args = append(args, *state)
	}

	if len(prefilter.CreatorIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(prefilter.CreatorIDs)), ",")
		query += ` AND m.creator_id IN (` + placeholders + `)`
		for _, id := range prefilter.CreatorIDs {
			args = append(args, id)
		}
	}

	if len(prefilter.VisibilityIn) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(prefilter.VisibilityIn)), ",")
		query += ` AND m.visibility IN (` + placeholders + `)`
		for _, v := range prefilter.VisibilityIn {
			args = append(args, v)
		}
	}

	if len(prefilter.StateIn) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(prefilter.StateIn)), ",")
		query += ` AND m.state IN (` + placeholders + `)`
		for _, st := range prefilter.StateIn {
			args = append(args, st)
		}
	}

	if prefilter.Pinned != nil {
		query += ` AND m.pinned = ?`
		args = append(args, boolToSQLiteInt(*prefilter.Pinned))
	}

	addJSONBoolConstraint := func(path string, value *bool) {
		if value == nil {
			return
		}
		query += fmt.Sprintf(` AND COALESCE(JSON_EXTRACT(m.payload_json, '$.property.%s'), 0) = ?`, path)
		args = append(args, boolToSQLiteInt(*value))
	}
	addJSONBoolConstraint("hasLink", prefilter.HasLink)
	addJSONBoolConstraint("hasTaskList", prefilter.HasTaskList)
	addJSONBoolConstraint("hasCode", prefilter.HasCode)
	addJSONBoolConstraint("hasIncompleteTasks", prefilter.HasIncompleteTasks)

	for _, group := range prefilter.TagGroups {
		if len(group.Options) == 0 {
			continue
		}
		query += ` AND EXISTS (SELECT 1 FROM json_each(m.payload_json, '$.tags') jt WHERE `
		groupClauses := make([]string, 0, len(group.Options))
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				groupClauses = append(groupClauses, `jt.value = ?`)
				args = append(args, option.Value)
			case TagMatchPrefix:
				groupClauses = append(groupClauses, `jt.value LIKE ?`)
				args = append(args, option.Value+"%")
			}
		}
		if len(groupClauses) == 0 {
			continue
		}
		query += strings.Join(groupClauses, " OR ") + `)`
	}
	for _, group := range prefilter.ExcludeTagGroups {
		if len(group.Options) == 0 {
			continue
		}
		query += ` AND NOT EXISTS (SELECT 1 FROM json_each(m.payload_json, '$.tags') jt WHERE `
		groupClauses := make([]string, 0, len(group.Options))
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				groupClauses = append(groupClauses, `jt.value = ?`)
				args = append(args, option.Value)
			case TagMatchPrefix:
				groupClauses = append(groupClauses, `jt.value LIKE ?`)
				args = append(args, option.Value+"%")
			}
		}
		if len(groupClauses) == 0 {
			continue
		}
		query += strings.Join(groupClauses, " OR ") + `)`
	}

	query += ` ORDER BY m.display_time DESC, m.id DESC`
	if limit > 0 {
		query += ` LIMIT ? OFFSET ?`
		args = append(args, limit, offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	memos := make([]models.Memo, 0)
	for rows.Next() {
		memo, err := scanMemo(rows)
		if err != nil {
			return nil, err
		}
		memos = append(memos, memo)
	}
	return memos, rows.Err()
}

func (s *SQLStore) ListVisibleMemosByCreator(ctx context.Context, creatorID int64, viewerID int64, state models.MemoState) ([]models.Memo, error) {
	query := `SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json
		FROM memos
		WHERE creator_id = ? AND state = ?`
	args := []any{creatorID, state}
	if creatorID != viewerID {
		query += ` AND visibility IN ('PUBLIC', 'PROTECTED')`
	}
	query += ` ORDER BY display_time DESC, id DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Memo, 0)
	for rows.Next() {
		memo, err := scanMemo(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, memo)
	}
	return result, rows.Err()
}

func (s *SQLStore) ListAllMemos(ctx context.Context) ([]models.Memo, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json
		FROM memos
		ORDER BY id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Memo, 0)
	for rows.Next() {
		memo, err := scanMemo(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, memo)
	}
	return result, rows.Err()
}

func (s *SQLStore) UpdateMemoPayload(ctx context.Context, memoID int64, payload models.MemoPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE memos SET payload_json = ? WHERE id = ?`, string(data), memoID)
	return err
}

func (s *SQLStore) CreateAttachment(ctx context.Context, creatorID int64, filename string, externalLink string, fileType string, size int64, contentHash string, storageType string, storageKey string) (models.Attachment, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO attachments (creator_id, filename, external_link, type, size, content_hash, storage_type, storage_key, create_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		filename,
		externalLink,
		fileType,
		size,
		contentHash,
		storageType,
		storageKey,
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.Attachment{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return models.Attachment{}, err
	}
	attachment, err := s.GetAttachmentByID(ctx, id)
	if err != nil {
		return models.Attachment{}, err
	}
	return attachment, nil
}

func (s *SQLStore) CreateAttachmentUploadSession(ctx context.Context, session models.AttachmentUploadSession) (models.AttachmentUploadSession, error) {
	if session.ID == "" {
		return models.AttachmentUploadSession{}, fmt.Errorf("upload session id is required")
	}
	now := time.Now().UTC()
	createTime := session.CreateTime
	if createTime.IsZero() {
		createTime = now
	}
	updateTime := session.UpdateTime
	if updateTime.IsZero() {
		updateTime = now
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO attachment_upload_sessions (id, creator_id, filename, type, size, memo_name, temp_path, received_size, create_time, update_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID,
		session.CreatorID,
		session.Filename,
		session.Type,
		session.Size,
		session.MemoName,
		session.TempPath,
		session.ReceivedSize,
		createTime.Format(time.RFC3339Nano),
		updateTime.Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	return s.GetAttachmentUploadSessionByID(ctx, session.ID)
}

func (s *SQLStore) GetAttachmentUploadSessionByID(ctx context.Context, id string) (models.AttachmentUploadSession, error) {
	var session models.AttachmentUploadSession
	var memoName sql.NullString
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, creator_id, filename, type, size, memo_name, temp_path, received_size, create_time, update_time
		FROM attachment_upload_sessions
		WHERE id = ?`,
		id,
	).Scan(
		&session.ID,
		&session.CreatorID,
		&session.Filename,
		&session.Type,
		&session.Size,
		&memoName,
		&session.TempPath,
		&session.ReceivedSize,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	if memoName.Valid {
		session.MemoName = &memoName.String
	}
	session.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	session.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.AttachmentUploadSession{}, err
	}
	return session, nil
}

func (s *SQLStore) UpdateAttachmentUploadSessionOffset(ctx context.Context, id string, expectedOffset int64, newOffset int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE attachment_upload_sessions
		SET received_size = ?, update_time = ?
		WHERE id = ? AND received_size = ?`,
		newOffset,
		time.Now().UTC().Format(time.RFC3339Nano),
		id,
		expectedOffset,
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

func (s *SQLStore) DeleteAttachmentUploadSessionByID(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM attachment_upload_sessions WHERE id = ?`, id)
	return err
}

func (s *SQLStore) FindAttachmentByContentHash(ctx context.Context, creatorID int64, contentHash string) (models.Attachment, bool, error) {
	var attachment models.Attachment
	var createTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, create_time
		FROM attachments
		WHERE creator_id = ? AND content_hash = ?
		ORDER BY id DESC
		LIMIT 1`,
		creatorID,
		contentHash,
	).Scan(
		&attachment.ID,
		&attachment.CreatorID,
		&attachment.Filename,
		&attachment.ExternalLink,
		&attachment.Type,
		&attachment.Size,
		&attachment.StorageType,
		&attachment.StorageKey,
		&createTime,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return models.Attachment{}, false, nil
		}
		return models.Attachment{}, false, err
	}
	attachment.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Attachment{}, false, err
	}
	return attachment, true, nil
}

func (s *SQLStore) ListAttachmentCandidates(ctx context.Context, creatorID int64, filename string, fileType string, size int64, limit int) ([]models.Attachment, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, create_time
		FROM attachments
		WHERE creator_id = ? AND filename = ? AND type = ? AND size = ?
		ORDER BY id DESC
		LIMIT ?`,
		creatorID,
		filename,
		fileType,
		size,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Attachment, 0)
	for rows.Next() {
		attachment, scanErr := scanAttachment(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, attachment)
	}
	return result, rows.Err()
}

func (s *SQLStore) GetAttachmentByID(ctx context.Context, id int64) (models.Attachment, error) {
	var attachment models.Attachment
	var createTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, create_time
		FROM attachments
		WHERE id = ?`,
		id,
	).Scan(
		&attachment.ID,
		&attachment.CreatorID,
		&attachment.Filename,
		&attachment.ExternalLink,
		&attachment.Type,
		&attachment.Size,
		&attachment.StorageType,
		&attachment.StorageKey,
		&createTime,
	)
	if err != nil {
		return models.Attachment{}, err
	}
	attachment.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Attachment{}, err
	}
	return attachment, nil
}

func (s *SQLStore) ListAttachmentsByCreator(ctx context.Context, creatorID int64) ([]models.Attachment, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, create_time
		FROM attachments
		WHERE creator_id = ?
		ORDER BY id DESC`,
		creatorID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Attachment, 0)
	for rows.Next() {
		attachment, err := scanAttachment(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, attachment)
	}
	return result, rows.Err()
}

func (s *SQLStore) DeleteAttachment(ctx context.Context, attachmentID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM attachments WHERE id = ?`, attachmentID)
	return err
}

func (s *SQLStore) CountAttachmentsByStorageKey(ctx context.Context, storageKey string) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM attachments WHERE storage_key = ?`, storageKey).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLStore) SetMemoAttachments(ctx context.Context, memoID int64, attachmentIDs []int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := setMemoAttachmentsInTx(ctx, tx, memoID, attachmentIDs); err != nil {
		return err
	}
	return tx.Commit()
}

func setMemoAttachmentsInTx(ctx context.Context, tx *sql.Tx, memoID int64, attachmentIDs []int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memo_attachments WHERE memo_id = ?`, memoID); err != nil {
		return err
	}
	for i, attachmentID := range attachmentIDs {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memo_attachments (memo_id, attachment_id, position) VALUES (?, ?, ?)`,
			memoID,
			attachmentID,
			i,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLStore) ListAttachmentsByMemoIDs(ctx context.Context, memoIDs []int64) (map[int64][]models.Attachment, error) {
	result := make(map[int64][]models.Attachment)
	if len(memoIDs) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(memoIDs))
	args := make([]any, 0, len(memoIDs))
	for _, memoID := range memoIDs {
		placeholders = append(placeholders, "?")
		args = append(args, memoID)
	}

	query := fmt.Sprintf(
		`SELECT ma.memo_id, a.id, a.creator_id, a.filename, a.external_link, a.type, a.size, a.storage_type, a.storage_key, a.create_time
		FROM memo_attachments ma
		JOIN attachments a ON a.id = ma.attachment_id
		WHERE ma.memo_id IN (%s)
		ORDER BY ma.memo_id, ma.position ASC, ma.attachment_id ASC`,
		strings.Join(placeholders, ","),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var memoID int64
		var attachment models.Attachment
		var createTime string
		if err := rows.Scan(
			&memoID,
			&attachment.ID,
			&attachment.CreatorID,
			&attachment.Filename,
			&attachment.ExternalLink,
			&attachment.Type,
			&attachment.Size,
			&attachment.StorageType,
			&attachment.StorageKey,
			&createTime,
		); err != nil {
			return nil, err
		}
		attachment.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		result[memoID] = append(result[memoID], attachment)
	}
	return result, rows.Err()
}

func (s *SQLStore) AttachmentBelongsToUser(ctx context.Context, attachmentID int64, userID int64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM attachments WHERE id = ? AND creator_id = ?`,
		attachmentID,
		userID,
	).Scan(&count)
	return count > 0, err
}

func (s *SQLStore) GetMemoByIDAndCreator(ctx context.Context, memoID int64, creatorID int64) (models.Memo, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, payload_json
		FROM memos
		WHERE id = ? AND creator_id = ?`,
		memoID,
		creatorID,
	)
	return scanMemo(row)
}

func scanMemo(scanner interface {
	Scan(dest ...any) error
}) (models.Memo, error) {
	var memo models.Memo
	var visibility string
	var state string
	var pinned int
	var createTime string
	var updateTime string
	var displayTime string
	var payloadJSON string
	if err := scanner.Scan(
		&memo.ID,
		&memo.CreatorID,
		&memo.Content,
		&visibility,
		&state,
		&pinned,
		&createTime,
		&updateTime,
		&displayTime,
		&payloadJSON,
	); err != nil {
		return models.Memo{}, err
	}
	memo.Visibility = models.Visibility(visibility)
	memo.State = models.MemoState(state)
	memo.Pinned = pinned == 1
	var err error
	memo.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Memo{}, err
	}
	memo.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.Memo{}, err
	}
	memo.DisplayTime, err = parseTime(displayTime)
	if err != nil {
		return models.Memo{}, err
	}
	if strings.TrimSpace(payloadJSON) == "" {
		payloadJSON = "{}"
	}
	if err := json.Unmarshal([]byte(payloadJSON), &memo.Payload); err != nil {
		return models.Memo{}, err
	}
	if memo.Payload.Tags == nil {
		memo.Payload.Tags = []string{}
	}
	return memo, nil
}

func scanAttachment(scanner interface {
	Scan(dest ...any) error
}) (models.Attachment, error) {
	var attachment models.Attachment
	var createTime string
	if err := scanner.Scan(
		&attachment.ID,
		&attachment.CreatorID,
		&attachment.Filename,
		&attachment.ExternalLink,
		&attachment.Type,
		&attachment.Size,
		&attachment.StorageType,
		&attachment.StorageKey,
		&createTime,
	); err != nil {
		return models.Attachment{}, err
	}
	var err error
	attachment.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Attachment{}, err
	}
	return attachment, nil
}

func parseTime(raw string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, raw)
}

func parseNullableTime(raw sql.NullString) (*time.Time, error) {
	if !raw.Valid {
		return nil, nil
	}
	t, err := parseTime(raw.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func boolToSQLiteInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
