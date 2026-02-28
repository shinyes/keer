package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
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
	Content      *string
	Visibility   *models.Visibility
	State        *models.MemoState
	Pinned       *bool
	LatitudeSet  bool
	Latitude     *float64
	LongitudeSet bool
	Longitude    *float64
	Payload      *models.MemoPayload
}

type MemoQueryBounds struct {
	UpdatedAfter         *time.Time
	UpdatedBeforeOrEqual *time.Time
}

const (
	memoChangeEventTypeDelete            = "DELETE"
	memoChangeEventTypeVisibilityRevoked = "VISIBILITY_REVOKED"
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

func (s *SQLStore) CreateMemo(ctx context.Context, creatorID int64, content string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, createTime time.Time, latitude *float64, longitude *float64) (models.Memo, error) {
	return s.CreateMemoWithAttachments(
		ctx,
		creatorID,
		content,
		visibility,
		state,
		pinned,
		payload,
		createTime,
		latitude,
		longitude,
		[]int64{},
	)
}

func (s *SQLStore) CreateMemoWithAttachments(ctx context.Context, creatorID int64, content string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, createTime time.Time, latitude *float64, longitude *float64, attachmentIDs []int64) (models.Memo, error) {
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
		`INSERT INTO memos (
			creator_id, content, visibility, state, pinned, create_time, update_time, display_time,
			latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		content,
		visibility,
		state,
		pinnedInt,
		createTime.UTC().Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		createTime.UTC().Format(time.RFC3339Nano),
		latitude,
		longitude,
		boolToSQLiteInt(payload.Property.HasLink),
		boolToSQLiteInt(payload.Property.HasTaskList),
		boolToSQLiteInt(payload.Property.HasCode),
		boolToSQLiteInt(payload.Property.HasIncompleteTasks),
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
	if err := setMemoTagsInTx(ctx, tx, creatorID, memoID, payload.Tags); err != nil {
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
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		FROM memos
		WHERE id = ?`,
		id,
	)
	memo, err := scanMemo(row)
	if err != nil {
		return models.Memo{}, err
	}
	tagsByMemoID, err := s.listMemoTagsByMemoIDs(ctx, []int64{memo.ID})
	if err != nil {
		return models.Memo{}, err
	}
	memo.Payload.Tags = tagsByMemoID[memo.ID]
	if memo.Payload.Tags == nil {
		memo.Payload.Tags = []string{}
	}
	return memo, nil
}

func (s *SQLStore) UpdateMemo(ctx context.Context, memoID int64, update MemoUpdate) (models.Memo, error) {
	return s.UpdateMemoWithAttachments(ctx, memoID, update, nil)
}

func (s *SQLStore) UpdateMemoWithAttachments(ctx context.Context, memoID int64, update MemoUpdate, attachmentIDs *[]int64) (models.Memo, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Memo{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	var creatorID int64
	var previousCollaboratorIDs map[int64]struct{}
	if update.Payload != nil {
		if err := tx.QueryRowContext(ctx, `SELECT creator_id FROM memos WHERE id = ?`, memoID).Scan(&creatorID); err != nil {
			return models.Memo{}, err
		}
		previousTags, err := listMemoTagNamesInTx(ctx, tx, memoID)
		if err != nil {
			return models.Memo{}, err
		}
		previousCollaboratorIDs = collaboratorIDSetFromTags(previousTags)
	}

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
	if update.LatitudeSet || update.Latitude != nil {
		assignments = append(assignments, "latitude = ?")
		if update.Latitude != nil {
			args = append(args, *update.Latitude)
		} else {
			args = append(args, nil)
		}
	}
	if update.LongitudeSet || update.Longitude != nil {
		assignments = append(assignments, "longitude = ?")
		if update.Longitude != nil {
			args = append(args, *update.Longitude)
		} else {
			args = append(args, nil)
		}
	}
	if update.Payload != nil {
		assignments = append(assignments, "has_link = ?")
		args = append(args, boolToSQLiteInt(update.Payload.Property.HasLink))
		assignments = append(assignments, "has_task_list = ?")
		args = append(args, boolToSQLiteInt(update.Payload.Property.HasTaskList))
		assignments = append(assignments, "has_code = ?")
		args = append(args, boolToSQLiteInt(update.Payload.Property.HasCode))
		assignments = append(assignments, "has_incomplete_tasks = ?")
		args = append(args, boolToSQLiteInt(update.Payload.Property.HasIncompleteTasks))
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
	if update.Payload != nil {
		if err := setMemoTagsInTx(ctx, tx, creatorID, memoID, update.Payload.Tags); err != nil {
			return models.Memo{}, err
		}
		currentCollaboratorIDs := collaboratorIDSetFromTags(update.Payload.Tags)
		revokedRecipientIDs := make([]int64, 0)
		for collaboratorID := range previousCollaboratorIDs {
			if collaboratorID == creatorID {
				continue
			}
			if _, stillCollaborator := currentCollaboratorIDs[collaboratorID]; stillCollaborator {
				continue
			}
			revokedRecipientIDs = append(revokedRecipientIDs, collaboratorID)
		}
		if err := appendMemoChangeEventInTx(
			ctx,
			tx,
			memoID,
			creatorID,
			memoChangeEventTypeVisibilityRevoked,
			revokedRecipientIDs,
			time.Now().UTC(),
		); err != nil {
			return models.Memo{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return models.Memo{}, err
	}
	return s.GetMemoByID(ctx, memoID)
}

func (s *SQLStore) DeleteMemo(ctx context.Context, memoID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var creatorID int64
	if err := tx.QueryRowContext(ctx, `SELECT creator_id FROM memos WHERE id = ?`, memoID).Scan(&creatorID); err != nil {
		if err == sql.ErrNoRows {
			return sql.ErrNoRows
		}
		return err
	}
	tagNames, err := listMemoTagNamesInTx(ctx, tx, memoID)
	if err != nil {
		return err
	}
	collaboratorIDs := collaboratorIDSetFromTags(tagNames)
	recipientIDs := make([]int64, 0, len(collaboratorIDs)+1)
	recipientIDs = append(recipientIDs, creatorID)
	for collaboratorID := range collaboratorIDs {
		if collaboratorID == creatorID {
			continue
		}
		recipientIDs = append(recipientIDs, collaboratorID)
	}
	if err := appendMemoChangeEventInTx(
		ctx,
		tx,
		memoID,
		creatorID,
		memoChangeEventTypeDelete,
		recipientIDs,
		time.Now().UTC(),
	); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `DELETE FROM memos WHERE id = ?`, memoID)
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

	return tx.Commit()
}

func (s *SQLStore) ListVisibleMemos(
	ctx context.Context,
	viewerID int64,
	state *models.MemoState,
	prefilter MemoSQLPrefilter,
	limit int,
	offset int,
	bounds *MemoQueryBounds,
) ([]models.Memo, error) {
	if prefilter.Unsatisfiable {
		return []models.Memo{}, nil
	}

	collaboratorTag := fmt.Sprintf("collab/%d", viewerID)
	query := `SELECT m.id, m.creator_id, m.content, m.visibility, m.state, m.pinned, m.create_time, m.update_time, m.display_time, m.latitude, m.longitude, m.has_link, m.has_task_list, m.has_code, m.has_incomplete_tasks
		FROM memos m
		WHERE (
			m.creator_id = ?
			OR m.visibility IN ('PUBLIC', 'PROTECTED')
			OR EXISTS (
				SELECT 1
				FROM memo_tags mt
				JOIN tags t ON t.id = mt.tag_id
				WHERE mt.memo_id = m.id AND t.name = ?
			)
		)`
	args := []any{viewerID, collaboratorTag}

	if state != nil {
		query += ` AND m.state = ?`
		args = append(args, *state)
	}
	if bounds != nil && bounds.UpdatedAfter != nil {
		query += ` AND m.update_time > ?`
		args = append(args, bounds.UpdatedAfter.UTC().Format(time.RFC3339Nano))
	}
	if bounds != nil && bounds.UpdatedBeforeOrEqual != nil {
		query += ` AND m.update_time <= ?`
		args = append(args, bounds.UpdatedBeforeOrEqual.UTC().Format(time.RFC3339Nano))
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

	addPropertyConstraint := func(column string, value *bool) {
		if value == nil {
			return
		}
		query += fmt.Sprintf(` AND m.%s = ?`, column)
		args = append(args, boolToSQLiteInt(*value))
	}
	addPropertyConstraint("has_link", prefilter.HasLink)
	addPropertyConstraint("has_task_list", prefilter.HasTaskList)
	addPropertyConstraint("has_code", prefilter.HasCode)
	addPropertyConstraint("has_incomplete_tasks", prefilter.HasIncompleteTasks)

	for _, group := range prefilter.TagGroups {
		if len(group.Options) == 0 {
			continue
		}
		query += ` AND EXISTS (
			SELECT 1
			FROM memo_tags mt
			JOIN tags t ON t.id = mt.tag_id
			WHERE mt.memo_id = m.id AND `
		groupClauses := make([]string, 0, len(group.Options))
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				groupClauses = append(groupClauses, `t.name = ?`)
				args = append(args, option.Value)
			case TagMatchPrefix:
				groupClauses = append(groupClauses, `t.name LIKE ?`)
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
		query += ` AND NOT EXISTS (
			SELECT 1
			FROM memo_tags mt
			JOIN tags t ON t.id = mt.tag_id
			WHERE mt.memo_id = m.id AND `
		groupClauses := make([]string, 0, len(group.Options))
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				groupClauses = append(groupClauses, `t.name = ?`)
				args = append(args, option.Value)
			case TagMatchPrefix:
				groupClauses = append(groupClauses, `t.name LIKE ?`)
				args = append(args, option.Value+"%")
			}
		}
		if len(groupClauses) == 0 {
			continue
		}
		query += strings.Join(groupClauses, " OR ") + `)`
	}

	if bounds != nil && (bounds.UpdatedAfter != nil || bounds.UpdatedBeforeOrEqual != nil) {
		query += ` ORDER BY m.update_time ASC, m.id ASC`
	} else {
		query += ` ORDER BY m.create_time DESC, m.id DESC`
	}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.hydrateMemoTags(ctx, memos); err != nil {
		return nil, err
	}
	return memos, nil
}

func (s *SQLStore) ListDeletedVisibleMemoNames(
	ctx context.Context,
	viewerID int64,
	deletedAfter time.Time,
	deletedBeforeOrEqual time.Time,
	limit int,
) ([]string, error) {
	query := `SELECT DISTINCT mce.memo_name
		FROM memo_change_events mce
		JOIN memo_change_event_recipients mcer ON mcer.event_id = mce.id
		WHERE mce.event_time > ?
			AND mce.event_time <= ?
			AND mcer.user_id = ?
			AND mce.event_type IN (?, ?)
		ORDER BY mce.event_time ASC, mce.id ASC`
	args := []any{
		deletedAfter.UTC().Format(time.RFC3339Nano),
		deletedBeforeOrEqual.UTC().Format(time.RFC3339Nano),
		viewerID,
		memoChangeEventTypeDelete,
		memoChangeEventTypeVisibilityRevoked,
	}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var memoName string
		if err := rows.Scan(&memoName); err != nil {
			return nil, err
		}
		result = append(result, memoName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) ListVisibleMemosByCreator(ctx context.Context, creatorID int64, viewerID int64, state models.MemoState) ([]models.Memo, error) {
	query := `SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		FROM memos
		WHERE creator_id = ? AND state = ?`
	args := []any{creatorID, state}
	if creatorID != viewerID {
		collaboratorTag := fmt.Sprintf("collab/%d", viewerID)
		query += ` AND (
			visibility IN ('PUBLIC', 'PROTECTED')
			OR EXISTS (
				SELECT 1
				FROM memo_tags mt
				JOIN tags t ON t.id = mt.tag_id
				WHERE mt.memo_id = memos.id AND t.name = ?
			)
		)`
		args = append(args, collaboratorTag)
	}
	query += ` ORDER BY create_time DESC, id DESC`

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.hydrateMemoTags(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) ListAllMemos(ctx context.Context) ([]models.Memo, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.hydrateMemoTags(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) UpdateMemoPayload(ctx context.Context, memoID int64, payload models.MemoPayload) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE memos
		SET has_link = ?, has_task_list = ?, has_code = ?, has_incomplete_tasks = ?
		WHERE id = ?`,
		boolToSQLiteInt(payload.Property.HasLink),
		boolToSQLiteInt(payload.Property.HasTaskList),
		boolToSQLiteInt(payload.Property.HasCode),
		boolToSQLiteInt(payload.Property.HasIncompleteTasks),
		memoID,
	); err != nil {
		return err
	}
	var creatorID int64
	if err := tx.QueryRowContext(ctx, `SELECT creator_id FROM memos WHERE id = ?`, memoID).Scan(&creatorID); err != nil {
		return err
	}
	if err := setMemoTagsInTx(ctx, tx, creatorID, memoID, payload.Tags); err != nil {
		return err
	}
	return tx.Commit()
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

func (s *SQLStore) UpdateAttachmentThumbnail(
	ctx context.Context,
	attachmentID int64,
	thumbnailFilename string,
	thumbnailType string,
	thumbnailSize int64,
	thumbnailStorageType string,
	thumbnailStorageKey string,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE attachments
		SET thumbnail_filename = ?, thumbnail_type = ?, thumbnail_size = ?, thumbnail_storage_type = ?, thumbnail_storage_key = ?
		WHERE id = ?`,
		thumbnailFilename,
		thumbnailType,
		thumbnailSize,
		thumbnailStorageType,
		thumbnailStorageKey,
		attachmentID,
	)
	return err
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
		`INSERT INTO attachment_upload_sessions (
			id,
			creator_id,
			filename,
			type,
			size,
			memo_name,
			temp_path,
			thumbnail_filename,
			thumbnail_type,
			thumbnail_temp_path,
			received_size,
			create_time,
			update_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID,
		session.CreatorID,
		session.Filename,
		session.Type,
		session.Size,
		session.MemoName,
		session.TempPath,
		session.ThumbnailFilename,
		session.ThumbnailType,
		session.ThumbnailTempPath,
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
		`SELECT
			id,
			creator_id,
			filename,
			type,
			size,
			memo_name,
			temp_path,
			thumbnail_filename,
			thumbnail_type,
			thumbnail_temp_path,
			received_size,
			create_time,
			update_time
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
		&session.ThumbnailFilename,
		&session.ThumbnailType,
		&session.ThumbnailTempPath,
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

func (s *SQLStore) ListAttachmentUploadSessionsUpdatedBefore(ctx context.Context, cutoff time.Time, limit int) ([]models.AttachmentUploadSession, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
			id,
			creator_id,
			filename,
			type,
			size,
			memo_name,
			temp_path,
			thumbnail_filename,
			thumbnail_type,
			thumbnail_temp_path,
			received_size,
			create_time,
			update_time
		FROM attachment_upload_sessions
		WHERE julianday(update_time) <= julianday(?)
		ORDER BY update_time ASC
		LIMIT ?`,
		cutoff.UTC().Format(time.RFC3339Nano),
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make([]models.AttachmentUploadSession, 0, limit)
	for rows.Next() {
		var session models.AttachmentUploadSession
		var memoName sql.NullString
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&session.ID,
			&session.CreatorID,
			&session.Filename,
			&session.Type,
			&session.Size,
			&memoName,
			&session.TempPath,
			&session.ThumbnailFilename,
			&session.ThumbnailType,
			&session.ThumbnailTempPath,
			&session.ReceivedSize,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		if memoName.Valid {
			session.MemoName = &memoName.String
		}
		parsedCreateTime, err := parseTime(createTime)
		if err != nil {
			return nil, err
		}
		parsedUpdateTime, err := parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		session.CreateTime = parsedCreateTime
		session.UpdateTime = parsedUpdateTime
		sessions = append(sessions, session)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessions, nil
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
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		&attachment.ThumbnailFilename,
		&attachment.ThumbnailType,
		&attachment.ThumbnailSize,
		&attachment.ThumbnailStorageType,
		&attachment.ThumbnailStorageKey,
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
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		&attachment.ThumbnailFilename,
		&attachment.ThumbnailType,
		&attachment.ThumbnailSize,
		&attachment.ThumbnailStorageType,
		&attachment.ThumbnailStorageKey,
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
		`SELECT id, creator_id, filename, external_link, type, size, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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

func setMemoTagsInTx(ctx context.Context, tx *sql.Tx, creatorID int64, memoID int64, tags []string) error {
	normalized := normalizeTagNames(tags)
	if _, err := tx.ExecContext(ctx, `DELETE FROM memo_tags WHERE memo_id = ?`, memoID); err != nil {
		return err
	}
	if len(normalized) == 0 {
		_, err := tx.ExecContext(
			ctx,
			`DELETE FROM tags WHERE creator_id = ? AND id NOT IN (SELECT DISTINCT tag_id FROM memo_tags)`,
			creatorID,
		)
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, tag := range normalized {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO tags (creator_id, name, create_time, update_time)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(creator_id, name) DO UPDATE SET update_time = excluded.update_time`,
			creatorID,
			tag,
			now,
			now,
		); err != nil {
			return err
		}

		var tagID int64
		if err := tx.QueryRowContext(
			ctx,
			`SELECT id FROM tags WHERE creator_id = ? AND name = ?`,
			creatorID,
			tag,
		).Scan(&tagID); err != nil {
			return err
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memo_tags (memo_id, tag_id, create_time) VALUES (?, ?, ?)`,
			memoID,
			tagID,
			now,
		); err != nil {
			return err
		}
	}

	_, err := tx.ExecContext(
		ctx,
		`DELETE FROM tags WHERE creator_id = ? AND id NOT IN (SELECT DISTINCT tag_id FROM memo_tags)`,
		creatorID,
	)
	return err
}

func listMemoTagNamesInTx(ctx context.Context, tx *sql.Tx, memoID int64) ([]string, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT t.name
		FROM memo_tags mt
		JOIN tags t ON t.id = mt.tag_id
		WHERE mt.memo_id = ?`,
		memoID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}

func collaboratorIDSetFromTags(tags []string) map[int64]struct{} {
	result := make(map[int64]struct{})
	for _, tag := range tags {
		collaboratorID, ok := collaboratorIDFromTag(tag)
		if !ok {
			continue
		}
		result[collaboratorID] = struct{}{}
	}
	return result
}

func appendMemoChangeEventInTx(
	ctx context.Context,
	tx *sql.Tx,
	memoID int64,
	creatorID int64,
	eventType string,
	recipientIDs []int64,
	eventTime time.Time,
) error {
	if len(recipientIDs) == 0 {
		return nil
	}

	dedupedRecipients := make([]int64, 0, len(recipientIDs))
	seen := make(map[int64]struct{}, len(recipientIDs))
	for _, recipientID := range recipientIDs {
		if recipientID <= 0 {
			continue
		}
		if _, exists := seen[recipientID]; exists {
			continue
		}
		seen[recipientID] = struct{}{}
		dedupedRecipients = append(dedupedRecipients, recipientID)
	}
	if len(dedupedRecipients) == 0 {
		return nil
	}

	memoName := "memos/" + models.Int64ToString(memoID)
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO memo_change_events (memo_id, memo_name, creator_id, event_type, event_time)
		VALUES (?, ?, ?, ?, ?)`,
		memoID,
		memoName,
		creatorID,
		eventType,
		eventTime.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return err
	}
	eventID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, recipientID := range dedupedRecipients {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memo_change_event_recipients (event_id, user_id) VALUES (?, ?)`,
			eventID,
			recipientID,
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
		`SELECT ma.memo_id, a.id, a.creator_id, a.filename, a.external_link, a.type, a.size, a.storage_type, a.storage_key, a.thumbnail_filename, a.thumbnail_type, a.thumbnail_size, a.thumbnail_storage_type, a.thumbnail_storage_key, a.create_time
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
			&attachment.ThumbnailFilename,
			&attachment.ThumbnailType,
			&attachment.ThumbnailSize,
			&attachment.ThumbnailStorageType,
			&attachment.ThumbnailStorageKey,
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
		`SELECT id, creator_id, content, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		FROM memos
		WHERE id = ? AND creator_id = ?`,
		memoID,
		creatorID,
	)
	memo, err := scanMemo(row)
	if err != nil {
		return models.Memo{}, err
	}
	tagsByMemoID, err := s.listMemoTagsByMemoIDs(ctx, []int64{memo.ID})
	if err != nil {
		return models.Memo{}, err
	}
	memo.Payload.Tags = tagsByMemoID[memo.ID]
	if memo.Payload.Tags == nil {
		memo.Payload.Tags = []string{}
	}
	return memo, nil
}

func (s *SQLStore) listMemoTagsByMemoIDs(ctx context.Context, memoIDs []int64) (map[int64][]string, error) {
	result := make(map[int64][]string)
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
		`SELECT mt.memo_id, t.name
		FROM memo_tags mt
		JOIN tags t ON t.id = mt.tag_id
		WHERE mt.memo_id IN (%s)
		ORDER BY mt.memo_id, t.name`,
		strings.Join(placeholders, ","),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var memoID int64
		var tag string
		if err := rows.Scan(&memoID, &tag); err != nil {
			return nil, err
		}
		result[memoID] = append(result[memoID], tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) hydrateMemoTags(ctx context.Context, memos []models.Memo) error {
	memoIDs := make([]int64, 0, len(memos))
	for _, memo := range memos {
		memoIDs = append(memoIDs, memo.ID)
	}
	tagsByMemoID, err := s.listMemoTagsByMemoIDs(ctx, memoIDs)
	if err != nil {
		return err
	}
	for i := range memos {
		tags := tagsByMemoID[memos[i].ID]
		if tags == nil {
			tags = []string{}
		}
		memos[i].Payload.Tags = tags
	}
	return nil
}

func normalizeTagNames(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(tags))
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
		out = append(out, tag)
	}
	return out
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
	var legacySortTime string
	var latitude sql.NullFloat64
	var longitude sql.NullFloat64
	var hasLink int
	var hasTaskList int
	var hasCode int
	var hasIncompleteTasks int
	if err := scanner.Scan(
		&memo.ID,
		&memo.CreatorID,
		&memo.Content,
		&visibility,
		&state,
		&pinned,
		&createTime,
		&updateTime,
		&legacySortTime,
		&latitude,
		&longitude,
		&hasLink,
		&hasTaskList,
		&hasCode,
		&hasIncompleteTasks,
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
	if _, err = parseTime(legacySortTime); err != nil {
		return models.Memo{}, err
	}
	if latitude.Valid {
		memo.Latitude = &latitude.Float64
	}
	if longitude.Valid {
		memo.Longitude = &longitude.Float64
	}
	memo.Payload.Property = models.MemoPayloadProperty{
		HasLink:            hasLink == 1,
		HasTaskList:        hasTaskList == 1,
		HasCode:            hasCode == 1,
		HasIncompleteTasks: hasIncompleteTasks == 1,
	}
	memo.Payload.Tags = []string{}
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
		&attachment.ThumbnailFilename,
		&attachment.ThumbnailType,
		&attachment.ThumbnailSize,
		&attachment.ThumbnailStorageType,
		&attachment.ThumbnailStorageKey,
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

func collaboratorIDFromTag(tag string) (int64, bool) {
	tag = strings.TrimSpace(tag)
	const prefix = "collab/"
	if !strings.HasPrefix(tag, prefix) {
		return 0, false
	}
	rawID := strings.TrimSpace(strings.TrimPrefix(tag, prefix))
	if rawID == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
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
