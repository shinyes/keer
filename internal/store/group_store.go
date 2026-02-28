package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) CreateGroup(ctx context.Context, creatorID int64, name string, description string) (models.Group, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Group{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO groups (name, description, creator_id, create_time, update_time)
		VALUES (?, ?, ?, ?, ?)`,
		name,
		description,
		creatorID,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.Group{}, err
	}
	groupID, err := res.LastInsertId()
	if err != nil {
		return models.Group{}, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO group_members (group_id, user_id, join_time) VALUES (?, ?, ?)`,
		groupID,
		creatorID,
		now.Format(time.RFC3339Nano),
	); err != nil {
		return models.Group{}, err
	}

	if err := tx.Commit(); err != nil {
		return models.Group{}, err
	}
	return s.GetGroupByID(ctx, groupID)
}

func (s *SQLStore) GetGroupByID(ctx context.Context, groupID int64) (models.Group, error) {
	var group models.Group
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, name, description, creator_id, create_time, update_time
		FROM groups
		WHERE id = ?`,
		groupID,
	).Scan(
		&group.ID,
		&group.GroupName,
		&group.Description,
		&group.CreatorID,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.Group{}, err
	}
	group.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Group{}, err
	}
	group.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.Group{}, err
	}
	return group, nil
}

func (s *SQLStore) ListGroupsByUser(ctx context.Context, userID int64) ([]models.Group, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT g.id, g.name, g.description, g.creator_id, g.create_time, g.update_time
		FROM groups g
		JOIN group_members gm ON gm.group_id = g.id
		WHERE gm.user_id = ?
		ORDER BY g.update_time DESC, g.id DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Group, 0)
	for rows.Next() {
		var group models.Group
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&group.ID,
			&group.GroupName,
			&group.Description,
			&group.CreatorID,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		group.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		group.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		result = append(result, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) ListGroupMembers(ctx context.Context, groupID int64) ([]models.User, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.id, u.username, u.display_name, u.avatar_url, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time
		FROM group_members gm
		JOIN users u ON u.id = gm.user_id
		WHERE gm.group_id = ?
		ORDER BY gm.join_time ASC, u.id ASC`,
		groupID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.User, 0)
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
		result = append(result, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) IsGroupMember(ctx context.Context, groupID int64, userID int64) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?`,
		groupID,
		userID,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLStore) AddGroupMember(ctx context.Context, groupID int64, userID int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO group_members (group_id, user_id, join_time) VALUES (?, ?, ?)`,
		groupID,
		userID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLStore) RemoveGroupMember(ctx context.Context, groupID int64, userID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`,
		groupID,
		userID,
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

func (s *SQLStore) UpdateGroup(ctx context.Context, groupID int64, name string, description string) (models.Group, error) {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE groups SET name = ?, description = ?, update_time = ? WHERE id = ?`,
		name,
		description,
		time.Now().UTC().Format(time.RFC3339Nano),
		groupID,
	)
	if err != nil {
		return models.Group{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return models.Group{}, err
	}
	if affected == 0 {
		return models.Group{}, sql.ErrNoRows
	}
	return s.GetGroupByID(ctx, groupID)
}

func (s *SQLStore) DeleteGroup(ctx context.Context, groupID int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM groups WHERE id = ?`, groupID)
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

func (s *SQLStore) ListGroupTags(ctx context.Context, groupID int64) ([]string, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT name
		FROM group_tags
		WHERE group_id = ?
		ORDER BY update_time DESC, name ASC`,
		groupID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		result = append(result, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) UpsertGroupTags(ctx context.Context, groupID int64, creatorID int64, tags []string) error {
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		return upsertGroupTagsInTx(ctx, tx, groupID, creatorID, tags)
	})
}

func upsertGroupTagsInTx(ctx context.Context, tx *sql.Tx, groupID int64, creatorID int64, tags []string) error {
	normalized := normalizeGroupTags(tags)
	if len(normalized) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, tag := range normalized {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_tags (group_id, name, creator_id, create_time, update_time)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, name) DO UPDATE SET update_time = excluded.update_time`,
			groupID,
			tag,
			creatorID,
			now,
			now,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLStore) CreateGroupMessage(
	ctx context.Context,
	groupID int64,
	creatorID int64,
	content string,
	tags []string,
) (models.GroupMessage, error) {
	now := time.Now().UTC()
	normalizedTags := normalizeGroupTags(tags)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.GroupMessage{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := upsertGroupTagsInTx(ctx, tx, groupID, creatorID, normalizedTags); err != nil {
		return models.GroupMessage{}, err
	}

	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO group_messages (group_id, creator_id, content, create_time, update_time)
		VALUES (?, ?, ?, ?, ?)`,
		groupID,
		creatorID,
		content,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return models.GroupMessage{}, err
	}
	messageID, err := res.LastInsertId()
	if err != nil {
		return models.GroupMessage{}, err
	}

	for _, tag := range normalizedTags {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_message_tags (message_id, group_id, tag_name, create_time)
			VALUES (?, ?, ?, ?)`,
			messageID,
			groupID,
			tag,
			now.Format(time.RFC3339Nano),
		); err != nil {
			return models.GroupMessage{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return models.GroupMessage{}, err
	}
	return s.GetGroupMessageByID(ctx, messageID)
}

func (s *SQLStore) GetGroupMessageByID(ctx context.Context, messageID int64) (models.GroupMessage, error) {
	var msg models.GroupMessage
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, group_id, creator_id, content, create_time, update_time
		FROM group_messages
		WHERE id = ?`,
		messageID,
	).Scan(
		&msg.ID,
		&msg.GroupID,
		&msg.CreatorID,
		&msg.Content,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.GroupMessage{}, err
	}
	msg.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.GroupMessage{}, err
	}
	msg.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.GroupMessage{}, err
	}
	if err := s.hydrateGroupMessageTags(ctx, []models.GroupMessage{msg}); err != nil {
		return models.GroupMessage{}, err
	}
	msg.Tags = normalizeGroupTags(msg.Tags)
	return msg, nil
}

func (s *SQLStore) ListGroupMessagesPage(
	ctx context.Context,
	groupID int64,
	limit int,
	offset int,
) ([]models.GroupMessage, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, group_id, creator_id, content, create_time, update_time
		FROM group_messages
		WHERE group_id = ?
		ORDER BY create_time ASC, id ASC
		LIMIT ? OFFSET ?`,
		groupID,
		limit+1,
		offset,
	)
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()

	result := make([]models.GroupMessage, 0, limit+1)
	for rows.Next() {
		var msg models.GroupMessage
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&msg.ID,
			&msg.GroupID,
			&msg.CreatorID,
			&msg.Content,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, -1, err
		}
		msg.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, -1, err
		}
		msg.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, -1, err
		}
		result = append(result, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, -1, err
	}

	nextOffset := -1
	if len(result) > limit {
		result = result[:limit]
		nextOffset = offset + limit
	}
	if err := s.hydrateGroupMessageTags(ctx, result); err != nil {
		return nil, -1, err
	}
	for i := range result {
		result[i].Tags = normalizeGroupTags(result[i].Tags)
	}
	return result, nextOffset, nil
}

func (s *SQLStore) hydrateGroupMessageTags(ctx context.Context, messages []models.GroupMessage) error {
	if len(messages) == 0 {
		return nil
	}

	ids := make([]int64, 0, len(messages))
	indexByID := make(map[int64]int, len(messages))
	for idx, msg := range messages {
		ids = append(ids, msg.ID)
		indexByID[msg.ID] = idx
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := s.db.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT message_id, tag_name
			FROM group_message_tags
			WHERE message_id IN (%s)
			ORDER BY create_time ASC, tag_name ASC`,
			placeholders,
		),
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var messageID int64
		var tag string
		if err := rows.Scan(&messageID, &tag); err != nil {
			return err
		}
		if idx, ok := indexByID[messageID]; ok {
			messages[idx].Tags = append(messages[idx].Tags, tag)
		}
	}
	return rows.Err()
}

func normalizeGroupTags(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(tags))
	result := make([]string, 0, len(tags))
	for _, raw := range tags {
		tag := strings.TrimSpace(raw)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, tag)
	}
	return result
}

func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
