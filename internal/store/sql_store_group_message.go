package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"strings"
	"time"
)

func (s *SQLStore) CreateGroupMessage(
	ctx context.Context,
	groupID int64,
	creatorID int64,
	content string,
	payloadEnvelope string,
	tags []string,
	attachments []AttachmentBinding,
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
		`INSERT INTO group_messages (group_id, creator_id, content, payload_envelope, create_time, update_time)
		VALUES (?, ?, ?, ?, ?, ?)`,
		groupID,
		creatorID,
		content,
		payloadEnvelope,
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
	for index, attachment := range attachments {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_message_attachments (message_id, attachment_id, association_encryption_metadata, position) VALUES (?, ?, ?, ?)`,
			messageID,
			attachment.AttachmentID,
			strings.TrimSpace(attachment.AssociationEncryptionMetadata),
			index,
		); err != nil {
			return models.GroupMessage{}, err
		}
	}
	if err := touchGroupUpdateTimeInTx(ctx, tx, groupID, now); err != nil {
		return models.GroupMessage{}, err
	}

	if err := tx.Commit(); err != nil {
		return models.GroupMessage{}, err
	}
	return s.GetGroupMessageByID(ctx, messageID)
}

func (s *SQLStore) UpdateGroupMessage(
	ctx context.Context,
	groupID int64,
	messageID int64,
	actorID int64,
	content string,
	payloadEnvelope string,
	tags []string,
	attachments []AttachmentBinding,
) (models.GroupMessage, error) {
	now := time.Now().UTC()
	normalizedTags := normalizeGroupTags(tags)
	normalizedContent := strings.TrimSpace(content)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.GroupMessage{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	var existingGroupID int64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT group_id FROM group_messages WHERE id = ?`,
		messageID,
	).Scan(&existingGroupID); err != nil {
		return models.GroupMessage{}, err
	}
	if existingGroupID != groupID {
		return models.GroupMessage{}, sql.ErrNoRows
	}

	if err := upsertGroupTagsInTx(ctx, tx, groupID, actorID, normalizedTags); err != nil {
		return models.GroupMessage{}, err
	}

	query := `UPDATE group_messages SET content = ?, payload_envelope = ?, update_time = ?`
	args := []any{
		normalizedContent,
		payloadEnvelope,
		now.Format(time.RFC3339Nano),
	}
	query += ` WHERE id = ? AND group_id = ?`
	args = append(args, messageID, groupID)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return models.GroupMessage{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return models.GroupMessage{}, err
	}
	if affected == 0 {
		return models.GroupMessage{}, sql.ErrNoRows
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM group_message_tags WHERE message_id = ?`, messageID); err != nil {
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM group_message_attachments WHERE message_id = ?`, messageID); err != nil {
		return models.GroupMessage{}, err
	}
	for index, attachment := range attachments {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_message_attachments (message_id, attachment_id, association_encryption_metadata, position) VALUES (?, ?, ?, ?)`,
			messageID,
			attachment.AttachmentID,
			strings.TrimSpace(attachment.AssociationEncryptionMetadata),
			index,
		); err != nil {
			return models.GroupMessage{}, err
		}
	}
	if err := touchGroupUpdateTimeInTx(ctx, tx, groupID, now); err != nil {
		return models.GroupMessage{}, err
	}

	if err := tx.Commit(); err != nil {
		return models.GroupMessage{}, err
	}
	return s.GetGroupMessageByID(ctx, messageID)
}

func (s *SQLStore) DeleteGroupMessage(ctx context.Context, groupID int64, messageID int64) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(
		ctx,
		`DELETE FROM group_messages WHERE id = ? AND group_id = ?`,
		messageID,
		groupID,
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
	if err := touchGroupUpdateTimeInTx(ctx, tx, groupID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLStore) GetGroupMessageByID(ctx context.Context, messageID int64) (models.GroupMessage, error) {
	var msg models.GroupMessage
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, group_id, creator_id, content, payload_envelope, create_time, update_time
		FROM group_messages
		WHERE id = ?`,
		messageID,
	).Scan(
		&msg.ID,
		&msg.GroupID,
		&msg.CreatorID,
		&msg.Content,
		&msg.PayloadEnvelope,
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

func (s *SQLStore) ListGroupMessagesByIDs(
	ctx context.Context,
	groupID int64,
	messageIDs []int64,
) ([]models.GroupMessage, error) {
	if groupID <= 0 || len(messageIDs) == 0 {
		return []models.GroupMessage{}, nil
	}

	normalizedIDs := make([]int64, 0, len(messageIDs))
	seen := make(map[int64]struct{}, len(messageIDs))
	for _, messageID := range messageIDs {
		if messageID <= 0 {
			continue
		}
		if _, exists := seen[messageID]; exists {
			continue
		}
		seen[messageID] = struct{}{}
		normalizedIDs = append(normalizedIDs, messageID)
	}
	if len(normalizedIDs) == 0 {
		return []models.GroupMessage{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(normalizedIDs)), ",")
	args := make([]any, 0, len(normalizedIDs)+1)
	args = append(args, groupID)
	for _, messageID := range normalizedIDs {
		args = append(args, messageID)
	}

	rows, err := s.db.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT id, group_id, creator_id, content, payload_envelope, create_time, update_time
			FROM group_messages
			WHERE group_id = ? AND id IN (%s)
			ORDER BY create_time ASC, id ASC`,
			placeholders,
		),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.GroupMessage, 0, len(normalizedIDs))
	for rows.Next() {
		var msg models.GroupMessage
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&msg.ID,
			&msg.GroupID,
			&msg.CreatorID,
			&msg.Content,
			&msg.PayloadEnvelope,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		msg.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		msg.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.hydrateGroupMessageTags(ctx, result); err != nil {
		return nil, err
	}
	for i := range result {
		result[i].Tags = normalizeGroupTags(result[i].Tags)
	}
	return result, nil
}

func (s *SQLStore) ListGroupMessageTombstonesSince(
	ctx context.Context,
	groupID int64,
	since time.Time,
) ([]models.GroupMessageTombstone, error) {
	if groupID <= 0 {
		return []models.GroupMessageTombstone{}, nil
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT group_id, message_id, deleted_time
		FROM group_message_tombstones
		WHERE group_id = ? AND deleted_time >= ?
		ORDER BY deleted_time ASC, message_id ASC`,
		groupID,
		since.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.GroupMessageTombstone, 0, 16)
	for rows.Next() {
		var tombstone models.GroupMessageTombstone
		var deletedTimeRaw string
		if err := rows.Scan(
			&tombstone.GroupID,
			&tombstone.MessageID,
			&deletedTimeRaw,
		); err != nil {
			return nil, err
		}
		deletedTime, err := parseTime(deletedTimeRaw)
		if err != nil {
			return nil, err
		}
		tombstone.DeletedTime = deletedTime
		result = append(result, tombstone)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
		`SELECT id, group_id, creator_id, content, payload_envelope, create_time, update_time
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
			&msg.PayloadEnvelope,
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
