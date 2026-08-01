package store

import (
	"context"
	"database/sql"
	"github.com/shinyes/keer/internal/models"
	"strings"
	"time"
)

func setMemoAttachmentsInTx(ctx context.Context, tx *sql.Tx, memoID int64, attachments []AttachmentBinding) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memo_attachments WHERE memo_id = ?`, memoID); err != nil {
		return err
	}
	for i, attachment := range attachments {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memo_attachments (memo_id, attachment_id, association_encryption_metadata, position) VALUES (?, ?, ?, ?)`,
			memoID,
			attachment.AttachmentID,
			strings.TrimSpace(attachment.AssociationEncryptionMetadata),
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
