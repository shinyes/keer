package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) CreateGroupKeyVersion(
	ctx context.Context,
	groupID int64,
	algorithm string,
	recipients []models.GroupKeyVersionRecipient,
) (models.GroupKeyVersion, []models.GroupKeyVersionRecipient, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	var nextVersion int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(version), 0) + 1 FROM group_key_versions WHERE group_id = ?`,
		groupID,
	).Scan(&nextVersion); err != nil {
		return models.GroupKeyVersion{}, nil, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO group_key_versions (group_id, version, algorithm, create_time, update_time)
		VALUES (?, ?, ?, ?, ?)`,
		groupID,
		nextVersion,
		algorithm,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		return models.GroupKeyVersion{}, nil, err
	}

	normalizedRecipients := make([]models.GroupKeyVersionRecipient, 0, len(recipients))
	seen := make(map[int64]struct{}, len(recipients))
	for _, recipient := range recipients {
		if recipient.UserID <= 0 {
			continue
		}
		if _, exists := seen[recipient.UserID]; exists {
			continue
		}
		seen[recipient.UserID] = struct{}{}
		recipient.GroupID = groupID
		recipient.Version = nextVersion
		recipient.CreateTime = now
		recipient.UpdateTime = now
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_key_version_recipients (
				group_id, version, user_id, slot_ref, wrap_algorithm, wrapped_key, create_time, update_time
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			recipient.GroupID,
			recipient.Version,
			recipient.UserID,
			recipient.SlotRef,
			recipient.WrapAlgorithm,
			recipient.WrappedKey,
			now.Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano),
		); err != nil {
			return models.GroupKeyVersion{}, nil, err
		}
		normalizedRecipients = append(normalizedRecipients, recipient)
	}

	if err := tx.Commit(); err != nil {
		return models.GroupKeyVersion{}, nil, err
	}

	version := models.GroupKeyVersion{
		GroupID:    groupID,
		Version:    nextVersion,
		Algorithm:  algorithm,
		CreateTime: now,
		UpdateTime: now,
	}
	return version, normalizedRecipients, nil
}

func (s *SQLStore) GetCurrentGroupKeyVersion(ctx context.Context, groupID int64) (models.GroupKeyVersion, error) {
	var version models.GroupKeyVersion
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT group_id, version, algorithm, create_time, update_time
		FROM group_key_versions
		WHERE group_id = ?
		ORDER BY version DESC
		LIMIT 1`,
		groupID,
	).Scan(
		&version.GroupID,
		&version.Version,
		&version.Algorithm,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.GroupKeyVersion{}, err
	}
	version.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.GroupKeyVersion{}, err
	}
	version.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.GroupKeyVersion{}, err
	}
	return version, nil
}

func (s *SQLStore) ListGroupKeyVersionRecipients(
	ctx context.Context,
	groupID int64,
	version int,
) ([]models.GroupKeyVersionRecipient, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT group_id, version, user_id, slot_ref, wrap_algorithm, wrapped_key, create_time, update_time
		FROM group_key_version_recipients
		WHERE group_id = ? AND version = ?
		ORDER BY user_id ASC`,
		groupID,
		version,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.GroupKeyVersionRecipient, 0)
	for rows.Next() {
		var recipient models.GroupKeyVersionRecipient
		var createTime string
		var updateTime string
		if err := rows.Scan(
			&recipient.GroupID,
			&recipient.Version,
			&recipient.UserID,
			&recipient.SlotRef,
			&recipient.WrapAlgorithm,
			&recipient.WrappedKey,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		recipient.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		recipient.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		result = append(result, recipient)
	}
	return result, rows.Err()
}

func (s *SQLStore) SetGroupMessageAttachments(ctx context.Context, messageID int64, attachments []AttachmentBinding) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx, `DELETE FROM group_message_attachments WHERE message_id = ?`, messageID); err != nil {
		return err
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
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLStore) ListAttachmentsByGroupMessageIDs(ctx context.Context, messageIDs []int64) (map[int64][]models.Attachment, error) {
	result := make(map[int64][]models.Attachment)
	if len(messageIDs) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(messageIDs))
	args := make([]any, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		placeholders = append(placeholders, "?")
		args = append(args, messageID)
	}

	query := fmt.Sprintf(
		`SELECT gma.message_id, a.id, a.creator_id, a.filename, a.external_link, a.type, a.size, a.encryption_metadata, gma.association_encryption_metadata, a.storage_type, a.storage_key, a.thumbnail_filename, a.thumbnail_type, a.thumbnail_size, a.thumbnail_storage_type, a.thumbnail_storage_key, a.create_time
		FROM group_message_attachments gma
		JOIN attachments a ON a.id = gma.attachment_id
		WHERE gma.message_id IN (%s)
		ORDER BY gma.message_id, gma.position ASC, gma.attachment_id ASC`,
		strings.Join(placeholders, ","),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var messageID int64
		attachment, err := scanAttachmentWithMemoContext(rows, &messageID)
		if err != nil {
			return nil, err
		}
		result[messageID] = append(result[messageID], attachment)
	}
	return result, rows.Err()
}

func scanAttachmentWithMemoContext(scanner interface {
	Scan(dest ...any) error
}, leadingDest ...any) (models.Attachment, error) {
	var attachment models.Attachment
	var createTime string
	dest := append(leadingDest,
		&attachment.ID,
		&attachment.CreatorID,
		&attachment.Filename,
		&attachment.ExternalLink,
		&attachment.Type,
		&attachment.Size,
		&attachment.EncryptionMetadata,
		&attachment.AssociationEncryptionMetadata,
		&attachment.StorageType,
		&attachment.StorageKey,
		&attachment.ThumbnailFilename,
		&attachment.ThumbnailType,
		&attachment.ThumbnailSize,
		&attachment.ThumbnailStorageType,
		&attachment.ThumbnailStorageKey,
		&createTime,
	)
	if err := scanner.Scan(dest...); err != nil {
		return models.Attachment{}, err
	}
	var err error
	attachment.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Attachment{}, err
	}
	return attachment, nil
}

func (s *SQLStore) AttachmentVisibleToUser(ctx context.Context, attachmentID int64, userID int64) (bool, error) {
	collaboratorTag := fmt.Sprintf("collab/%d", userID)
	var exists int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT 1
		WHERE EXISTS (
			SELECT 1
			FROM attachments a
			WHERE a.id = ? AND a.creator_id = ?
		)
		OR EXISTS (
			SELECT 1
			FROM memo_attachments ma
			JOIN memos m ON m.id = ma.memo_id
			WHERE ma.attachment_id = ?
				AND (
					m.creator_id = ?
					OR m.visibility IN ('PUBLIC', 'PROTECTED')
					OR EXISTS (
						SELECT 1
						FROM memo_tags mt
						JOIN tags t ON t.id = mt.tag_id
						WHERE mt.memo_id = m.id AND t.name = ?
					)
				)
		)
		OR EXISTS (
			SELECT 1
			FROM group_message_attachments gma
			JOIN group_messages gm ON gm.id = gma.message_id
			JOIN group_members gmem ON gmem.group_id = gm.group_id
			WHERE gma.attachment_id = ? AND gmem.user_id = ?
		)`,
		attachmentID,
		userID,
		attachmentID,
		userID,
		collaboratorTag,
		attachmentID,
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
