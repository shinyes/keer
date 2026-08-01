package store

import (
	"context"
	"strings"
	"github.com/shinyes/keer/internal/models"
)

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

	var queryBuilder strings.Builder
	queryBuilder.WriteString(`SELECT ma.memo_id, a.id, a.creator_id, a.filename, a.external_link, a.type, a.size, a.encryption_metadata, ma.association_encryption_metadata, a.storage_type, a.storage_key, a.thumbnail_filename, a.thumbnail_type, a.thumbnail_size, a.thumbnail_storage_type, a.thumbnail_storage_key, a.create_time
		FROM memo_attachments ma
		JOIN attachments a ON a.id = ma.attachment_id
		WHERE ma.memo_id IN (`)
	queryBuilder.WriteString(strings.Join(placeholders, ","))
	queryBuilder.WriteString(`)
		ORDER BY ma.memo_id, ma.position ASC, ma.attachment_id ASC`)
	query := queryBuilder.String()
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
		`SELECT id, creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
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

	var queryBuilder strings.Builder
	queryBuilder.WriteString(`SELECT mt.memo_id, t.name
		FROM memo_tags mt
		JOIN tags t ON t.id = mt.tag_id
		WHERE mt.memo_id IN (`)
	queryBuilder.WriteString(strings.Join(placeholders, ","))
	queryBuilder.WriteString(`)
		ORDER BY mt.memo_id, t.name`)
	query := queryBuilder.String()
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
