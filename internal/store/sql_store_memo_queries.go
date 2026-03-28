package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) CreateMemo(ctx context.Context, creatorID int64, content string, payloadEnvelope string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, createTime time.Time, latitude *float64, longitude *float64) (models.Memo, error) {
	return s.CreateMemoWithAttachments(
		ctx,
		creatorID,
		content,
		payloadEnvelope,
		visibility,
		state,
		pinned,
		payload,
		createTime,
		latitude,
		longitude,
		[]AttachmentBinding{},
	)
}

func (s *SQLStore) CreateMemoWithAttachments(ctx context.Context, creatorID int64, content string, payloadEnvelope string, visibility models.Visibility, state models.MemoState, pinned bool, payload models.MemoPayload, createTime time.Time, latitude *float64, longitude *float64, attachments []AttachmentBinding) (models.Memo, error) {
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
			creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time,
			latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		content,
		payloadEnvelope,
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
	if err := setMemoAttachmentsInTx(ctx, tx, memoID, attachments); err != nil {
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
		`SELECT id, creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
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

func (s *SQLStore) UpdateMemoWithAttachments(ctx context.Context, memoID int64, update MemoUpdate, attachments *[]AttachmentBinding) (models.Memo, error) {
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
	if update.PayloadEnvelope != nil {
		assignments = append(assignments, "payload_envelope = ?")
		args = append(args, *update.PayloadEnvelope)
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

	var queryBuilder strings.Builder
	queryBuilder.WriteString("UPDATE memos SET ")
	queryBuilder.WriteString(strings.Join(assignments, ", "))
	queryBuilder.WriteString(" WHERE id = ?")
	query := queryBuilder.String()
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return models.Memo{}, err
	}

	if attachments != nil {
		if err := setMemoAttachmentsInTx(ctx, tx, memoID, *attachments); err != nil {
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
	query := `SELECT m.id, m.creator_id, m.content, m.payload_envelope, m.visibility, m.state, m.pinned, m.create_time, m.update_time, m.display_time, m.latitude, m.longitude, m.has_link, m.has_task_list, m.has_code, m.has_incomplete_tasks
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
		query += ` AND m.creator_id IN (`
		for i, id := range prefilter.CreatorIDs {
			if i > 0 {
				query += `,`
			}
			query += `?`
			args = append(args, id)
		}
		query += `)`
	}

	if len(prefilter.VisibilityIn) > 0 {
		query += ` AND m.visibility IN (`
		for i, v := range prefilter.VisibilityIn {
			if i > 0 {
				query += `,`
			}
			query += `?`
			args = append(args, v)
		}
		query += `)`
	}

	if len(prefilter.StateIn) > 0 {
		query += ` AND m.state IN (`
		for i, st := range prefilter.StateIn {
			if i > 0 {
				query += `,`
			}
			query += `?`
			args = append(args, st)
		}
		query += `)`
	}

	if prefilter.Pinned != nil {
		query += ` AND m.pinned = ?`
		args = append(args, boolToSQLiteInt(*prefilter.Pinned))
	}

	if prefilter.HasLink != nil {
		query += ` AND m.has_link = ?`
		args = append(args, boolToSQLiteInt(*prefilter.HasLink))
	}
	if prefilter.HasTaskList != nil {
		query += ` AND m.has_task_list = ?`
		args = append(args, boolToSQLiteInt(*prefilter.HasTaskList))
	}
	if prefilter.HasCode != nil {
		query += ` AND m.has_code = ?`
		args = append(args, boolToSQLiteInt(*prefilter.HasCode))
	}
	if prefilter.HasIncompleteTasks != nil {
		query += ` AND m.has_incomplete_tasks = ?`
		args = append(args, boolToSQLiteInt(*prefilter.HasIncompleteTasks))
	}

	for _, group := range prefilter.TagGroups {
		if len(group.Options) == 0 {
			continue
		}
		validOptionCount := 0
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact, TagMatchPrefix:
				validOptionCount++
			}
		}
		if validOptionCount == 0 {
			continue
		}
		query += ` AND EXISTS (
			SELECT 1
			FROM memo_tags mt
			JOIN tags t ON t.id = mt.tag_id
			WHERE mt.memo_id = m.id AND `
		addedClauses := 0
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				if addedClauses > 0 {
					query += ` OR `
				}
				query += `t.name = ?`
				addedClauses++
				args = append(args, option.Value)
			case TagMatchPrefix:
				if addedClauses > 0 {
					query += ` OR `
				}
				query += `t.name LIKE ?`
				addedClauses++
				args = append(args, option.Value+"%")
			}
		}
		query += `)`
	}
	for _, group := range prefilter.ExcludeTagGroups {
		if len(group.Options) == 0 {
			continue
		}
		validOptionCount := 0
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact, TagMatchPrefix:
				validOptionCount++
			}
		}
		if validOptionCount == 0 {
			continue
		}
		query += ` AND NOT EXISTS (
			SELECT 1
			FROM memo_tags mt
			JOIN tags t ON t.id = mt.tag_id
			WHERE mt.memo_id = m.id AND `
		addedClauses := 0
		for _, option := range group.Options {
			switch option.Kind {
			case TagMatchExact:
				if addedClauses > 0 {
					query += ` OR `
				}
				query += `t.name = ?`
				addedClauses++
				args = append(args, option.Value)
			case TagMatchPrefix:
				if addedClauses > 0 {
					query += ` OR `
				}
				query += `t.name LIKE ?`
				addedClauses++
				args = append(args, option.Value+"%")
			}
		}
		query += `)`
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

func (s *SQLStore) ListVisibleMemosByIDs(ctx context.Context, viewerID int64, memoIDs []int64) ([]models.Memo, error) {
	normalizedIDs := make([]int64, 0, len(memoIDs))
	seen := make(map[int64]struct{}, len(memoIDs))
	for _, memoID := range memoIDs {
		if memoID <= 0 {
			continue
		}
		if _, exists := seen[memoID]; exists {
			continue
		}
		seen[memoID] = struct{}{}
		normalizedIDs = append(normalizedIDs, memoID)
	}
	if len(normalizedIDs) == 0 {
		return []models.Memo{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(normalizedIDs)), ",")
	collaboratorTag := fmt.Sprintf("collab/%d", viewerID)
	var queryBuilder strings.Builder
	queryBuilder.WriteString(`SELECT m.id, m.creator_id, m.content, m.payload_envelope, m.visibility, m.state, m.pinned, m.create_time, m.update_time, m.display_time, m.latitude, m.longitude, m.has_link, m.has_task_list, m.has_code, m.has_incomplete_tasks
		FROM memos m
		WHERE m.id IN (`)
	queryBuilder.WriteString(placeholders)
	queryBuilder.WriteString(`)
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
		ORDER BY m.id ASC`)
	query := queryBuilder.String()

	args := make([]any, 0, len(normalizedIDs)+2)
	for _, memoID := range normalizedIDs {
		args = append(args, memoID)
	}
	args = append(args, viewerID, collaboratorTag)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Memo, 0, len(normalizedIDs))
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
	query := `SELECT id, creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
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
		`SELECT id, creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time, latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
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

func (s *SQLStore) CreateAttachment(ctx context.Context, creatorID int64, filename string, externalLink string, fileType string, size int64, contentHash string, encryptionMetadata string, storageType string, storageKey string) (models.Attachment, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO attachments (creator_id, filename, external_link, type, size, content_hash, encryption_metadata, storage_type, storage_key, create_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		creatorID,
		filename,
		externalLink,
		fileType,
		size,
		contentHash,
		encryptionMetadata,
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

func (s *SQLStore) UpdateAttachmentEncryptionMetadata(
	ctx context.Context,
	attachmentID int64,
	encryptionMetadata string,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE attachments SET encryption_metadata = ? WHERE id = ?`,
		strings.TrimSpace(encryptionMetadata),
		attachmentID,
	)
	return err
}

func (s *SQLStore) ListMemoAttachmentAssociationMetadataByAttachmentID(
	ctx context.Context,
	attachmentID int64,
) ([]MemoAttachmentAssociationMetadata, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT memo_id, attachment_id, association_encryption_metadata
		FROM memo_attachments
		WHERE attachment_id = ?`,
		attachmentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]MemoAttachmentAssociationMetadata, 0)
	for rows.Next() {
		var item MemoAttachmentAssociationMetadata
		if err := rows.Scan(
			&item.MemoID,
			&item.AttachmentID,
			&item.AssociationEncryptionMetadata,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) UpdateMemoAttachmentAssociationEncryptionMetadata(
	ctx context.Context,
	memoID int64,
	attachmentID int64,
	associationEncryptionMetadata string,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE memo_attachments
		SET association_encryption_metadata = ?
		WHERE memo_id = ? AND attachment_id = ?`,
		strings.TrimSpace(associationEncryptionMetadata),
		memoID,
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
			encryption_metadata,
			memo_name,
			temp_path,
			thumbnail_filename,
			thumbnail_type,
			thumbnail_temp_path,
			received_size,
			create_time,
			update_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID,
		session.CreatorID,
		session.Filename,
		session.Type,
		session.Size,
		session.EncryptionMetadata,
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
			encryption_metadata,
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
		&session.EncryptionMetadata,
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
			encryption_metadata,
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
			&session.EncryptionMetadata,
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
		`SELECT id, creator_id, filename, external_link, type, size, encryption_metadata, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		&attachment.EncryptionMetadata,
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
		`SELECT id, creator_id, filename, external_link, type, size, encryption_metadata, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		`SELECT id, creator_id, filename, external_link, type, size, encryption_metadata, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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
		&attachment.EncryptionMetadata,
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
		`SELECT id, creator_id, filename, external_link, type, size, encryption_metadata, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
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

func (s *SQLStore) ListAllAttachments(ctx context.Context) ([]models.Attachment, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, creator_id, filename, external_link, type, size, encryption_metadata, storage_type, storage_key, thumbnail_filename, thumbnail_type, thumbnail_size, thumbnail_storage_type, thumbnail_storage_key, create_time
		FROM attachments
		ORDER BY id DESC`,
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

func (s *SQLStore) ListUnattachedAttachments(ctx context.Context) ([]models.Attachment, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT a.id, a.creator_id, a.filename, a.external_link, a.type, a.size, a.encryption_metadata, a.storage_type, a.storage_key, a.thumbnail_filename, a.thumbnail_type, a.thumbnail_size, a.thumbnail_storage_type, a.thumbnail_storage_key, a.create_time
		FROM attachments a
		WHERE NOT EXISTS (
			SELECT 1 FROM memo_attachments ma WHERE ma.attachment_id = a.id
		)
		AND NOT EXISTS (
			SELECT 1 FROM group_message_attachments gma WHERE gma.attachment_id = a.id
		)
		ORDER BY a.id DESC`,
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

func (s *SQLStore) DeleteAttachmentsByIDs(ctx context.Context, attachmentIDs []int64) error {
	if len(attachmentIDs) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(attachmentIDs))
	args := make([]any, 0, len(attachmentIDs))
	for _, attachmentID := range attachmentIDs {
		placeholders = append(placeholders, "?")
		args = append(args, attachmentID)
	}
	query := `DELETE FROM attachments WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLStore) CountAttachmentsByStorageKey(ctx context.Context, storageKey string) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM attachments WHERE storage_key = ?`, storageKey).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLStore) SetMemoAttachments(ctx context.Context, memoID int64, attachments []AttachmentBinding) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := setMemoAttachmentsInTx(ctx, tx, memoID, attachments); err != nil {
		return err
	}
	return tx.Commit()
}

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
