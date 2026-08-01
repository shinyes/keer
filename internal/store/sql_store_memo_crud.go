package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"strings"
	"time"
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
