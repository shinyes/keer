package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) GetUserGeneralSettings(ctx context.Context, userID int64) (models.UserGeneralSettings, error) {
	return getUserGeneralSettingsWithExecutor(ctx, s.db, userID)
}

func getUserGeneralSettingsWithExecutor(
	ctx context.Context,
	executor queryExecutor,
	userID int64,
) (models.UserGeneralSettings, error) {
	var settings models.UserGeneralSettings
	var memoVisibility string
	var memoEditGesture sql.NullString
	var memoColumnsJSON sql.NullString
	var createTime sql.NullString
	var updateTime sql.NullString
	err := executor.QueryRowContext(
		ctx,
		`SELECT
			u.id,
			u.default_visibility,
			ugs.memo_edit_gesture,
			ugs.memo_columns_json,
			ugs.create_time,
			ugs.update_time
		FROM users u
		LEFT JOIN user_general_settings ugs ON ugs.user_id = u.id
		WHERE u.id = ?`,
		userID,
	).Scan(
		&settings.UserID,
		&memoVisibility,
		&memoEditGesture,
		&memoColumnsJSON,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.UserGeneralSettings{}, err
	}

	settings.MemoVisibility = models.Visibility(memoVisibility)
	if memoEditGesture.Valid && memoEditGesture.String != "" {
		settings.MemoEditGesture = models.MemoEditGesture(memoEditGesture.String)
	} else {
		settings.MemoEditGesture = models.MemoEditGestureNone
	}

	if memoColumnsJSON.Valid && memoColumnsJSON.String != "" {
		if err := json.Unmarshal([]byte(memoColumnsJSON.String), &settings.MemoColumns); err != nil {
			return models.UserGeneralSettings{}, err
		}
	} else {
		settings.MemoColumns = []models.MemoColumnConfig{}
	}

	if createTime.Valid && createTime.String != "" {
		parsedCreateTime, err := parseTime(createTime.String)
		if err != nil {
			return models.UserGeneralSettings{}, err
		}
		settings.CreateTime = parsedCreateTime
	}
	if updateTime.Valid && updateTime.String != "" {
		parsedUpdateTime, err := parseTime(updateTime.String)
		if err != nil {
			return models.UserGeneralSettings{}, err
		}
		settings.UpdateTime = parsedUpdateTime
	}

	return settings, nil
}

func (s *SQLStore) UpdateUserGeneralSettings(
	ctx context.Context,
	settings models.UserGeneralSettings,
) (models.UserGeneralSettings, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.UserGeneralSettings{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := updateUserGeneralSettingsWithExecutor(ctx, tx, settings); err != nil {
		return models.UserGeneralSettings{}, err
	}
	if err := tx.Commit(); err != nil {
		return models.UserGeneralSettings{}, err
	}
	return s.GetUserGeneralSettings(ctx, settings.UserID)
}

func updateUserGeneralSettingsWithExecutor(
	ctx context.Context,
	executor queryExecutor,
	settings models.UserGeneralSettings,
) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	columnsJSON, err := json.Marshal(settings.MemoColumns)
	if err != nil {
		return err
	}

	res, err := executor.ExecContext(
		ctx,
		`UPDATE users
		SET default_visibility = ?, update_time = ?
		WHERE id = ?`,
		settings.MemoVisibility,
		now,
		settings.UserID,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	_, err = executor.ExecContext(
		ctx,
		`INSERT INTO user_general_settings (
			user_id, memo_edit_gesture, memo_columns_json, create_time, update_time
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			memo_edit_gesture = excluded.memo_edit_gesture,
			memo_columns_json = excluded.memo_columns_json,
			update_time = excluded.update_time`,
		settings.UserID,
		settings.MemoEditGesture,
		string(columnsJSON),
		now,
		now,
	)
	return err
}
