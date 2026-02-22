package store

import (
	"context"
	"database/sql"
	"time"
)

func (s *SQLStore) UpsertSetting(ctx context.Context, key string, value string) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO system_settings (key, value, update_time)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			update_time = excluded.update_time`,
		key,
		value,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLStore) GetSetting(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM system_settings WHERE key = ?`, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (s *SQLStore) DeleteSetting(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM system_settings WHERE key = ?`, key)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}
