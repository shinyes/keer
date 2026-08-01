package store

import (
	"context"
	"database/sql"
	"github.com/shinyes/keer/internal/models"
	"strings"
	"time"
)

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

func normalizedGroupType(groupType models.GroupType) models.GroupType {
	switch models.GroupType(strings.ToUpper(strings.TrimSpace(string(groupType)))) {
	case models.GroupTypeDirect:
		return models.GroupTypeDirect
	default:
		return models.GroupTypeGroup
	}
}

func nullIfBlank(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
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

func touchGroupUpdateTimeInTx(ctx context.Context, tx *sql.Tx, groupID int64, now time.Time) error {
	res, err := tx.ExecContext(
		ctx,
		`UPDATE groups SET update_time = ? WHERE id = ?`,
		now.Format(time.RFC3339Nano),
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
	return nil
}
