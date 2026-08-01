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
	return s.createGroupWithMembers(
		ctx,
		creatorID,
		name,
		description,
		models.GroupTypeGroup,
		"",
		[]int64{creatorID},
	)
}

func (s *SQLStore) CreateDirectGroup(ctx context.Context, creatorID int64, targetUserID int64, directKey string) (models.Group, error) {
	return s.createGroupWithMembers(
		ctx,
		creatorID,
		"",
		"",
		models.GroupTypeDirect,
		directKey,
		[]int64{creatorID, targetUserID},
	)
}

func (s *SQLStore) createGroupWithMembers(
	ctx context.Context,
	creatorID int64,
	name string,
	description string,
	groupType models.GroupType,
	directKey string,
	memberIDs []int64,
) (models.Group, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Group{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO groups (name, description, type, direct_key, creator_id, create_time, update_time)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name,
		description,
		string(groupType),
		nullIfBlank(directKey),
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

	for _, memberID := range memberIDs {
		if memberID <= 0 {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO group_members (group_id, user_id, join_time) VALUES (?, ?, ?)`,
			groupID,
			memberID,
			now.Format(time.RFC3339Nano),
		); err != nil {
			return models.Group{}, err
		}
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
	var directKey sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, name, description, type, direct_key, creator_id, create_time, update_time
		FROM groups
		WHERE id = ?`,
		groupID,
	).Scan(
		&group.ID,
		&group.GroupName,
		&group.Description,
		&group.Type,
		&directKey,
		&group.CreatorID,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.Group{}, err
	}
	group.Type = normalizedGroupType(group.Type)
	group.DirectKey = strings.TrimSpace(directKey.String)
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

func (s *SQLStore) GetDirectGroupByKey(ctx context.Context, directKey string) (models.Group, error) {
	directKey = strings.TrimSpace(directKey)
	if directKey == "" {
		return models.Group{}, sql.ErrNoRows
	}
	var groupID int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id FROM groups WHERE direct_key = ?`,
		directKey,
	).Scan(&groupID)
	if err != nil {
		return models.Group{}, err
	}
	return s.GetGroupByID(ctx, groupID)
}

func (s *SQLStore) ListGroupsByUser(ctx context.Context, userID int64) ([]models.Group, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
			g.id,
			g.name,
			g.description,
			g.type,
			g.direct_key,
			g.creator_id,
			gm.last_read_message_id,
			COALESCE((
				SELECT MAX(msg.id)
				FROM group_messages msg
				WHERE msg.group_id = g.id AND msg.creator_id <> ?
			), 0) AS last_incoming_message_id,
			g.create_time,
			g.update_time
		FROM groups g
		JOIN group_members gm ON gm.group_id = g.id
		WHERE gm.user_id = ?
		ORDER BY g.update_time DESC, g.id DESC`,
		userID,
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
		var directKey sql.NullString
		if err := rows.Scan(
			&group.ID,
			&group.GroupName,
			&group.Description,
			&group.Type,
			&directKey,
			&group.CreatorID,
			&group.LastReadMessageID,
			&group.LastIncomingMessageID,
			&createTime,
			&updateTime,
		); err != nil {
			return nil, err
		}
		group.Type = normalizedGroupType(group.Type)
		group.DirectKey = strings.TrimSpace(directKey.String)
		group.CreateTime, err = parseTime(createTime)
		if err != nil {
			return nil, err
		}
		group.UpdateTime, err = parseTime(updateTime)
		if err != nil {
			return nil, err
		}
		group.HasUnread = group.LastIncomingMessageID > group.LastReadMessageID
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
		`SELECT u.id, u.username, u.avatar_url, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time
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

func (s *SQLStore) MarkGroupRead(ctx context.Context, groupID int64, userID int64, lastReadMessageID int64) error {
	if lastReadMessageID < 0 {
		lastReadMessageID = 0
	}
	if lastReadMessageID > 0 {
		var exists int
		if err := s.db.QueryRowContext(
			ctx,
			`SELECT 1 FROM group_messages WHERE id = ? AND group_id = ?`,
			lastReadMessageID,
			groupID,
		).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("last read message not found")
			}
			return err
		}
	}
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE group_members
		SET last_read_message_id = CASE
			WHEN last_read_message_id > ? THEN last_read_message_id
			ELSE ?
		END
		WHERE group_id = ? AND user_id = ?`,
		lastReadMessageID,
		lastReadMessageID,
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

