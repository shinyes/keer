package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func normalizeFriendshipPair(userID int64, friendID int64) (int64, int64) {
	if userID < friendID {
		return userID, friendID
	}
	return friendID, userID
}

func (s *SQLStore) AddFriend(ctx context.Context, userID int64, friendID int64) error {
	leftID, rightID := normalizeFriendshipPair(userID, friendID)
	_, err := s.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO friendships (user_id, friend_id, create_time) VALUES (?, ?, ?)`,
		leftID,
		rightID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLStore) RemoveFriend(ctx context.Context, userID int64, friendID int64) error {
	leftID, rightID := normalizeFriendshipPair(userID, friendID)
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM friendships WHERE user_id = ? AND friend_id = ?`,
		leftID,
		rightID,
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

func (s *SQLStore) AreFriends(ctx context.Context, userID int64, friendID int64) (bool, error) {
	leftID, rightID := normalizeFriendshipPair(userID, friendID)
	var exists int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM friendships WHERE user_id = ? AND friend_id = ?`,
		leftID,
		rightID,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLStore) ListFriends(ctx context.Context, userID int64) ([]models.User, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.id, u.username, u.display_name, u.avatar_url, u.password_hash, u.role, u.default_visibility, u.create_time, u.update_time
		FROM friendships f
		JOIN users u ON u.id = CASE
			WHEN f.user_id = ? THEN f.friend_id
			ELSE f.user_id
		END
		WHERE f.user_id = ? OR f.friend_id = ?
		ORDER BY LOWER(u.display_name) ASC, LOWER(u.username) ASC, u.id ASC`,
		userID,
		userID,
		userID,
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
			&user.DisplayName,
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
