package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) GetUserEncryptionKeyByUserID(ctx context.Context, userID int64) (models.UserEncryptionKey, error) {
	var encryptionKey models.UserEncryptionKey
	var createTime string
	var updateTime string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT user_id, version, kdf_algorithm, kdf_salt, kdf_iterations, wrap_algorithm, wrapped_account_key, sharing_public_key, wrapped_sharing_private_key, key_version, algorithms, create_time, update_time
		FROM user_encryption_keys
		WHERE user_id = ?`,
		userID,
	).Scan(
		&encryptionKey.UserID,
		&encryptionKey.Version,
		&encryptionKey.KDFAlgorithm,
		&encryptionKey.KDFSalt,
		&encryptionKey.KDFIterations,
		&encryptionKey.WrapAlgorithm,
		&encryptionKey.WrappedAccountKey,
		&encryptionKey.SharingPublicKey,
		&encryptionKey.WrappedSharingPrivateKey,
		&encryptionKey.KeyVersion,
		&encryptionKey.Algorithms,
		&createTime,
		&updateTime,
	)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}

	var parseErr error
	encryptionKey.CreateTime, parseErr = parseTime(createTime)
	if parseErr != nil {
		return models.UserEncryptionKey{}, parseErr
	}
	encryptionKey.UpdateTime, parseErr = parseTime(updateTime)
	if parseErr != nil {
		return models.UserEncryptionKey{}, parseErr
	}
	return encryptionKey, nil
}

func (s *SQLStore) UpsertUserEncryptionKey(ctx context.Context, encryptionKey models.UserEncryptionKey) (models.UserEncryptionKey, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO user_encryption_keys (
			user_id, version, kdf_algorithm, kdf_salt, kdf_iterations, wrap_algorithm, wrapped_account_key, sharing_public_key, wrapped_sharing_private_key, key_version, algorithms, create_time, update_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			version = excluded.version,
			kdf_algorithm = excluded.kdf_algorithm,
			kdf_salt = excluded.kdf_salt,
			kdf_iterations = excluded.kdf_iterations,
			wrap_algorithm = excluded.wrap_algorithm,
			wrapped_account_key = excluded.wrapped_account_key,
			sharing_public_key = excluded.sharing_public_key,
			wrapped_sharing_private_key = excluded.wrapped_sharing_private_key,
			key_version = excluded.key_version,
			algorithms = excluded.algorithms,
			update_time = excluded.update_time`,
		encryptionKey.UserID,
		encryptionKey.Version,
		encryptionKey.KDFAlgorithm,
		encryptionKey.KDFSalt,
		encryptionKey.KDFIterations,
		encryptionKey.WrapAlgorithm,
		encryptionKey.WrappedAccountKey,
		encryptionKey.SharingPublicKey,
		encryptionKey.WrappedSharingPrivateKey,
		encryptionKey.KeyVersion,
		encryptionKey.Algorithms,
		now,
		now,
	)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	return s.GetUserEncryptionKeyByUserID(ctx, encryptionKey.UserID)
}

func (s *SQLStore) DeleteUserEncryptionKey(ctx context.Context, userID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM user_encryption_keys WHERE user_id = ?`, userID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}
