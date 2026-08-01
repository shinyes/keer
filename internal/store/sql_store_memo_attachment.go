package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"strings"
	"time"
)

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
