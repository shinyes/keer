package db

import (
	"database/sql"
	"fmt"
)

func Migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL UNIQUE COLLATE NOCASE,
			display_name TEXT NOT NULL,
			email TEXT NOT NULL DEFAULT '',
			password_hash TEXT NOT NULL DEFAULT '',
			role TEXT NOT NULL DEFAULT 'USER',
			default_visibility TEXT NOT NULL DEFAULT 'PRIVATE',
			create_time TEXT NOT NULL,
			update_time TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS personal_access_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			token_prefix TEXT NOT NULL,
			token_hash TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			last_used_at TEXT,
			expires_at TEXT,
			revoked_at TEXT,
			FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS memos (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			creator_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			visibility TEXT NOT NULL DEFAULT 'PRIVATE',
			state TEXT NOT NULL DEFAULT 'NORMAL',
			pinned INTEGER NOT NULL DEFAULT 0,
			create_time TEXT NOT NULL,
			update_time TEXT NOT NULL,
			display_time TEXT NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(creator_id) REFERENCES users(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_memos_creator ON memos(creator_id);`,
		`CREATE INDEX IF NOT EXISTS idx_memos_state ON memos(state);`,
		`CREATE TABLE IF NOT EXISTS attachments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			creator_id INTEGER NOT NULL,
			filename TEXT NOT NULL,
			external_link TEXT NOT NULL DEFAULT '',
			type TEXT NOT NULL,
			size INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			storage_type TEXT NOT NULL,
			storage_key TEXT NOT NULL,
			thumbnail_filename TEXT NOT NULL DEFAULT '',
			thumbnail_type TEXT NOT NULL DEFAULT '',
			thumbnail_size INTEGER NOT NULL DEFAULT 0,
			thumbnail_storage_type TEXT NOT NULL DEFAULT '',
			thumbnail_storage_key TEXT NOT NULL DEFAULT '',
			create_time TEXT NOT NULL,
			FOREIGN KEY(creator_id) REFERENCES users(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_attachments_creator ON attachments(creator_id);`,
		`CREATE INDEX IF NOT EXISTS idx_attachments_creator_hash ON attachments(creator_id, content_hash);`,
		`CREATE TABLE IF NOT EXISTS memo_attachments (
			memo_id INTEGER NOT NULL,
			attachment_id INTEGER NOT NULL,
			position INTEGER NOT NULL,
			PRIMARY KEY(memo_id, attachment_id),
			FOREIGN KEY(memo_id) REFERENCES memos(id) ON DELETE CASCADE,
			FOREIGN KEY(attachment_id) REFERENCES attachments(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_memo_attachments_memo ON memo_attachments(memo_id, position);`,
		`CREATE TABLE IF NOT EXISTS attachment_upload_sessions (
			id TEXT PRIMARY KEY,
			creator_id INTEGER NOT NULL,
			filename TEXT NOT NULL,
			type TEXT NOT NULL,
			size INTEGER NOT NULL,
			memo_name TEXT,
			temp_path TEXT NOT NULL,
			thumbnail_filename TEXT NOT NULL DEFAULT '',
			thumbnail_type TEXT NOT NULL DEFAULT '',
			thumbnail_temp_path TEXT NOT NULL DEFAULT '',
			received_size INTEGER NOT NULL DEFAULT 0,
			create_time TEXT NOT NULL,
			update_time TEXT NOT NULL,
			FOREIGN KEY(creator_id) REFERENCES users(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_attachment_upload_sessions_creator ON attachment_upload_sessions(creator_id);`,
		`CREATE INDEX IF NOT EXISTS idx_attachment_upload_sessions_update_time ON attachment_upload_sessions(update_time);`,
		`CREATE TABLE IF NOT EXISTS system_settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			update_time TEXT NOT NULL
		);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	if err := ensureColumn(
		db,
		"attachment_upload_sessions",
		"thumbnail_filename",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := ensureColumn(
		db,
		"attachment_upload_sessions",
		"thumbnail_type",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := ensureColumn(
		db,
		"attachment_upload_sessions",
		"thumbnail_temp_path",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	return nil
}

func ensureColumn(db *sql.DB, table string, column string, definition string) error {
	exists, err := hasColumn(db, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition))
	return err
}

func hasColumn(db *sql.DB, table string, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}
