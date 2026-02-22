package db

import (
	"database/sql"
	"fmt"
	"strings"
)

func Migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL UNIQUE,
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
			storage_type TEXT NOT NULL,
			storage_key TEXT NOT NULL,
			create_time TEXT NOT NULL,
			FOREIGN KEY(creator_id) REFERENCES users(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_attachments_creator ON attachments(creator_id);`,
		`CREATE TABLE IF NOT EXISTS memo_attachments (
			memo_id INTEGER NOT NULL,
			attachment_id INTEGER NOT NULL,
			position INTEGER NOT NULL,
			PRIMARY KEY(memo_id, attachment_id),
			FOREIGN KEY(memo_id) REFERENCES memos(id) ON DELETE CASCADE,
			FOREIGN KEY(attachment_id) REFERENCES attachments(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_memo_attachments_memo ON memo_attachments(memo_id, position);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	hasPayload, err := hasColumn(db, "memos", "payload_json")
	if err != nil {
		return err
	}
	if !hasPayload {
		if _, err := db.Exec(`ALTER TABLE memos ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}';`); err != nil {
			return fmt.Errorf("add memos.payload_json: %w", err)
		}
	}

	hasUserEmail, err := hasColumn(db, "users", "email")
	if err != nil {
		return err
	}
	if !hasUserEmail {
		if _, err := db.Exec(`ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT '';`); err != nil {
			return fmt.Errorf("add users.email: %w", err)
		}
	}

	hasUserPasswordHash, err := hasColumn(db, "users", "password_hash")
	if err != nil {
		return err
	}
	if !hasUserPasswordHash {
		if _, err := db.Exec(`ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT '';`); err != nil {
			return fmt.Errorf("add users.password_hash: %w", err)
		}
	}

	return nil
}

func hasColumn(db *sql.DB, tableName string, columnName string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s);`, tableName))
	if err != nil {
		return false, fmt.Errorf("table info %s: %w", tableName, err)
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
			return false, fmt.Errorf("scan table_info(%s): %w", tableName, err)
		}
		if strings.EqualFold(name, columnName) {
			return true, nil
		}
	}
	return false, rows.Err()
}
