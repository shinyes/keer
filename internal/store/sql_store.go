package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/models"
)

type SQLStore struct {
	db *sql.DB
}

func New(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

func (s *SQLStore) DB() *sql.DB {
	return s.db
}

type MemoUpdate struct {
	Content         *string
	PayloadEnvelope *string
	Visibility      *models.Visibility
	State           *models.MemoState
	Pinned          *bool
	LatitudeSet     bool
	Latitude        *float64
	LongitudeSet    bool
	Longitude       *float64
	Payload         *models.MemoPayload
}

type AttachmentBinding struct {
	AttachmentID                  int64
	AssociationEncryptionMetadata string
}

type MemoQueryBounds struct {
	UpdatedAfter         *time.Time
	UpdatedBeforeOrEqual *time.Time
}

const (
	memoChangeEventTypeDelete            = "DELETE"
	memoChangeEventTypeVisibilityRevoked = "VISIBILITY_REVOKED"
)

func NormalizeTagNames(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, raw := range tags {
		tag := strings.TrimSpace(raw)
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func normalizeTagNames(tags []string) []string {
	return NormalizeTagNames(tags)
}

func scanMemo(scanner interface {
	Scan(dest ...any) error
}) (models.Memo, error) {
	var memo models.Memo
	var payloadEnvelope string
	var visibility string
	var state string
	var pinned int
	var createTime string
	var updateTime string
	var displayTime string
	var latitude sql.NullFloat64
	var longitude sql.NullFloat64
	var hasLink int
	var hasTaskList int
	var hasCode int
	var hasIncompleteTasks int
	if err := scanner.Scan(
		&memo.ID,
		&memo.CreatorID,
		&memo.Content,
		&payloadEnvelope,
		&visibility,
		&state,
		&pinned,
		&createTime,
		&updateTime,
		&displayTime,
		&latitude,
		&longitude,
		&hasLink,
		&hasTaskList,
		&hasCode,
		&hasIncompleteTasks,
	); err != nil {
		return models.Memo{}, err
	}
	memo.Visibility = models.Visibility(visibility)
	memo.State = models.MemoState(state)
	memo.Pinned = pinned == 1
	var err error
	memo.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Memo{}, err
	}
	memo.PayloadEnvelope = payloadEnvelope
	memo.UpdateTime, err = parseTime(updateTime)
	if err != nil {
		return models.Memo{}, err
	}
	if _, err = parseTime(displayTime); err != nil {
		return models.Memo{}, err
	}
	if latitude.Valid {
		memo.Latitude = &latitude.Float64
	}
	if longitude.Valid {
		memo.Longitude = &longitude.Float64
	}
	memo.Payload.Property = models.MemoPayloadProperty{
		HasLink:            hasLink == 1,
		HasTaskList:        hasTaskList == 1,
		HasCode:            hasCode == 1,
		HasIncompleteTasks: hasIncompleteTasks == 1,
	}
	memo.Payload.Tags = []string{}
	return memo, nil
}

func scanAttachment(scanner interface {
	Scan(dest ...any) error
}) (models.Attachment, error) {
	var attachment models.Attachment
	var createTime string
	if err := scanner.Scan(
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
	); err != nil {
		return models.Attachment{}, err
	}
	var err error
	attachment.CreateTime, err = parseTime(createTime)
	if err != nil {
		return models.Attachment{}, err
	}
	return attachment, nil
}

func parseTime(raw string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, raw)
}

func parseNullableTime(raw sql.NullString) (*time.Time, error) {
	if !raw.Valid {
		return nil, nil
	}
	t, err := parseTime(raw.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func collaboratorIDFromTag(tag string) (int64, bool) {
	tag = strings.TrimSpace(tag)
	const prefix = "collab/"
	if !strings.HasPrefix(tag, prefix) {
		return 0, false
	}
	rawID := strings.TrimSpace(strings.TrimPrefix(tag, prefix))
	if rawID == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func boolToSQLiteInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
