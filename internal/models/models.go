package models

import (
	"strconv"
	"time"
)

type Visibility string

const (
	VisibilityPrivate   Visibility = "PRIVATE"
	VisibilityProtected Visibility = "PROTECTED"
	VisibilityPublic    Visibility = "PUBLIC"
)

func (v Visibility) IsValid() bool {
	return v == VisibilityPrivate || v == VisibilityProtected || v == VisibilityPublic
}

type MemoState string

const (
	MemoStateNormal   MemoState = "NORMAL"
	MemoStateArchived MemoState = "ARCHIVED"
)

func (s MemoState) IsValid() bool {
	return s == MemoStateNormal || s == MemoStateArchived
}

type MemoPayloadProperty struct {
	HasLink            bool `json:"hasLink"`
	HasTaskList        bool `json:"hasTaskList"`
	HasCode            bool `json:"hasCode"`
	HasIncompleteTasks bool `json:"hasIncompleteTasks"`
}

type MemoPayload struct {
	Tags     []string            `json:"tags"`
	Property MemoPayloadProperty `json:"property"`
}

type User struct {
	ID                int64
	Username          string
	DisplayName       string
	PasswordHash      string
	Role              string
	DefaultVisibility Visibility
	CreateTime        time.Time
	UpdateTime        time.Time
}

type PersonalAccessToken struct {
	ID          int64
	UserID      int64
	TokenPrefix string
	TokenHash   string
	Description string
	CreatedAt   time.Time
	LastUsedAt  *time.Time
	ExpiresAt   *time.Time
	RevokedAt   *time.Time
}

type Memo struct {
	ID          int64
	CreatorID   int64
	Content     string
	Visibility  Visibility
	State       MemoState
	Pinned      bool
	CreateTime  time.Time
	UpdateTime  time.Time
	DisplayTime time.Time
	Payload     MemoPayload
}

type Attachment struct {
	ID           int64
	CreatorID    int64
	Filename     string
	ExternalLink string
	Type         string
	Size         int64
	StorageType  string
	StorageKey   string
	CreateTime   time.Time
}

type AttachmentUploadSession struct {
	ID           string
	CreatorID    int64
	Filename     string
	Type         string
	Size         int64
	MemoName     *string
	TempPath     string
	ReceivedSize int64
	CreateTime   time.Time
	UpdateTime   time.Time
}

func (m Memo) Name() string {
	return "memos/" + Int64ToString(m.ID)
}

func (u User) Name() string {
	return "users/" + Int64ToString(u.ID)
}

func Int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}
