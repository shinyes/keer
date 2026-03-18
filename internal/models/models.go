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
	AvatarURL         string
	AvatarStorageType string
	PasswordHash      string
	Role              string
	DefaultVisibility Visibility
	CreateTime        time.Time
	UpdateTime        time.Time
}

type MemoEditGesture string

const (
	MemoEditGestureNone   MemoEditGesture = "NONE"
	MemoEditGestureSingle MemoEditGesture = "SINGLE"
	MemoEditGestureDouble MemoEditGesture = "DOUBLE"
	MemoEditGestureLong   MemoEditGesture = "LONG"
)

func (g MemoEditGesture) IsValid() bool {
	return g == MemoEditGestureNone ||
		g == MemoEditGestureSingle ||
		g == MemoEditGestureDouble ||
		g == MemoEditGestureLong
}

type MemoColumnConfig struct {
	ID              string
	Name            string
	RequiredTags    []string
	VisibleInDrawer bool
	PinnedMemoNames []string
}

type ExploreDrawerEntryConfig struct {
	EntryID          string
	VisibleInExplore bool
}

type UserGeneralSettings struct {
	UserID               int64
	MemoVisibility       Visibility
	MemoEditGesture      MemoEditGesture
	MemoColumns          []MemoColumnConfig
	ExploreDrawerEntries []ExploreDrawerEntryConfig
	CreateTime           time.Time
	UpdateTime           time.Time
}

type UserEncryptionKey struct {
	UserID                   int64
	Version                  int
	KDFAlgorithm             string
	KDFSalt                  string
	KDFTimeCost              int
	KDFMemoryKiB             int
	KDFParallelism           int
	WrapAlgorithm            string
	WrappedAccountKey        string
	SharingPublicKey         string
	WrappedSharingPrivateKey string
	KeyVersion               int
	Algorithms               string
	CreateTime               time.Time
	UpdateTime               time.Time
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

type RefreshToken struct {
	ID          int64
	UserID      int64
	TokenPrefix string
	TokenHash   string
	CreatedAt   time.Time
	LastUsedAt  *time.Time
	ExpiresAt   time.Time
	RevokedAt   *time.Time
}

type Memo struct {
	ID              int64
	CreatorID       int64
	Content         string
	PayloadEnvelope string
	Visibility      Visibility
	State           MemoState
	Pinned          bool
	CreateTime      time.Time
	UpdateTime      time.Time
	Latitude        *float64
	Longitude       *float64
	Payload         MemoPayload
}

type Group struct {
	ID                    int64
	GroupName             string
	Description           string
	Type                  GroupType
	DirectKey             string
	CreatorID             int64
	LastReadMessageID     int64
	LastIncomingMessageID int64
	HasUnread             bool
	CreateTime            time.Time
	UpdateTime            time.Time
}

type GroupType string

const (
	GroupTypeGroup  GroupType = "GROUP"
	GroupTypeDirect GroupType = "DIRECT"
)

type GroupMember struct {
	GroupID  int64
	UserID   int64
	JoinTime time.Time
}

type GroupTag struct {
	GroupID    int64
	Name       string
	CreatorID  int64
	CreateTime time.Time
	UpdateTime time.Time
}

type GroupMessage struct {
	ID              int64
	GroupID         int64
	CreatorID       int64
	Content         string
	PayloadEnvelope string
	CreateTime      time.Time
	UpdateTime      time.Time
	Tags            []string
}

type SyncDomain string

const (
	SyncDomainMemos         SyncDomain = "MEMOS"
	SyncDomainUsers         SyncDomain = "USERS"
	SyncDomainGroups        SyncDomain = "GROUPS"
	SyncDomainGroupMessages SyncDomain = "GROUP_MESSAGES"
	SyncDomainSettings      SyncDomain = "SETTINGS"
)

func (d SyncDomain) IsValid() bool {
	return d == SyncDomainMemos ||
		d == SyncDomainUsers ||
		d == SyncDomainGroups ||
		d == SyncDomainGroupMessages ||
		d == SyncDomainSettings
}

type SyncEvent struct {
	ID             int64
	Domain         SyncDomain
	ActorUserID    int64
	TargetUserID   int64
	GroupID        int64
	MemoID         int64
	GroupMessageID int64
	EventTime      time.Time
}

type GroupKeyVersion struct {
	GroupID    int64
	Version    int
	Algorithm  string
	CreateTime time.Time
	UpdateTime time.Time
}

type GroupKeyVersionRecipient struct {
	GroupID       int64
	Version       int
	UserID        int64
	SlotRef       string
	WrapAlgorithm string
	WrappedKey    string
	CreateTime    time.Time
	UpdateTime    time.Time
}

type Attachment struct {
	ID                            int64
	CreatorID                     int64
	Filename                      string
	ExternalLink                  string
	Type                          string
	Size                          int64
	EncryptionMetadata            string
	AssociationEncryptionMetadata string
	StorageType                   string
	StorageKey                    string
	ThumbnailFilename             string
	ThumbnailType                 string
	ThumbnailSize                 int64
	ThumbnailStorageType          string
	ThumbnailStorageKey           string
	CreateTime                    time.Time
}

type AttachmentUploadSession struct {
	ID                 string
	CreatorID          int64
	Filename           string
	Type               string
	Size               int64
	EncryptionMetadata string
	MemoName           *string
	TempPath           string
	ThumbnailFilename  string
	ThumbnailType      string
	ThumbnailTempPath  string
	ReceivedSize       int64
	CreateTime         time.Time
	UpdateTime         time.Time
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

func (g Group) Name() string {
	return "groups/" + Int64ToString(g.ID)
}

func (g GroupMessage) Name() string {
	return "groups/" + Int64ToString(g.GroupID) + "/messages/" + Int64ToString(g.ID)
}
