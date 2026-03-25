package http

import (
	"bytes"
	"encoding/json"
	"time"
)

type getCurrentUserResponse struct {
	User apiUser `json:"user"`
}

type signInRequest struct {
	PasswordCredentials *signInPasswordCredentials `json:"passwordCredentials"`
}

type signInPasswordCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type signInResponse struct {
	User                  apiUser `json:"user"`
	AccessToken           string  `json:"accessToken"`
	AccessTokenExpiresAt  string  `json:"accessTokenExpiresAt,omitempty"`
	RefreshToken          string  `json:"refreshToken"`
	RefreshTokenExpiresAt string  `json:"refreshTokenExpiresAt,omitempty"`
}

type refreshSessionRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type createUserRequest struct {
	User         createUserBody `json:"user"`
	UserID       string         `json:"userId"`
	ValidateOnly bool           `json:"validateOnly"`
	RequestID    string         `json:"requestId"`
}

type updateUserRequest struct {
	User updateUserBody `json:"user"`
}

type updateUserBody struct {
	AvatarURL *string                 `json:"avatarUrl"`
	Avatar    *updateUserAvatarUpload `json:"avatar"`
}

type updateUserAvatarUpload struct {
	Content string `json:"content"`
	Type    string `json:"type,omitempty"`
}

type createUserBody struct {
	Name        string `json:"name"`
	Role        string `json:"role"`
	Username    string `json:"username"`
	AvatarURL   string `json:"avatarUrl"`
	Description string `json:"description"`
	Password    string `json:"password"`
	State       string `json:"state"`
}

type apiUser struct {
	Name        string `json:"name"`
	Role        string `json:"role,omitempty"`
	Username    string `json:"username"`
	AvatarURL   string `json:"avatarUrl,omitempty"`
	Description string `json:"description,omitempty"`
	State       string `json:"state,omitempty"`
	CreateTime  string `json:"createTime,omitempty"`
	UpdateTime  string `json:"updateTime,omitempty"`
}

type listUsersResponse struct {
	Users []apiUser `json:"users"`
}

type addFriendRequest struct {
	User string `json:"user"`
}

type listUserChangesResponse struct {
	Users      []apiUser `json:"users"`
	SyncAnchor string    `json:"syncAnchor"`
}

type listMemosResponse struct {
	Memos         []apiMemo `json:"memos"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

type listMemoChangesResponse struct {
	Memos            []apiMemo `json:"memos"`
	DeletedMemoNames []string  `json:"deletedMemoNames"`
	SyncAnchor       string    `json:"syncAnchor"`
}

type syncPullRequest struct {
	Cursor      string   `json:"cursor"`
	Domains     []string `json:"domains"`
	GroupScopes []string `json:"groupScopes"`
	Limit       int      `json:"limit"`
}

type syncPullResponse struct {
	NextCursor string          `json:"nextCursor"`
	HasMore    bool            `json:"hasMore"`
	Patches    syncPullPatches `json:"patches"`
}

type syncPullPatches struct {
	Memos         syncPullMemoPatch          `json:"memos"`
	Users         syncPullUserPatch          `json:"users"`
	Groups        syncPullGroupPatch         `json:"groups"`
	GroupMessages syncPullGroupMessagesPatch `json:"groupMessages"`
	Settings      syncPullSettingsPatch      `json:"settings"`
}

type syncPullMemoPatch struct {
	Upserts []apiMemo `json:"upserts"`
	Deletes []string  `json:"deletes"`
}

type syncPullUserPatch struct {
	Upserts []apiUser `json:"upserts"`
}

type syncPullGroupPatch struct {
	Upserts []apiGroup `json:"upserts"`
	Deletes []string   `json:"deletes"`
}

type syncPullGroupMessagesPatch struct {
	Groups []syncPullGroupMessagesGroupPatch `json:"groups"`
}

type syncPullGroupMessagesGroupPatch struct {
	Group     string            `json:"group"`
	HasUnread bool              `json:"hasUnread"`
	Upserts   []apiGroupMessage `json:"upserts"`
	Deletes   []string          `json:"deletes"`
	Tags      []string          `json:"tags"`
}

type syncPullSettingsPatch struct {
	GeneralSetting *generalSetting `json:"generalSetting,omitempty"`
}

type apiWrappedKeySlot struct {
	SlotType      string `json:"slotType"`
	SlotRef       string `json:"slotRef"`
	WrapAlgorithm string `json:"wrapAlgorithm"`
	WrappedKey    string `json:"wrappedKey"`
}

type apiPayloadEnvelope struct {
	WrappedKeys []apiWrappedKeySlot `json:"wrappedKeys"`
}

type createMemoRequest struct {
	EncryptedPayload string             `json:"encryptedPayload"`
	PayloadEnvelope  apiPayloadEnvelope `json:"payloadEnvelope"`
	Visibility       string             `json:"visibility"`
	Tags             []string           `json:"tags,omitempty"`
	Attachments      []apiAttachment    `json:"attachments"`
	CreateTime       *string            `json:"createTime"`
	Latitude         *float64           `json:"latitude,omitempty"`
	Longitude        *float64           `json:"longitude,omitempty"`
}

type updateMemoRequest struct {
	EncryptedPayload *string             `json:"encryptedPayload"`
	PayloadEnvelope  *apiPayloadEnvelope `json:"payloadEnvelope"`
	Visibility       *string             `json:"visibility"`
	Tags             *[]string           `json:"tags"`
	State            *string             `json:"state"`
	Pinned           *bool               `json:"pinned"`
	Attachments      *[]apiAttachment    `json:"attachments"`
	Latitude         optionalFloat64     `json:"latitude"`
	Longitude        optionalFloat64     `json:"longitude"`
}

type apiMemo struct {
	Name             string              `json:"name"`
	State            string              `json:"state,omitempty"`
	Creator          string              `json:"creator,omitempty"`
	CreateTime       string              `json:"createTime,omitempty"`
	UpdateTime       string              `json:"updateTime,omitempty"`
	EncryptedPayload string              `json:"encryptedPayload,omitempty"`
	PayloadEnvelope  *apiPayloadEnvelope `json:"payloadEnvelope,omitempty"`
	Visibility       string              `json:"visibility,omitempty"`
	Pinned           bool                `json:"pinned"`
	Latitude         *float64            `json:"latitude,omitempty"`
	Longitude        *float64            `json:"longitude,omitempty"`
	Attachments      []apiAttachment     `json:"attachments,omitempty"`
	Tags             []string            `json:"tags,omitempty"`
	Quote            *apiMemoQuote       `json:"quote,omitempty"`
}

type apiMemoQuote struct {
	SourceKind string            `json:"sourceKind"`
	Source     string            `json:"source"`
	Memo       *apiMemoQuoteMemo `json:"memo,omitempty"`
}

type apiMemoQuoteMemo struct {
	Name             string              `json:"name"`
	Creator          string              `json:"creator,omitempty"`
	CreateTime       string              `json:"createTime,omitempty"`
	UpdateTime       string              `json:"updateTime,omitempty"`
	EncryptedPayload string              `json:"encryptedPayload,omitempty"`
	PayloadEnvelope  *apiPayloadEnvelope `json:"payloadEnvelope,omitempty"`
	Visibility       string              `json:"visibility,omitempty"`
	Attachments      []apiAttachment     `json:"attachments,omitempty"`
	Tags             []string            `json:"tags,omitempty"`
}

type listGroupsResponse struct {
	Groups []apiGroup `json:"groups"`
}

type createGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type updateGroupRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

type addGroupMemberRequest struct {
	User string `json:"user"`
}

type createDirectGroupRequest struct {
	User string `json:"user"`
}

type apiGroupMember struct {
	Name     string `json:"name"`
	Username string `json:"username"`
}

type apiGroup struct {
	Name        string           `json:"name"`
	Creator     string           `json:"creator"`
	CreateTime  string           `json:"createTime,omitempty"`
	UpdateTime  string           `json:"updateTime,omitempty"`
	Type        string           `json:"type,omitempty"`
	HasUnread   bool             `json:"hasUnread"`
	GroupName   string           `json:"groupName"`
	Description string           `json:"description,omitempty"`
	Members     []apiGroupMember `json:"members,omitempty"`
}

type markGroupReadRequest struct {
	LastReadMessage string `json:"lastReadMessage"`
}

type listGroupMessagesResponse struct {
	Messages      []apiGroupMessage `json:"messages"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

type createGroupMessageRequest struct {
	EncryptedPayload string             `json:"encryptedPayload"`
	PayloadEnvelope  apiPayloadEnvelope `json:"payloadEnvelope"`
	Tags             []string           `json:"tags,omitempty"`
	Attachments      []apiAttachment    `json:"attachments,omitempty"`
}

type updateGroupMessageRequest struct {
	EncryptedPayload *string             `json:"encryptedPayload"`
	PayloadEnvelope  *apiPayloadEnvelope `json:"payloadEnvelope"`
	Tags             *[]string           `json:"tags"`
	Attachments      *[]apiAttachment    `json:"attachments"`
}

type apiGroupMessage struct {
	Name             string              `json:"name"`
	Group            string              `json:"group"`
	Creator          string              `json:"creator"`
	CreateTime       string              `json:"createTime,omitempty"`
	UpdateTime       string              `json:"updateTime,omitempty"`
	EncryptedPayload string              `json:"encryptedPayload,omitempty"`
	PayloadEnvelope  *apiPayloadEnvelope `json:"payloadEnvelope,omitempty"`
	Tags             []string            `json:"tags,omitempty"`
	Attachments      []apiAttachment     `json:"attachments,omitempty"`
}

type listGroupTagsResponse struct {
	Tags []string `json:"tags"`
}

type addGroupTagRequest struct {
	Tag string `json:"tag"`
}

type createAttachmentRequest struct {
	DescriptorCiphertext    string              `json:"descriptorCiphertext"`
	DescriptorEnvelope      *apiPayloadEnvelope `json:"descriptorEnvelope"`
	BlobEncryption          string              `json:"blobEncryption"`
	ThumbnailBlobEncryption string              `json:"thumbnailBlobEncryption"`
	Filename                string              `json:"filename"`
	Type                    string              `json:"type"`
	Content                 string              `json:"content"`
	Memo                    *string             `json:"memo"`
}

type createAttachmentUploadSessionRequest struct {
	DescriptorCiphertext    string                                  `json:"descriptorCiphertext"`
	DescriptorEnvelope      *apiPayloadEnvelope                     `json:"descriptorEnvelope"`
	BlobEncryption          string                                  `json:"blobEncryption"`
	ThumbnailBlobEncryption string                                  `json:"thumbnailBlobEncryption"`
	Filename                string                                  `json:"filename"`
	Type                    string                                  `json:"type"`
	Size                    int64                                   `json:"size"`
	Memo                    *string                                 `json:"memo"`
	Thumbnail               *createAttachmentUploadThumbnailRequest `json:"thumbnail"`
}

type createAttachmentUploadThumbnailRequest struct {
	Filename string `json:"filename"`
	Type     string `json:"type"`
	Content  string `json:"content"`
}

type updateAttachmentThumbnailRequest struct {
	Filename                string  `json:"filename"`
	Type                    string  `json:"type"`
	Content                 string  `json:"content"`
	ThumbnailBlobEncryption *string `json:"thumbnailBlobEncryption"`
}

type attachmentUploadSessionResponse struct {
	UploadID           string  `json:"uploadId"`
	Filename           string  `json:"filename"`
	Type               string  `json:"type"`
	Size               string  `json:"size"`
	UploadedSize       string  `json:"uploadedSize"`
	Memo               *string `json:"memo,omitempty"`
	UploadMode         string  `json:"uploadMode,omitempty"`
	DirectUploadURL    string  `json:"directUploadUrl,omitempty"`
	DirectUploadMethod string  `json:"directUploadMethod,omitempty"`
	MultipartPartSize  string  `json:"multipartPartSize,omitempty"`
}

type attachmentMultipartPartUploadResponse struct {
	UploadID   string `json:"uploadId"`
	PartNumber int32  `json:"partNumber"`
	Offset     string `json:"offset"`
	Size       string `json:"size"`
	UploadURL  string `json:"uploadUrl"`
	Method     string `json:"method"`
}

type listAttachmentsResponse struct {
	Attachments []apiAttachment `json:"attachments"`
}

type apiAttachment struct {
	Name                    string              `json:"name"`
	CreateTime              string              `json:"createTime,omitempty"`
	DescriptorCiphertext    string              `json:"descriptorCiphertext,omitempty"`
	BlobEncryption          string              `json:"blobEncryption,omitempty"`
	ThumbnailBlobEncryption string              `json:"thumbnailBlobEncryption,omitempty"`
	Filename                string              `json:"filename,omitempty"`
	ExternalLink            string              `json:"externalLink,omitempty"`
	Type                    string              `json:"type,omitempty"`
	Size                    string              `json:"size,omitempty"`
	DescriptorEnvelope      *apiPayloadEnvelope `json:"descriptorEnvelope,omitempty"`
	ThumbnailName           string              `json:"thumbnailName,omitempty"`
	ThumbnailExternalLink   string              `json:"thumbnailExternalLink,omitempty"`
	ThumbnailFilename       string              `json:"thumbnailFilename,omitempty"`
	ThumbnailType           string              `json:"thumbnailType,omitempty"`
	Memo                    string              `json:"memo,omitempty"`
}

type userSettingResponse struct {
	GeneralSetting generalSetting `json:"generalSetting"`
}

type generalSetting struct {
	MemoVisibility       string                        `json:"memoVisibility,omitempty"`
	MemoEditGesture      string                        `json:"memoEditGesture,omitempty"`
	MemoColumns          []apiMemoColumnConfig         `json:"memoColumns,omitempty"`
	ExploreDrawerEntries []apiExploreDrawerEntryConfig `json:"exploreDrawerEntries,omitempty"`
	Locale               string                        `json:"locale,omitempty"`
	Theme                string                        `json:"theme,omitempty"`
}

type apiMemoColumnConfig struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	RequiredTags    []string `json:"requiredTags"`
	VisibleInDrawer bool     `json:"visibleInDrawer"`
	PinnedMemoNames []string `json:"pinnedMemoNames"`
}

type apiExploreDrawerEntryConfig struct {
	EntryID          string `json:"entryId"`
	VisibleInExplore bool   `json:"visibleInExplore"`
}

type updateUserGeneralSettingRequest struct {
	GeneralSetting updateUserGeneralSetting `json:"generalSetting"`
}

type updateUserGeneralSetting struct {
	MemoVisibility       string                        `json:"memoVisibility"`
	MemoEditGesture      string                        `json:"memoEditGesture"`
	MemoColumns          []apiMemoColumnConfig         `json:"memoColumns"`
	ExploreDrawerEntries []apiExploreDrawerEntryConfig `json:"exploreDrawerEntries"`
}

type userEncryptionSettingResponse struct {
	EncryptionSetting apiUserEncryptionSetting `json:"encryptionSetting"`
}

type apiRecoveryBundle struct {
	Version           int    `json:"version"`
	KDFAlgorithm      string `json:"kdfAlgorithm"`
	KDFSalt           string `json:"kdfSalt"`
	KDFTimeCost       int    `json:"kdfTimeCost"`
	KDFMemoryKiB      int    `json:"kdfMemoryKiB"`
	KDFParallelism    int    `json:"kdfParallelism"`
	WrapAlgorithm     string `json:"wrapAlgorithm"`
	WrappedAccountKey string `json:"wrappedAccountKey"`
}

type apiUserEncryptionSetting struct {
	RecoveryBundle           apiRecoveryBundle `json:"recoveryBundle"`
	SharingPublicKey         string            `json:"sharingPublicKey"`
	WrappedSharingPrivateKey string            `json:"wrappedSharingPrivateKey"`
	KeyVersion               int               `json:"keyVersion"`
	Algorithms               string            `json:"algorithms"`
	CreateTime               string            `json:"createTime,omitempty"`
	UpdateTime               string            `json:"updateTime,omitempty"`
}

type updateUserEncryptionSettingRequest struct {
	EncryptionSetting updateUserEncryptionSetting `json:"encryptionSetting"`
}

type changeUserPasswordRequest struct {
	CurrentPassword   string                      `json:"currentPassword"`
	NewPassword       string                      `json:"newPassword"`
	EncryptionSetting updateUserEncryptionSetting `json:"encryptionSetting"`
}

type updateUserEncryptionSetting struct {
	RecoveryBundle           apiRecoveryBundle `json:"recoveryBundle"`
	SharingPublicKey         string            `json:"sharingPublicKey"`
	WrappedSharingPrivateKey string            `json:"wrappedSharingPrivateKey"`
	KeyVersion               int               `json:"keyVersion"`
	Algorithms               string            `json:"algorithms"`
}

type listUserPublicKeysResponse struct {
	Users []apiUserPublicKey `json:"users"`
}

type apiUserPublicKey struct {
	Name             string `json:"name"`
	SharingPublicKey string `json:"sharingPublicKey"`
	KeyVersion       int    `json:"keyVersion"`
}

type groupKeyVersionResponse struct {
	GroupKeyVersion apiGroupKeyVersion `json:"groupKeyVersion"`
}

type createGroupKeyVersionRequest struct {
	GroupKeyVersion createGroupKeyVersionBody `json:"groupKeyVersion"`
}

type createGroupKeyVersionBody struct {
	Algorithm   string              `json:"algorithm"`
	WrappedKeys []apiWrappedKeySlot `json:"wrappedKeys"`
}

type apiGroupKeyVersion struct {
	Name        string              `json:"name"`
	Group       string              `json:"group"`
	Version     int                 `json:"version"`
	Algorithm   string              `json:"algorithm"`
	WrappedKeys []apiWrappedKeySlot `json:"wrappedKeys,omitempty"`
	CreateTime  string              `json:"createTime,omitempty"`
	UpdateTime  string              `json:"updateTime,omitempty"`
}

type userStatsResponse struct {
	TagCount map[string]int `json:"tagCount"`
}

type storageCleanupResponse struct {
	Cleanup storageCleanupResult `json:"cleanup"`
}

type storageCleanupResult struct {
	ScannedKeys int `json:"scannedKeys"`
	DeletedKeys int `json:"deletedKeys"`
	FailedKeys  int `json:"failedKeys"`
}

type optionalFloat64 struct {
	Set   bool
	Value *float64
}

func (o *optionalFloat64) UnmarshalJSON(data []byte) error {
	o.Set = true
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		o.Value = nil
		return nil
	}

	var value float64
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func formatMaybeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return formatTime(t)
}
