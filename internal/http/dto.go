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
	User                 apiUser `json:"user"`
	AccessToken          string  `json:"accessToken"`
	AccessTokenExpiresAt string  `json:"accessTokenExpiresAt,omitempty"`
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
	DisplayName string `json:"displayName"`
	AvatarURL   string `json:"avatarUrl"`
	Description string `json:"description"`
	Password    string `json:"password"`
	State       string `json:"state"`
}

type apiUser struct {
	Name        string `json:"name"`
	Role        string `json:"role,omitempty"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName,omitempty"`
	AvatarURL   string `json:"avatarUrl,omitempty"`
	Description string `json:"description,omitempty"`
	State       string `json:"state,omitempty"`
	CreateTime  string `json:"createTime,omitempty"`
	UpdateTime  string `json:"updateTime,omitempty"`
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

type createMemoRequest struct {
	Content     string          `json:"content"`
	Visibility  string          `json:"visibility"`
	Tags        []string        `json:"tags,omitempty"`
	Attachments []apiAttachment `json:"attachments"`
	CreateTime  *string         `json:"createTime"`
	Latitude    *float64        `json:"latitude,omitempty"`
	Longitude   *float64        `json:"longitude,omitempty"`
}

type updateMemoRequest struct {
	Content     *string          `json:"content"`
	Visibility  *string          `json:"visibility"`
	Tags        *[]string        `json:"tags"`
	State       *string          `json:"state"`
	Pinned      *bool            `json:"pinned"`
	Attachments *[]apiAttachment `json:"attachments"`
	Latitude    optionalFloat64  `json:"latitude"`
	Longitude   optionalFloat64  `json:"longitude"`
}

type apiMemo struct {
	Name        string          `json:"name"`
	State       string          `json:"state,omitempty"`
	Creator     string          `json:"creator,omitempty"`
	CreateTime  string          `json:"createTime,omitempty"`
	UpdateTime  string          `json:"updateTime,omitempty"`
	Content     string          `json:"content,omitempty"`
	Visibility  string          `json:"visibility,omitempty"`
	Pinned      bool            `json:"pinned"`
	Latitude    *float64        `json:"latitude,omitempty"`
	Longitude   *float64        `json:"longitude,omitempty"`
	Attachments []apiAttachment `json:"attachments,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
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

type apiGroupMember struct {
	Name        string `json:"name"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName,omitempty"`
}

type apiGroup struct {
	Name        string           `json:"name"`
	Creator     string           `json:"creator"`
	CreateTime  string           `json:"createTime,omitempty"`
	UpdateTime  string           `json:"updateTime,omitempty"`
	GroupName   string           `json:"groupName"`
	Description string           `json:"description,omitempty"`
	Members     []apiGroupMember `json:"members,omitempty"`
}

type listGroupMessagesResponse struct {
	Messages      []apiGroupMessage `json:"messages"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

type createGroupMessageRequest struct {
	Content string   `json:"content"`
	Tags    []string `json:"tags,omitempty"`
}

type apiGroupMessage struct {
	Name       string   `json:"name"`
	Group      string   `json:"group"`
	Creator    string   `json:"creator"`
	CreateTime string   `json:"createTime,omitempty"`
	UpdateTime string   `json:"updateTime,omitempty"`
	Content    string   `json:"content,omitempty"`
	Tags       []string `json:"tags,omitempty"`
}

type listGroupTagsResponse struct {
	Tags []string `json:"tags"`
}

type addGroupTagRequest struct {
	Tag string `json:"tag"`
}

type createAttachmentRequest struct {
	Filename string  `json:"filename"`
	Type     string  `json:"type"`
	Content  string  `json:"content"`
	Memo     *string `json:"memo"`
}

type createAttachmentUploadSessionRequest struct {
	Filename  string                                  `json:"filename"`
	Type      string                                  `json:"type"`
	Size      int64                                   `json:"size"`
	Memo      *string                                 `json:"memo"`
	Thumbnail *createAttachmentUploadThumbnailRequest `json:"thumbnail"`
}

type createAttachmentUploadThumbnailRequest struct {
	Filename string `json:"filename"`
	Type     string `json:"type"`
	Content  string `json:"content"`
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
	Name                  string `json:"name"`
	CreateTime            string `json:"createTime,omitempty"`
	Filename              string `json:"filename,omitempty"`
	ExternalLink          string `json:"externalLink,omitempty"`
	Type                  string `json:"type,omitempty"`
	Size                  string `json:"size,omitempty"`
	ThumbnailName         string `json:"thumbnailName,omitempty"`
	ThumbnailExternalLink string `json:"thumbnailExternalLink,omitempty"`
	ThumbnailFilename     string `json:"thumbnailFilename,omitempty"`
	ThumbnailType         string `json:"thumbnailType,omitempty"`
	Memo                  string `json:"memo,omitempty"`
}

type userSettingResponse struct {
	GeneralSetting generalSetting `json:"generalSetting"`
}

type generalSetting struct {
	MemoVisibility string `json:"memoVisibility,omitempty"`
}

type userStatsResponse struct {
	TagCount map[string]int `json:"tagCount"`
}

type profileResponse struct {
	KeerAPIVersion string `json:"keer_api_version"`
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
