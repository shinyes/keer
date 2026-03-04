package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type MemoService struct {
	store *store.SQLStore
}

func NewMemoService(s *store.SQLStore) *MemoService {
	return &MemoService{
		store: s,
	}
}

type CreateMemoInput struct {
	Content         string
	Visibility      models.Visibility
	Tags            []string
	AttachmentNames []string
	CreateTime      *time.Time // 客户端指定的创建时间，为 nil 时使用当前时间
	Latitude        *float64
	Longitude       *float64
}

type UpdateMemoInput struct {
	Content         *string
	Visibility      *models.Visibility
	Tags            *[]string
	State           *models.MemoState
	Pinned          *bool
	AttachmentNames *[]string
	LatitudeSet     bool
	Latitude        *float64
	LongitudeSet    bool
	Longitude       *float64
}

type MemoWithAttachments struct {
	Memo        models.Memo
	Attachments []models.Attachment
}

type MemoChanges struct {
	Memos            []MemoWithAttachments
	DeletedMemoNames []string
	SyncAnchor       time.Time
}

func (s *MemoService) CreateMemo(ctx context.Context, creatorID int64, input CreateMemoInput) (MemoWithAttachments, error) {
	content := input.Content
	visibility := input.Visibility
	if !visibility.IsValid() {
		visibility = models.VisibilityPrivate
	}
	if err := validateCoordinates(input.Latitude, input.Longitude); err != nil {
		return MemoWithAttachments{}, err
	}

	payload := models.MemoPayload{
		Tags: normalizeMemoTags(input.Tags),
	}

	attachmentIDs, err := s.resolveAttachmentIDsFromNames(ctx, creatorID, input.AttachmentNames)
	if err != nil {
		return MemoWithAttachments{}, err
	}

	createTime := time.Now().UTC()
	if input.CreateTime != nil && !input.CreateTime.IsZero() {
		createTime = input.CreateTime.UTC()
	}

	memo, err := s.store.CreateMemoWithAttachments(
		ctx,
		creatorID,
		content,
		visibility,
		models.MemoStateNormal,
		false,
		payload,
		createTime,
		input.Latitude,
		input.Longitude,
		attachmentIDs,
	)
	if err != nil {
		return MemoWithAttachments{}, err
	}
	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memo.ID})
	if err != nil {
		return MemoWithAttachments{}, err
	}

	result := MemoWithAttachments{
		Memo:        memo,
		Attachments: attachmentsMap[memo.ID],
	}
	return result, nil
}

func (s *MemoService) UpdateMemo(ctx context.Context, updaterID int64, memoID int64, input UpdateMemoInput) (MemoWithAttachments, error) {
	current, err := s.store.GetMemoByID(ctx, memoID)
	if err != nil {
		return MemoWithAttachments{}, err
	}
	if !canManageMemo(current, updaterID) {
		return MemoWithAttachments{}, sql.ErrNoRows
	}
	if err := validateCoordinates(input.Latitude, input.Longitude); err != nil {
		return MemoWithAttachments{}, err
	}

	update := store.MemoUpdate{}
	if input.Content != nil {
		content := *input.Content
		update.Content = &content
		payload := current.Payload
		payload.Property = models.MemoPayloadProperty{}
		update.Payload = &payload
	}
	if input.Tags != nil {
		nextTags := normalizeMemoTags(*input.Tags)
		if update.Payload != nil {
			update.Payload.Tags = nextTags
		} else {
			payload := current.Payload
			payload.Tags = nextTags
			update.Payload = &payload
		}
	}
	if input.Visibility != nil {
		if !input.Visibility.IsValid() {
			return MemoWithAttachments{}, fmt.Errorf("invalid visibility")
		}
		update.Visibility = input.Visibility
	}
	if input.State != nil {
		if !input.State.IsValid() {
			return MemoWithAttachments{}, fmt.Errorf("invalid state")
		}
		update.State = input.State
	}
	if input.Pinned != nil {
		update.Pinned = input.Pinned
	}
	if input.LatitudeSet || input.Latitude != nil {
		update.LatitudeSet = true
		update.Latitude = input.Latitude
	}
	if input.LongitudeSet || input.Longitude != nil {
		update.LongitudeSet = true
		update.Longitude = input.Longitude
	}

	var attachmentIDs *[]int64
	if input.AttachmentNames != nil {
		ids, err := s.resolveAttachmentIDsForMemoUpdate(
			ctx,
			updaterID,
			current.CreatorID,
			current.ID,
			*input.AttachmentNames,
		)
		if err != nil {
			return MemoWithAttachments{}, err
		}
		attachmentIDs = &ids
	}

	updatedMemo, err := s.store.UpdateMemoWithAttachments(ctx, memoID, update, attachmentIDs)
	if err != nil {
		return MemoWithAttachments{}, err
	}

	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memoID})
	if err != nil {
		return MemoWithAttachments{}, err
	}

	result := MemoWithAttachments{
		Memo:        updatedMemo,
		Attachments: attachmentsMap[memoID],
	}
	return result, nil
}

func (s *MemoService) DeleteMemo(ctx context.Context, requesterID int64, memoID int64) error {
	memo, err := s.store.GetMemoByID(ctx, memoID)
	if err != nil {
		return err
	}
	if !canManageMemo(memo, requesterID) {
		return sql.ErrNoRows
	}
	return s.store.DeleteMemo(ctx, memoID)
}
