package service

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/markdown"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type MemoService struct {
	store    *store.SQLStore
	markdown *markdown.Service
}

func NewMemoService(s *store.SQLStore, markdownSvc *markdown.Service) *MemoService {
	return &MemoService{
		store:    s,
		markdown: markdownSvc,
	}
}

type CreateMemoInput struct {
	Content         string
	Visibility      models.Visibility
	Tags            []string
	AttachmentNames []string
	DisplayTime     *time.Time // 客户端指定的创建时间，为 nil 时使用当前时间
}

type UpdateMemoInput struct {
	Content         *string
	Visibility      *models.Visibility
	Tags            *[]string
	State           *models.MemoState
	Pinned          *bool
	AttachmentNames *[]string
}

type MemoWithAttachments struct {
	Memo        models.Memo
	Attachments []models.Attachment
}

func (s *MemoService) CreateMemo(ctx context.Context, creatorID int64, input CreateMemoInput) (MemoWithAttachments, error) {
	content := input.Content
	visibility := input.Visibility
	if !visibility.IsValid() {
		visibility = models.VisibilityPrivate
	}

	payload, err := s.markdown.ExtractPayload(content)
	if err != nil {
		return MemoWithAttachments{}, err
	}
	payload.Tags = normalizeMemoTags(input.Tags)

	attachmentIDs, err := s.resolveAttachmentIDsFromNames(ctx, creatorID, input.AttachmentNames)
	if err != nil {
		return MemoWithAttachments{}, err
	}

	displayTime := time.Now().UTC()
	if input.DisplayTime != nil && !input.DisplayTime.IsZero() {
		displayTime = input.DisplayTime.UTC()
	}

	memo, err := s.store.CreateMemoWithAttachments(
		ctx,
		creatorID,
		content,
		visibility,
		models.MemoStateNormal,
		false,
		payload,
		displayTime,
		attachmentIDs,
	)
	if err != nil {
		return MemoWithAttachments{}, err
	}
	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memo.ID})
	if err != nil {
		return MemoWithAttachments{}, err
	}

	return MemoWithAttachments{
		Memo:        memo,
		Attachments: attachmentsMap[memo.ID],
	}, nil
}

func (s *MemoService) UpdateMemo(ctx context.Context, updaterID int64, memoID int64, input UpdateMemoInput) (MemoWithAttachments, error) {
	current, err := s.store.GetMemoByID(ctx, memoID)
	if err != nil {
		return MemoWithAttachments{}, err
	}
	if current.CreatorID != updaterID {
		return MemoWithAttachments{}, sql.ErrNoRows
	}

	update := store.MemoUpdate{}
	if input.Content != nil {
		content := *input.Content
		update.Content = &content
		payload, err := s.markdown.ExtractPayload(content)
		if err != nil {
			return MemoWithAttachments{}, err
		}
		payload.Tags = current.Payload.Tags
		update.Payload = &payload
		update.DisplayTime = ptrTime(time.Now().UTC())
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

	var attachmentIDs *[]int64
	if input.AttachmentNames != nil {
		ids, err := s.resolveAttachmentIDsFromNames(ctx, updaterID, *input.AttachmentNames)
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

	return MemoWithAttachments{
		Memo:        updatedMemo,
		Attachments: attachmentsMap[memoID],
	}, nil
}

func (s *MemoService) DeleteMemo(ctx context.Context, requesterID int64, memoID int64) error {
	memo, err := s.store.GetMemoByID(ctx, memoID)
	if err != nil {
		return err
	}
	if memo.CreatorID != requesterID {
		return sql.ErrNoRows
	}
	return s.store.DeleteMemo(ctx, memoID)
}

func (s *MemoService) ListMemos(ctx context.Context, viewerID int64, state *models.MemoState, rawFilter string, pageSize int, pageToken string) ([]MemoWithAttachments, string, error) {
	filter, err := CompileMemoFilter(rawFilter)
	if err != nil {
		return nil, "", err
	}

	if state == nil {
		defaultState := models.MemoStateNormal
		state = &defaultState
	}

	prefilter := store.EmptyMemoPrefilter()
	if filter != nil {
		prefilter = filter.SQLPrefilter()
	}

	// 设置安全上限，避免一次性加载过多 memo 到内存
	const maxMemoQueryLimit = 10000
	allVisible, err := s.store.ListVisibleMemos(ctx, viewerID, state, prefilter, maxMemoQueryLimit, 0)
	if err != nil {
		return nil, "", err
	}

	filtered := make([]models.Memo, 0, len(allVisible))
	for _, memo := range allVisible {
		matched, err := filter.Matches(memo)
		if err != nil {
			return nil, "", err
		}
		if !matched {
			continue
		}
		filtered = append(filtered, memo)
	}

	offset, err := parsePageToken(pageToken)
	if err != nil {
		return nil, "", fmt.Errorf("invalid pageToken")
	}
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 200 {
		pageSize = 200
	}

	if offset >= len(filtered) {
		return []MemoWithAttachments{}, "", nil
	}
	end := min(offset+pageSize, len(filtered))
	page := filtered[offset:end]
	nextToken := ""
	if end < len(filtered) {
		nextToken = strconv.Itoa(end)
	}

	memoIDs := make([]int64, 0, len(page))
	for _, memo := range page {
		memoIDs = append(memoIDs, memo.ID)
	}
	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, memoIDs)
	if err != nil {
		return nil, "", err
	}

	out := make([]MemoWithAttachments, 0, len(page))
	for _, memo := range page {
		out = append(out, MemoWithAttachments{
			Memo:        memo,
			Attachments: attachmentsMap[memo.ID],
		})
	}
	return out, nextToken, nil
}

func (s *MemoService) GetUserTagCount(ctx context.Context, requestedUserID int64, viewerID int64) (map[string]int, error) {
	memos, err := s.store.ListVisibleMemosByCreator(ctx, requestedUserID, viewerID, models.MemoStateNormal)
	if err != nil {
		return nil, err
	}

	tagCount := make(map[string]int)
	for _, memo := range memos {
		for _, tag := range memo.Payload.Tags {
			tagCount[tag]++
		}
	}
	return tagCount, nil
}

func (s *MemoService) RebuildAllMemoPayloads(ctx context.Context) (int, error) {
	memos, err := s.store.ListAllMemos(ctx)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, memo := range memos {
		payload, err := s.markdown.ExtractPayload(memo.Content)
		if err != nil {
			return updated, err
		}
		payload.Tags = memo.Payload.Tags
		if slices.Equal(payload.Tags, memo.Payload.Tags) &&
			payload.Property == memo.Payload.Property {
			continue
		}
		if err := s.store.UpdateMemoPayload(ctx, memo.ID, payload); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

func parsePageToken(pageToken string) (int, error) {
	pageToken = strings.TrimSpace(pageToken)
	if pageToken == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(pageToken)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid page token")
	}
	return offset, nil
}

func (s *MemoService) resolveAttachmentIDsFromNames(ctx context.Context, userID int64, names []string) ([]int64, error) {
	if len(names) == 0 {
		return []int64{}, nil
	}
	ids := make([]int64, 0, len(names))
	seen := make(map[int64]struct{})
	for _, name := range names {
		id, err := parseResourceID(name)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[id]; ok {
			continue
		}
		belongs, err := s.store.AttachmentBelongsToUser(ctx, id, userID)
		if err != nil {
			return nil, err
		}
		if !belongs {
			return nil, fmt.Errorf("attachment %d not found", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func parseResourceID(name string) (int64, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, fmt.Errorf("invalid attachment name")
	}
	name = strings.SplitN(name, "|", 2)[0]
	name = strings.Trim(name, "/")
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" {
		return 0, fmt.Errorf("invalid attachment name")
	}
	id, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid attachment id")
	}
	return id, nil
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func normalizeMemoTags(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	normalized := make([]string, 0, len(tags))
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
		normalized = append(normalized, tag)
	}
	return normalized
}
