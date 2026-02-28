package service

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

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
	if !canManageMemo(memo, requesterID) {
		return sql.ErrNoRows
	}
	return s.store.DeleteMemo(ctx, memoID)
}

func (s *MemoService) ListMemos(ctx context.Context, viewerID int64, state *models.MemoState, rawFilter string, pageSize int, pageToken string) ([]MemoWithAttachments, string, error) {
	if containsContentDrivenFilter(rawFilter) {
		return nil, "", fmt.Errorf("content-based filter is disabled")
	}

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
	allVisible, err := s.store.ListVisibleMemos(ctx, viewerID, state, prefilter, maxMemoQueryLimit, 0, nil)
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

func (s *MemoService) ListMemoChanges(
	ctx context.Context,
	viewerID int64,
	state *models.MemoState,
	rawFilter string,
	since time.Time,
	syncAnchor time.Time,
) (MemoChanges, error) {
	if containsContentDrivenFilter(rawFilter) {
		return MemoChanges{}, fmt.Errorf("content-based filter is disabled")
	}

	filter, err := CompileMemoFilter(rawFilter)
	if err != nil {
		return MemoChanges{}, err
	}

	normalizedSince := since.UTC()
	normalizedAnchor := syncAnchor.UTC()
	if normalizedAnchor.IsZero() {
		normalizedAnchor = time.Now().UTC()
	}
	if normalizedSince.After(normalizedAnchor) {
		normalizedSince = normalizedAnchor
	}

	prefilter := store.EmptyMemoPrefilter()
	if filter != nil {
		prefilter = filter.SQLPrefilter()
	}

	// Incremental sync must return a complete window to avoid advancing
	// the client anchor past unseen changes.
	const noQueryLimit = 0
	allVisible, err := s.store.ListVisibleMemos(
		ctx,
		viewerID,
		state,
		prefilter,
		noQueryLimit,
		0,
		&store.MemoQueryBounds{
			UpdatedAfter:         &normalizedSince,
			UpdatedBeforeOrEqual: &normalizedAnchor,
		},
	)
	if err != nil {
		return MemoChanges{}, err
	}

	filtered := make([]models.Memo, 0, len(allVisible))
	for _, memo := range allVisible {
		matched, err := filter.Matches(memo)
		if err != nil {
			return MemoChanges{}, err
		}
		if !matched {
			continue
		}
		filtered = append(filtered, memo)
	}

	memoIDs := make([]int64, 0, len(filtered))
	for _, memo := range filtered {
		memoIDs = append(memoIDs, memo.ID)
	}

	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, memoIDs)
	if err != nil {
		return MemoChanges{}, err
	}

	changedMemos := make([]MemoWithAttachments, 0, len(filtered))
	for _, memo := range filtered {
		changedMemos = append(changedMemos, MemoWithAttachments{
			Memo:        memo,
			Attachments: attachmentsMap[memo.ID],
		})
	}

	deletedMemoNames, err := s.store.ListDeletedVisibleMemoNames(
		ctx,
		viewerID,
		normalizedSince,
		normalizedAnchor,
		noQueryLimit,
	)
	if err != nil {
		return MemoChanges{}, err
	}

	return MemoChanges{
		Memos:            changedMemos,
		DeletedMemoNames: deletedMemoNames,
		SyncAnchor:       normalizedAnchor,
	}, nil
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

func containsContentDrivenFilter(rawFilter string) bool {
	trimmed := strings.TrimSpace(rawFilter)
	if trimmed == "" {
		return false
	}

	identifiers := extractFilterIdentifiers(trimmed)
	if len(identifiers) == 0 {
		return false
	}

	for _, ident := range identifiers {
		if ident == "content" || strings.HasPrefix(ident, "content.") {
			return true
		}
		if ident == "property" || strings.HasPrefix(ident, "property.") {
			return true
		}
		switch ident {
		case "has_link",
			"has_task_list",
			"has_code",
			"has_incomplete_tasks":
			return true
		}
	}
	return false
}

func extractFilterIdentifiers(filter string) []string {
	runes := []rune(filter)
	identifiers := make([]string, 0, 8)
	var quote rune
	escaped := false

	for i := 0; i < len(runes); {
		ch := runes[i]

		if quote != 0 {
			if escaped {
				escaped = false
				i++
				continue
			}
			if ch == '\\' {
				escaped = true
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			i++
			continue
		}

		if ch == '"' || ch == '\'' {
			quote = ch
			i++
			continue
		}

		if !isFilterIdentifierStart(ch) {
			i++
			continue
		}

		start := i
		i++
		for i < len(runes) {
			next := runes[i]
			if isFilterIdentifierPart(next) || next == '.' {
				i++
				continue
			}
			break
		}

		identifiers = append(identifiers, strings.ToLower(string(runes[start:i])))
	}

	return identifiers
}

func isFilterIdentifierStart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch)
}

func isFilterIdentifierPart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
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

func (s *MemoService) resolveAttachmentIDsForMemoUpdate(
	ctx context.Context,
	updaterID int64,
	memoCreatorID int64,
	memoID int64,
	names []string,
) ([]int64, error) {
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
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	existingMap, err := s.store.ListAttachmentsByMemoIDs(ctx, []int64{memoID})
	if err != nil {
		return nil, err
	}
	existingAttachmentIDs := make(map[int64]struct{}, len(existingMap[memoID]))
	for _, attachment := range existingMap[memoID] {
		existingAttachmentIDs[attachment.ID] = struct{}{}
	}

	for _, id := range ids {
		if _, alreadyAttached := existingAttachmentIDs[id]; alreadyAttached {
			continue
		}

		belongsToUpdater, err := s.store.AttachmentBelongsToUser(ctx, id, updaterID)
		if err != nil {
			return nil, err
		}
		if belongsToUpdater {
			continue
		}

		if memoCreatorID != updaterID {
			belongsToCreator, err := s.store.AttachmentBelongsToUser(ctx, id, memoCreatorID)
			if err != nil {
				return nil, err
			}
			if belongsToCreator {
				continue
			}
		}

		return nil, fmt.Errorf("attachment %d not found", id)
	}

	return ids, nil
}

func canManageMemo(memo models.Memo, userID int64) bool {
	if memo.CreatorID == userID {
		return true
	}
	collaboratorTag := "collab/" + strconv.FormatInt(userID, 10)
	for _, tag := range memo.Payload.Tags {
		if strings.TrimSpace(tag) == collaboratorTag {
			return true
		}
	}
	return false
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

func validateCoordinates(latitude *float64, longitude *float64) error {
	if latitude != nil && (*latitude < -90 || *latitude > 90) {
		return fmt.Errorf("invalid latitude")
	}
	if longitude != nil && (*longitude < -180 || *longitude > 180) {
		return fmt.Errorf("invalid longitude")
	}
	return nil
}
