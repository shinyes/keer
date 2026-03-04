package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func (s *MemoService) ListMemos(ctx context.Context, viewerID int64, state *models.MemoState, rawFilter string, pageSize int, pageToken string) ([]MemoWithAttachments, string, error) {
	filter, prefilter, err := compileMemoFilter(rawFilter)
	if err != nil {
		return nil, "", err
	}

	if state == nil {
		defaultState := models.MemoStateNormal
		state = &defaultState
	}

	// 设置安全上限，避免一次性加载过多 memo 到内存
	const maxMemoQueryLimit = 10000
	filtered, err := s.listFilteredVisibleMemos(
		ctx,
		viewerID,
		state,
		filter,
		prefilter,
		maxMemoQueryLimit,
		nil,
	)
	if err != nil {
		return nil, "", err
	}

	page, nextToken, err := paginateMemos(filtered, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	out, err := s.attachMemos(ctx, page)
	if err != nil {
		return nil, "", err
	}
	_ = viewerID
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

func (s *MemoService) listFilteredVisibleMemos(
	ctx context.Context,
	viewerID int64,
	state *models.MemoState,
	filter *CELMemoFilter,
	prefilter store.MemoSQLPrefilter,
	limit int,
	bounds *store.MemoQueryBounds,
) ([]models.Memo, error) {
	allVisible, err := s.store.ListVisibleMemos(
		ctx,
		viewerID,
		state,
		prefilter,
		limit,
		0,
		bounds,
	)
	if err != nil {
		return nil, err
	}

	filtered := make([]models.Memo, 0, len(allVisible))
	for _, memo := range allVisible {
		matched, err := filter.Matches(memo)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		filtered = append(filtered, memo)
	}
	return filtered, nil
}

func paginateMemos(memos []models.Memo, pageSize int, pageToken string) ([]models.Memo, string, error) {
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

	if offset >= len(memos) {
		return []models.Memo{}, "", nil
	}
	end := min(offset+pageSize, len(memos))
	nextToken := ""
	if end < len(memos) {
		nextToken = strconv.Itoa(end)
	}
	return memos[offset:end], nextToken, nil
}
