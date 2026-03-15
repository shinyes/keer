package service

import (
	"context"
	"fmt"

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

	rawOffset, err := parsePageToken(pageToken)
	if err != nil {
		return nil, "", fmt.Errorf("invalid pageToken")
	}
	pageSize = normalizeMemoPageSize(pageSize)

	page, nextToken, err := s.listFilteredVisibleMemoPage(
		ctx,
		viewerID,
		state,
		filter,
		prefilter,
		pageSize,
		rawOffset,
	)
	if err != nil {
		return nil, "", err
	}

	out, err := s.attachMemos(ctx, viewerID, page)
	if err != nil {
		return nil, "", err
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

func (s *MemoService) listFilteredVisibleMemoPage(
	ctx context.Context,
	viewerID int64,
	state *models.MemoState,
	filter *CELMemoFilter,
	prefilter store.MemoSQLPrefilter,
	pageSize int,
	rawOffset int,
) ([]models.Memo, string, error) {
	const (
		minScanBatchSize = 100
		maxScanBatchSize = 1000
	)

	scanBatchSize := pageSize * 4
	if scanBatchSize < minScanBatchSize {
		scanBatchSize = minScanBatchSize
	}
	if scanBatchSize > maxScanBatchSize {
		scanBatchSize = maxScanBatchSize
	}
	if scanBatchSize < pageSize+1 {
		scanBatchSize = pageSize + 1
	}

	collected := make([]models.Memo, 0, pageSize)
	currentOffset := rawOffset
	lastReturnedOffset := rawOffset

	for {
		batch, err := s.store.ListVisibleMemos(
			ctx,
			viewerID,
			state,
			prefilter,
			scanBatchSize,
			currentOffset,
			nil,
		)
		if err != nil {
			return nil, "", err
		}
		if len(batch) == 0 {
			return collected, "", nil
		}

		for index, memo := range batch {
			matched, err := filter.Matches(memo)
			if err != nil {
				return nil, "", err
			}
			if !matched {
				continue
			}

			rawPositionAfterRow := currentOffset + index + 1
			if len(collected) == pageSize {
				return collected, encodePageToken(lastReturnedOffset), nil
			}
			collected = append(collected, memo)
			lastReturnedOffset = rawPositionAfterRow
		}

		if len(batch) < scanBatchSize {
			return collected, "", nil
		}
		currentOffset += len(batch)
	}
}

func normalizeMemoPageSize(pageSize int) int {
	if pageSize <= 0 {
		return 50
	}
	if pageSize > 200 {
		return 200
	}
	return pageSize
}
