package service

import (
	"context"
	"time"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func (s *MemoService) ListMemoChanges(
	ctx context.Context,
	viewerID int64,
	state *models.MemoState,
	rawFilter string,
	since time.Time,
	syncAnchor time.Time,
) (MemoChanges, error) {
	filter, prefilter, err := compileMemoFilter(rawFilter)
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

	// Incremental sync must return a complete window to avoid advancing
	// the client anchor past unseen changes.
	const noQueryLimit = 0
	filtered, err := s.listFilteredVisibleMemos(
		ctx,
		viewerID,
		state,
		filter,
		prefilter,
		noQueryLimit,
		&store.MemoQueryBounds{
			UpdatedAfter:         &normalizedSince,
			UpdatedBeforeOrEqual: &normalizedAnchor,
		},
	)
	if err != nil {
		return MemoChanges{}, err
	}

	changedMemos, err := s.attachMemos(ctx, filtered)
	if err != nil {
		return MemoChanges{}, err
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
