package service

import "context"

// ListVisibleMemosByIDs returns viewer-visible memos for the provided IDs with
// attachments and quote metadata resolved.
func (s *MemoService) ListVisibleMemosByIDs(
	ctx context.Context,
	viewerID int64,
	memoIDs []int64,
) ([]MemoWithAttachments, error) {
	memos, err := s.store.ListVisibleMemosByIDs(ctx, viewerID, memoIDs)
	if err != nil {
		return nil, err
	}
	return s.attachMemos(ctx, viewerID, memos)
}
