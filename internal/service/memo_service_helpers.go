package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func compileMemoFilter(rawFilter string) (*CELMemoFilter, store.MemoSQLPrefilter, error) {
	if containsContentDrivenFilter(rawFilter) {
		return nil, store.EmptyMemoPrefilter(), fmt.Errorf("content-based filter is disabled")
	}

	filter, err := CompileMemoFilter(rawFilter)
	if err != nil {
		return nil, store.EmptyMemoPrefilter(), err
	}

	prefilter := store.EmptyMemoPrefilter()
	if filter != nil {
		prefilter = filter.SQLPrefilter()
	}
	return filter, prefilter, nil
}

func (s *MemoService) attachMemos(ctx context.Context, viewerID int64, memos []models.Memo) ([]MemoWithAttachments, error) {
	if len(memos) == 0 {
		return []MemoWithAttachments{}, nil
	}

	memoIDs := make([]int64, 0, len(memos))
	for _, memo := range memos {
		memoIDs = append(memoIDs, memo.ID)
	}

	attachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, memoIDs)
	if err != nil {
		return nil, err
	}

	quoteDescriptorsByMemoID := make(map[int64]MemoQuoteDescriptor)
	referencedMemoIDs := make([]int64, 0)
	referencedMemoIDSet := make(map[int64]struct{})
	for _, memo := range memos {
		descriptor, ok := parseMemoQuoteDescriptor(memo.Payload.Tags)
		if !ok {
			continue
		}
		quoteDescriptorsByMemoID[memo.ID] = descriptor
		referencedMemoID, resolvable := parseMemoIDFromQuoteSource(descriptor.Source)
		if !resolvable {
			continue
		}
		if _, exists := referencedMemoIDSet[referencedMemoID]; exists {
			continue
		}
		referencedMemoIDSet[referencedMemoID] = struct{}{}
		referencedMemoIDs = append(referencedMemoIDs, referencedMemoID)
	}

	referencedMemoByID := make(map[int64]MemoWithAttachments)
	if len(referencedMemoIDs) > 0 {
		referencedMemos, err := s.store.ListVisibleMemosByIDs(ctx, viewerID, referencedMemoIDs)
		if err != nil {
			return nil, err
		}
		referencedAttachmentsMap, err := s.store.ListAttachmentsByMemoIDs(ctx, referencedMemoIDs)
		if err != nil {
			return nil, err
		}
		for _, referenced := range referencedMemos {
			referencedMemoByID[referenced.ID] = MemoWithAttachments{
				Memo:        referenced,
				Attachments: referencedAttachmentsMap[referenced.ID],
			}
		}
	}

	out := make([]MemoWithAttachments, 0, len(memos))
	for _, memo := range memos {
		item := MemoWithAttachments{
			Memo:        memo,
			Attachments: attachmentsMap[memo.ID],
		}
		if descriptor, ok := quoteDescriptorsByMemoID[memo.ID]; ok {
			quote := &MemoQuote{
				SourceKind: descriptor.SourceKind,
				Source:     descriptor.Source,
			}
			if referencedMemoID, resolvable := parseMemoIDFromQuoteSource(descriptor.Source); resolvable {
				if referenced, exists := referencedMemoByID[referencedMemoID]; exists {
					quote.Memo = &MemoQuoteMemo{
						Memo:        referenced.Memo,
						Attachments: referenced.Attachments,
					}
				}
			}
			item.Quote = quote
		}
		out = append(out, item)
	}
	return out, nil
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
	return store.NormalizeTagNames(tags)
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
