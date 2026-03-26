package http

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func normalizeSyncDomains(raw []string) []models.SyncDomain {
	if len(raw) == 0 {
		return []models.SyncDomain{
			models.SyncDomainMemos,
			models.SyncDomainUsers,
			models.SyncDomainFriendships,
			models.SyncDomainGroups,
			models.SyncDomainGroupMessages,
			models.SyncDomainAttachments,
			models.SyncDomainSettings,
			models.SyncDomainSettingsE2EE,
			models.SyncDomainGroupKeys,
		}
	}

	seen := map[models.SyncDomain]struct{}{}
	result := make([]models.SyncDomain, 0, len(raw))
	for _, item := range raw {
		domain := models.SyncDomain(strings.ToUpper(strings.TrimSpace(item)))
		if !domain.IsValid() {
			continue
		}
		if _, exists := seen[domain]; exists {
			continue
		}
		seen[domain] = struct{}{}
		result = append(result, domain)
	}
	if len(result) == 0 {
		return []models.SyncDomain{
			models.SyncDomainMemos,
			models.SyncDomainUsers,
			models.SyncDomainFriendships,
			models.SyncDomainGroups,
			models.SyncDomainGroupMessages,
			models.SyncDomainAttachments,
			models.SyncDomainSettings,
			models.SyncDomainSettingsE2EE,
			models.SyncDomainGroupKeys,
		}
	}
	return result
}

func parseSyncGroupScopeIDs(raw []string) (map[int64]struct{}, error) {
	ids := map[int64]struct{}{}
	for _, item := range raw {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "groups/") {
			value = strings.TrimPrefix(value, "groups/")
		}
		groupID, err := parseID(value)
		if err != nil {
			return nil, err
		}
		if groupID <= 0 {
			return nil, fmt.Errorf("group id must be positive")
		}
		ids[groupID] = struct{}{}
	}
	return ids, nil
}

func syncGroupVisible(
	ctx context.Context,
	sqlStore *store.SQLStore,
	userID int64,
	groupID int64,
	cache map[int64]bool,
	groupScopes map[int64]struct{},
) (bool, error) {
	if groupID <= 0 {
		return false, nil
	}
	if len(groupScopes) > 0 {
		if _, exists := groupScopes[groupID]; !exists {
			return false, nil
		}
	}
	if cached, exists := cache[groupID]; exists {
		return cached, nil
	}
	member, err := sqlStore.IsGroupMember(ctx, groupID, userID)
	if err != nil {
		return false, err
	}
	cache[groupID] = member
	return member, nil
}

func syncGroupDeleteRelevant(currentUserID int64, event models.SyncEvent) bool {
	if event.GroupID <= 0 {
		return false
	}
	if event.TargetUserID == currentUserID {
		return true
	}
	if event.ActorUserID == currentUserID {
		return true
	}
	return event.TargetUserID == 0
}

func sortedInt64Keys(set map[int64]struct{}) []int64 {
	if len(set) == 0 {
		return []int64{}
	}
	result := make([]int64, 0, len(set))
	for key := range set {
		if key > 0 {
			result = append(result, key)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func addGroupedMessageID(target map[int64]map[int64]struct{}, groupID int64, messageID int64) {
	if groupID <= 0 || messageID <= 0 {
		return
	}
	groupSet, exists := target[groupID]
	if !exists {
		groupSet = map[int64]struct{}{}
		target[groupID] = groupSet
	}
	groupSet[messageID] = struct{}{}
}

func groupedMessageIDs(source map[int64]map[int64]struct{}, groupID int64) []int64 {
	groupSet, exists := source[groupID]
	if !exists || len(groupSet) == 0 {
		return []int64{}
	}
	ids := make([]int64, 0, len(groupSet))
	for messageID := range groupSet {
		if messageID > 0 {
			ids = append(ids, messageID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func groupedMessageGroupIDs(
	upserts map[int64]map[int64]struct{},
	deletes map[int64]map[int64]struct{},
) []int64 {
	merged := map[int64]struct{}{}
	for groupID := range upserts {
		if groupID > 0 {
			merged[groupID] = struct{}{}
		}
	}
	for groupID := range deletes {
		if groupID > 0 {
			merged[groupID] = struct{}{}
		}
	}
	return sortedInt64Keys(merged)
}
