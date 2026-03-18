package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

func registerSyncRoutes(
	api fiber.Router,
	sqlStore *store.SQLStore,
	userService *service.UserService,
	memoService *service.MemoService,
	groupService *service.GroupService,
	buildAPIMemo func(service.MemoWithAttachments) apiMemo,
) {
	api.Post("/sync/pull", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)

		var req syncPullRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		domains := normalizeSyncDomains(req.Domains)
		domainSet := make(map[models.SyncDomain]struct{}, len(domains))
		for _, domain := range domains {
			domainSet[domain] = struct{}{}
		}

		cursor := int64(0)
		if rawCursor := strings.TrimSpace(req.Cursor); rawCursor != "" {
			parsed, err := parseNonNegativeInt64(rawCursor)
			if err != nil {
				return badRequest(c, "invalid cursor")
			}
			cursor = parsed
		}

		limit := req.Limit
		if limit <= 0 {
			limit = 200
		}
		if limit > 1000 {
			limit = 1000
		}

		groupScopeIDs, err := parseSyncGroupScopeIDs(req.GroupScopes)
		if err != nil {
			return badRequest(c, "invalid groupScopes")
		}

		response := syncPullResponse{
			NextCursor: models.Int64ToString(cursor),
			HasMore:    false,
			Patches: syncPullPatches{
				Memos: syncPullMemoPatch{
					Upserts: []apiMemo{},
					Deletes: []string{},
				},
				Users: syncPullUserPatch{
					Upserts: []apiUser{},
				},
				Groups: syncPullGroupPatch{
					Upserts: []apiGroup{},
					Deletes: []string{},
				},
				GroupMessages: syncPullGroupMessagesPatch{
					Groups: []syncPullGroupMessagesGroupPatch{},
				},
				Settings: syncPullSettingsPatch{},
			},
		}

		events, err := sqlStore.ListSyncEvents(c.Context(), cursor, domains, limit)
		if err != nil {
			return internalError(c, err)
		}

		nextCursor := cursor
		for _, event := range events {
			if event.ID > nextCursor {
				nextCursor = event.ID
			}
		}
		response.NextCursor = models.Int64ToString(nextCursor)
		hasMore, err := sqlStore.HasSyncEventsAfter(c.Context(), nextCursor, domains)
		if err != nil {
			return internalError(c, err)
		}
		response.HasMore = hasMore

		memoUpsertIDs := map[int64]struct{}{}
		memoDeleteIDs := map[int64]struct{}{}
		userIDs := map[int64]struct{}{currentUser.ID: {}}
		groupUpsertIDs := map[int64]struct{}{}
		groupDeleteIDs := map[int64]struct{}{}
		groupMessageUpsertIDsByGroup := map[int64]map[int64]struct{}{}
		groupMessageDeleteIDsByGroup := map[int64]map[int64]struct{}{}
		groupMemberCache := map[int64]bool{}
		settingsChanged := cursor == 0

		for _, event := range events {
			switch event.Domain {
			case models.SyncDomainMemos:
				if event.MemoID <= 0 {
					continue
				}
				if event.Action == models.SyncActionDelete {
					memoDeleteIDs[event.MemoID] = struct{}{}
				} else {
					memoUpsertIDs[event.MemoID] = struct{}{}
				}
			case models.SyncDomainUsers:
				if event.TargetUserID > 0 {
					userIDs[event.TargetUserID] = struct{}{}
				}
				if event.ActorUserID > 0 {
					userIDs[event.ActorUserID] = struct{}{}
				}
			case models.SyncDomainGroups:
				if event.GroupID <= 0 {
					continue
				}
				if len(groupScopeIDs) > 0 {
					if _, exists := groupScopeIDs[event.GroupID]; !exists {
						continue
					}
				}
				if event.Action == models.SyncActionDelete {
					if syncGroupDeleteRelevant(currentUser.ID, event) {
						groupDeleteIDs[event.GroupID] = struct{}{}
					}
					continue
				}
				visible, err := syncGroupVisible(
					c.Context(),
					sqlStore,
					currentUser.ID,
					event.GroupID,
					groupMemberCache,
					groupScopeIDs,
				)
				if err != nil {
					return internalError(c, err)
				}
				if visible {
					groupUpsertIDs[event.GroupID] = struct{}{}
				}
			case models.SyncDomainGroupMessages:
				if event.GroupID <= 0 {
					continue
				}
				visible, err := syncGroupVisible(
					c.Context(),
					sqlStore,
					currentUser.ID,
					event.GroupID,
					groupMemberCache,
					groupScopeIDs,
				)
				if err != nil {
					return internalError(c, err)
				}
				if !visible {
					continue
				}
				groupUpsertIDs[event.GroupID] = struct{}{}
				if event.GroupID > 0 && event.GroupMessageID > 0 {
					if event.Action == models.SyncActionDelete {
						addGroupedMessageID(groupMessageDeleteIDsByGroup, event.GroupID, event.GroupMessageID)
					} else {
						addGroupedMessageID(groupMessageUpsertIDsByGroup, event.GroupID, event.GroupMessageID)
					}
				}
			case models.SyncDomainSettings:
				if event.TargetUserID == 0 || event.TargetUserID == currentUser.ID {
					settingsChanged = true
				}
			}
		}

		if _, requested := domainSet[models.SyncDomainMemos]; requested {
			upsertIDs := sortedInt64Keys(memoUpsertIDs)
			if len(upsertIDs) > 0 {
				items, err := memoService.ListVisibleMemosByIDs(c.Context(), currentUser.ID, upsertIDs)
				if err != nil {
					return internalError(c, err)
				}
				visibleIDs := make(map[int64]struct{}, len(items))
				for _, item := range items {
					visibleIDs[item.Memo.ID] = struct{}{}
					response.Patches.Memos.Upserts = append(response.Patches.Memos.Upserts, buildAPIMemo(item))
				}
				for _, memoID := range upsertIDs {
					if _, visible := visibleIDs[memoID]; !visible {
						memoDeleteIDs[memoID] = struct{}{}
					}
				}
			}
			for _, memoID := range sortedInt64Keys(memoDeleteIDs) {
				response.Patches.Memos.Deletes = append(response.Patches.Memos.Deletes, fmt.Sprintf("memos/%d", memoID))
			}
		}

		if _, requested := domainSet[models.SyncDomainUsers]; requested {
			for _, userID := range sortedInt64Keys(userIDs) {
				user, err := userService.GetUser(c.Context(), userID)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					return internalError(c, err)
				}
				response.Patches.Users.Upserts = append(response.Patches.Users.Upserts, toAPIUserSync(user))
			}
		}

		groupByID := map[int64]service.GroupWithMembers{}
		needGroupMeta := len(groupUpsertIDs) > 0 || len(groupMessageUpsertIDsByGroup) > 0 || len(groupMessageDeleteIDsByGroup) > 0
		if needGroupMeta {
			groups, err := groupService.ListGroups(c.Context(), currentUser.ID)
			if err != nil {
				return internalError(c, err)
			}
			for _, group := range groups {
				groupByID[group.Group.ID] = group
			}
		}

		if _, requested := domainSet[models.SyncDomainGroups]; requested {
			for _, groupID := range sortedInt64Keys(groupUpsertIDs) {
				group, exists := groupByID[groupID]
				if !exists {
					groupDeleteIDs[groupID] = struct{}{}
					continue
				}
				response.Patches.Groups.Upserts = append(response.Patches.Groups.Upserts, toAPIGroup(group))
			}
			for _, groupID := range sortedInt64Keys(groupDeleteIDs) {
				response.Patches.Groups.Deletes = append(response.Patches.Groups.Deletes, fmt.Sprintf("groups/%d", groupID))
			}
		}

		if _, requested := domainSet[models.SyncDomainGroupMessages]; requested {
			groupsToPull := groupedMessageGroupIDs(groupMessageUpsertIDsByGroup, groupMessageDeleteIDsByGroup)
			for _, groupID := range groupsToPull {
				if groupID <= 0 {
					continue
				}
				groupMeta, exists := groupByID[groupID]
				if !exists {
					continue
				}

				upserts := make([]apiGroupMessage, 0, 16)
				upsertIDs := groupedMessageIDs(groupMessageUpsertIDsByGroup, groupID)
				if len(upsertIDs) > 0 {
					items, err := groupService.ListGroupMessagesByIDs(
						c.Context(),
						currentUser.ID,
						groupID,
						upsertIDs,
					)
					if err != nil {
						return internalError(c, err)
					}
					for _, item := range items {
						upserts = append(upserts, toAPIGroupMessage(item))
					}
				}

				deleteIDSet := map[int64]struct{}{}
				for _, messageID := range groupedMessageIDs(groupMessageDeleteIDsByGroup, groupID) {
					if messageID > 0 {
						deleteIDSet[messageID] = struct{}{}
					}
				}
				deletes := make([]string, 0, len(deleteIDSet))
				for messageID := range deleteIDSet {
					deletes = append(
						deletes,
						fmt.Sprintf("groups/%d/messages/%d", groupID, messageID),
					)
				}
				sort.Strings(deletes)

				tags, err := groupService.ListGroupTags(c.Context(), currentUser.ID, groupID)
				if err != nil {
					if !errors.Is(err, sql.ErrNoRows) {
						return internalError(c, err)
					}
					tags = []string{}
				}

				response.Patches.GroupMessages.Groups = append(
					response.Patches.GroupMessages.Groups,
					syncPullGroupMessagesGroupPatch{
						Group:     groupMeta.Group.Name(),
						HasUnread: groupMeta.Group.HasUnread,
						Upserts:   upserts,
						Deletes:   deletes,
						Tags:      tags,
					},
				)
			}
		}

		if _, requested := domainSet[models.SyncDomainSettings]; requested && settingsChanged {
			settings, err := userService.GetUserGeneralSettings(c.Context(), currentUser.ID)
			if err != nil {
				return internalError(c, err)
			}
			apiSettings := toAPIGeneralSetting(settings)
			response.Patches.Settings.GeneralSetting = &apiSettings
		}

		return c.JSON(response)
	})
}

func normalizeSyncDomains(raw []string) []models.SyncDomain {
	if len(raw) == 0 {
		return []models.SyncDomain{
			models.SyncDomainMemos,
			models.SyncDomainUsers,
			models.SyncDomainGroups,
			models.SyncDomainGroupMessages,
			models.SyncDomainSettings,
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
			models.SyncDomainGroups,
			models.SyncDomainGroupMessages,
			models.SyncDomainSettings,
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
