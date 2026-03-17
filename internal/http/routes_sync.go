package http

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

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
					Directory: []apiGroup{},
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

		isInitialPull := cursor == 0
		memoChanged := isInitialPull
		userChanged := isInitialPull
		groupChanged := isInitialPull
		groupMessageChanged := isInitialPull
		settingsChanged := isInitialPull

		memoSince := time.Unix(0, 0).UTC()
		userSince := time.Unix(0, 0).UTC()
		var memoSinceSet bool
		var userSinceSet bool
		userIDs := map[int64]struct{}{currentUser.ID: {}}
		groupIDs := map[int64]struct{}{}
		groupMemberCache := map[int64]bool{}

		for _, event := range events {
			switch event.Domain {
			case models.SyncDomainMemos:
				memoChanged = true
				if !memoSinceSet || event.EventTime.Before(memoSince) {
					memoSince = event.EventTime
					memoSinceSet = true
				}
			case models.SyncDomainUsers:
				userChanged = true
				if !userSinceSet || event.EventTime.Before(userSince) {
					userSince = event.EventTime
					userSinceSet = true
				}
				if event.TargetUserID > 0 {
					userIDs[event.TargetUserID] = struct{}{}
				}
				if event.ActorUserID > 0 {
					userIDs[event.ActorUserID] = struct{}{}
				}
			case models.SyncDomainGroups:
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
				groupChanged = true
				if event.GroupID > 0 {
					groupIDs[event.GroupID] = struct{}{}
				}
			case models.SyncDomainGroupMessages:
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
				groupMessageChanged = true
				if event.GroupID > 0 {
					groupIDs[event.GroupID] = struct{}{}
				}
			case models.SyncDomainSettings:
				if event.TargetUserID == 0 || event.TargetUserID == currentUser.ID {
					settingsChanged = true
				}
			}
		}

		if _, requested := domainSet[models.SyncDomainMemos]; requested && memoChanged {
			if !memoSinceSet {
				memoSince = time.Unix(0, 0).UTC()
			}
			changes, err := memoService.ListMemoChanges(
				c.Context(),
				currentUser.ID,
				nil,
				"",
				memoSince,
				time.Now().UTC(),
			)
			if err != nil {
				return internalError(c, err)
			}
			for _, item := range changes.Memos {
				response.Patches.Memos.Upserts = append(response.Patches.Memos.Upserts, buildAPIMemo(item))
			}
			response.Patches.Memos.Deletes = append(response.Patches.Memos.Deletes, changes.DeletedMemoNames...)
		}

		if _, requested := domainSet[models.SyncDomainUsers]; requested && userChanged {
			if !userSinceSet {
				userSince = time.Unix(0, 0).UTC()
			}
			identifiers := make([]string, 0, len(userIDs))
			for id := range userIDs {
				if id <= 0 {
					continue
				}
				identifiers = append(identifiers, models.Int64ToString(id))
			}
			sort.Strings(identifiers)
			if len(identifiers) > 200 {
				identifiers = identifiers[:200]
			}
			changes, err := userService.ListUserChanges(
				c.Context(),
				identifiers,
				userSince,
				time.Now().UTC(),
			)
			if err != nil {
				return internalError(c, err)
			}
			for _, user := range changes.Users {
				response.Patches.Users.Upserts = append(response.Patches.Users.Upserts, toAPIUserSync(user))
			}
		}

		groupByID := make(map[int64]service.GroupWithMembers)
		if _, requested := domainSet[models.SyncDomainGroups]; requested && groupChanged {
			groups, err := groupService.ListGroups(c.Context(), currentUser.ID)
			if err != nil {
				return internalError(c, err)
			}
			for _, group := range groups {
				groupByID[group.Group.ID] = group
				response.Patches.Groups.Directory = append(response.Patches.Groups.Directory, toAPIGroup(group))
			}
		}

		if _, requested := domainSet[models.SyncDomainGroupMessages]; requested && groupMessageChanged {
			if len(groupByID) == 0 {
				groups, err := groupService.ListGroups(c.Context(), currentUser.ID)
				if err != nil {
					return internalError(c, err)
				}
				for _, group := range groups {
					groupByID[group.Group.ID] = group
				}
			}

			groupsToPull := make([]int64, 0, len(groupIDs))
			if len(groupScopeIDs) > 0 {
				for groupID := range groupScopeIDs {
					groupsToPull = append(groupsToPull, groupID)
				}
			} else if isInitialPull {
				for groupID := range groupByID {
					groupsToPull = append(groupsToPull, groupID)
				}
			} else {
				for groupID := range groupIDs {
					groupsToPull = append(groupsToPull, groupID)
				}
			}
			sort.Slice(groupsToPull, func(i, j int) bool { return groupsToPull[i] < groupsToPull[j] })

			for _, groupID := range groupsToPull {
				if groupID <= 0 {
					continue
				}
				groupMeta, exists := groupByID[groupID]
				if !exists {
					continue
				}

				messages := make([]apiGroupMessage, 0, 64)
				pageToken := ""
				for {
					items, nextToken, err := groupService.ListGroupMessages(
						c.Context(),
						currentUser.ID,
						groupID,
						200,
						pageToken,
					)
					if err != nil {
						return internalError(c, err)
					}
					for _, item := range items {
						messages = append(messages, toAPIGroupMessage(item))
					}
					if strings.TrimSpace(nextToken) == "" {
						break
					}
					pageToken = nextToken
				}

				tags, err := groupService.ListGroupTags(c.Context(), currentUser.ID, groupID)
				if err != nil {
					return internalError(c, err)
				}

				response.Patches.GroupMessages.Groups = append(
					response.Patches.GroupMessages.Groups,
					syncPullGroupMessagesGroupPatch{
						Group:       groupMeta.Group.Name(),
						FullReplace: true,
						HasUnread:   groupMeta.Group.HasUnread,
						Messages:    messages,
						Tags:        tags,
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
