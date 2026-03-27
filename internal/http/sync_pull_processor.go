package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

type syncPullProcessor struct {
	sqlStore     *store.SQLStore
	userService  *service.UserService
	memoService  *service.MemoService
	groupService *service.GroupService
	buildAPIMemo func(service.MemoWithAttachments) apiMemo
}

var (
	errInvalidSyncCursor      = errors.New("invalid cursor")
	errInvalidSyncGroupScopes = errors.New("invalid groupScopes")
)

func newSyncPullProcessor(
	sqlStore *store.SQLStore,
	userService *service.UserService,
	memoService *service.MemoService,
	groupService *service.GroupService,
	buildAPIMemo func(service.MemoWithAttachments) apiMemo,
) syncPullProcessor {
	return syncPullProcessor{
		sqlStore:     sqlStore,
		userService:  userService,
		memoService:  memoService,
		groupService: groupService,
		buildAPIMemo: buildAPIMemo,
	}
}

func (p syncPullProcessor) Compute(
	ctx context.Context,
	currentUser models.User,
	req syncPullRequest,
) (syncPullResponse, error) {
	domains := normalizeSyncDomains(req.Domains)
	domainSet := make(map[models.SyncDomain]struct{}, len(domains))
	for _, domain := range domains {
		domainSet[domain] = struct{}{}
	}

	cursor := int64(0)
	if rawCursor := strings.TrimSpace(req.Cursor); rawCursor != "" {
		parsed, err := parseNonNegativeInt64(rawCursor)
		if err != nil {
			return syncPullResponse{}, errInvalidSyncCursor
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
		return syncPullResponse{}, errInvalidSyncGroupScopes
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
			Friendships: syncPullFriendshipsPatch{
				Upserts: []apiUser{},
				Deletes: []string{},
			},
			Groups: syncPullGroupPatch{
				Upserts: []apiGroup{},
				Deletes: []string{},
			},
			GroupMessages: syncPullGroupMessagesPatch{
				Groups: []syncPullGroupMessagesGroupPatch{},
			},
			Attachments: syncPullAttachmentsPatch{
				Upserts: []apiAttachment{},
				Deletes: []string{},
			},
			Settings: syncPullSettingsPatch{},
			GroupKeys: syncPullGroupKeysPatch{
				Upserts: []apiGroupKeyVersion{},
				Deletes: []string{},
			},
		},
	}

	events, err := p.sqlStore.ListSyncEvents(ctx, cursor, domains, limit)
	if err != nil {
		return syncPullResponse{}, err
	}

	nextCursor := cursor
	for _, event := range events {
		if event.ID > nextCursor {
			nextCursor = event.ID
		}
	}
	response.NextCursor = models.Int64ToString(nextCursor)
	hasMore, err := p.sqlStore.HasSyncEventsAfter(ctx, nextCursor, domains)
	if err != nil {
		return syncPullResponse{}, err
	}
	response.HasMore = hasMore

	memoUpsertIDs := map[int64]struct{}{}
	memoDeleteIDs := map[int64]struct{}{}
	userIDs := map[int64]struct{}{currentUser.ID: {}}
	friendshipUpsertIDs := map[int64]struct{}{}
	friendshipDeleteIDs := map[int64]struct{}{}
	groupUpsertIDs := map[int64]struct{}{}
	groupDeleteIDs := map[int64]struct{}{}
	groupMessageUpsertIDsByGroup := map[int64]map[int64]struct{}{}
	groupMessageDeleteIDsByGroup := map[int64]map[int64]struct{}{}
	attachmentUpsertIDs := map[int64]struct{}{}
	attachmentDeleteIDs := map[int64]struct{}{}
	groupKeyUpsertGroupIDs := map[int64]struct{}{}
	groupKeyDeleteNames := map[string]struct{}{}
	groupMemberCache := map[int64]bool{}
	settingsChanged := cursor == 0
	encryptionSettingsChanged := cursor == 0

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
		case models.SyncDomainFriendships:
			friendID := int64(0)
			if event.ActorUserID == currentUser.ID && event.TargetUserID > 0 {
				friendID = event.TargetUserID
			} else if event.TargetUserID == currentUser.ID && event.ActorUserID > 0 {
				friendID = event.ActorUserID
			}
			if friendID <= 0 || friendID == currentUser.ID {
				continue
			}
			if event.Action == models.SyncActionDelete {
				friendshipDeleteIDs[friendID] = struct{}{}
			} else {
				friendshipUpsertIDs[friendID] = struct{}{}
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
				ctx,
				p.sqlStore,
				currentUser.ID,
				event.GroupID,
				groupMemberCache,
				groupScopeIDs,
			)
			if err != nil {
				return syncPullResponse{}, err
			}
			if visible {
				groupUpsertIDs[event.GroupID] = struct{}{}
			}
		case models.SyncDomainGroupMessages:
			if event.GroupID <= 0 {
				continue
			}
			visible, err := syncGroupVisible(
				ctx,
				p.sqlStore,
				currentUser.ID,
				event.GroupID,
				groupMemberCache,
				groupScopeIDs,
			)
			if err != nil {
				return syncPullResponse{}, err
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
		case models.SyncDomainSettingsE2EE:
			if event.TargetUserID == 0 || event.TargetUserID == currentUser.ID {
				encryptionSettingsChanged = true
			}
		case models.SyncDomainAttachments:
			if event.MemoID <= 0 {
				continue
			}
			if event.Action == models.SyncActionDelete {
				attachmentDeleteIDs[event.MemoID] = struct{}{}
			} else {
				attachmentUpsertIDs[event.MemoID] = struct{}{}
			}
		case models.SyncDomainGroupKeys:
			if event.GroupID <= 0 {
				continue
			}
			if event.Action == models.SyncActionDelete {
				if event.GroupMessageID > 0 {
					groupKeyDeleteNames[fmt.Sprintf("groups/%d/keyVersions/%d", event.GroupID, event.GroupMessageID)] = struct{}{}
				} else {
					groupKeyDeleteNames[fmt.Sprintf("groups/%d/keyVersions/current", event.GroupID)] = struct{}{}
				}
			} else {
				groupKeyUpsertGroupIDs[event.GroupID] = struct{}{}
			}
		}
	}

	if _, requested := domainSet[models.SyncDomainMemos]; requested {
		upsertIDs := sortedInt64Keys(memoUpsertIDs)
		if len(upsertIDs) > 0 {
			items, err := p.memoService.ListVisibleMemosByIDs(ctx, currentUser.ID, upsertIDs)
			if err != nil {
				return syncPullResponse{}, err
			}
			visibleIDs := make(map[int64]struct{}, len(items))
			for _, item := range items {
				visibleIDs[item.Memo.ID] = struct{}{}
				response.Patches.Memos.Upserts = append(response.Patches.Memos.Upserts, p.buildAPIMemo(item))
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
			user, err := p.userService.GetUser(ctx, userID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return syncPullResponse{}, err
			}
			response.Patches.Users.Upserts = append(response.Patches.Users.Upserts, toAPIUserSync(user))
		}
	}

	if _, requested := domainSet[models.SyncDomainFriendships]; requested {
		for _, friendID := range sortedInt64Keys(friendshipUpsertIDs) {
			friend, err := p.userService.GetUser(ctx, friendID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					friendshipDeleteIDs[friendID] = struct{}{}
					continue
				}
				return syncPullResponse{}, err
			}
			response.Patches.Friendships.Upserts = append(response.Patches.Friendships.Upserts, toAPIUserSync(friend))
		}
		for _, friendID := range sortedInt64Keys(friendshipDeleteIDs) {
			response.Patches.Friendships.Deletes = append(response.Patches.Friendships.Deletes, fmt.Sprintf("users/%d", friendID))
		}
	}

	groupByID := map[int64]service.GroupWithMembers{}
	needGroupMeta := len(groupUpsertIDs) > 0 || len(groupMessageUpsertIDsByGroup) > 0 || len(groupMessageDeleteIDsByGroup) > 0
	if needGroupMeta {
		groups, err := p.groupService.ListGroups(ctx, currentUser.ID)
		if err != nil {
			return syncPullResponse{}, err
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
				items, err := p.groupService.ListGroupMessagesByIDs(
					ctx,
					currentUser.ID,
					groupID,
					upsertIDs,
				)
				if err != nil {
					return syncPullResponse{}, err
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

			tags, err := p.groupService.ListGroupTags(ctx, currentUser.ID, groupID)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return syncPullResponse{}, err
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
		settings, err := p.userService.GetUserGeneralSettings(ctx, currentUser.ID)
		if err != nil {
			return syncPullResponse{}, err
		}
		apiSettings := toAPIGeneralSetting(settings)
		response.Patches.Settings.GeneralSetting = &apiSettings
	}

	if _, requested := domainSet[models.SyncDomainSettingsE2EE]; requested && encryptionSettingsChanged {
		encryptionKey, err := p.userService.GetUserEncryptionKey(ctx, currentUser.ID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return syncPullResponse{}, err
			}
		} else {
			apiEncryption := toAPIUserEncryptionSetting(encryptionKey)
			response.Patches.Settings.Encryption = &apiEncryption
		}
	}

	if _, requested := domainSet[models.SyncDomainAttachments]; requested {
		for _, attachmentID := range sortedInt64Keys(attachmentUpsertIDs) {
			attachment, err := p.sqlStore.GetAttachmentByID(ctx, attachmentID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					attachmentDeleteIDs[attachmentID] = struct{}{}
					continue
				}
				return syncPullResponse{}, err
			}
			visible, err := p.sqlStore.AttachmentVisibleToUser(ctx, attachmentID, currentUser.ID)
			if err != nil {
				return syncPullResponse{}, err
			}
			if !visible {
				attachmentDeleteIDs[attachmentID] = struct{}{}
				continue
			}
			response.Patches.Attachments.Upserts = append(
				response.Patches.Attachments.Upserts,
				toAPIAttachment(attachment, "", "", "", false),
			)
		}
		for _, attachmentID := range sortedInt64Keys(attachmentDeleteIDs) {
			response.Patches.Attachments.Deletes = append(
				response.Patches.Attachments.Deletes,
				fmt.Sprintf("attachments/%d", attachmentID),
			)
		}
	}

	if _, requested := domainSet[models.SyncDomainGroupKeys]; requested {
		for _, groupID := range sortedInt64Keys(groupKeyUpsertGroupIDs) {
			version, recipients, err := p.groupService.GetCurrentGroupKeyVersion(ctx, currentUser.ID, groupID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					groupKeyDeleteNames[fmt.Sprintf("groups/%d/keyVersions/current", groupID)] = struct{}{}
					continue
				}
				return syncPullResponse{}, err
			}
			response.Patches.GroupKeys.Upserts = append(response.Patches.GroupKeys.Upserts, toAPIGroupKeyVersion(version, recipients))
		}
		if len(groupKeyDeleteNames) > 0 {
			deleteNames := make([]string, 0, len(groupKeyDeleteNames))
			for name := range groupKeyDeleteNames {
				deleteNames = append(deleteNames, name)
			}
			sort.Strings(deleteNames)
			response.Patches.GroupKeys.Deletes = append(response.Patches.GroupKeys.Deletes, deleteNames...)
		}
	}

	return response, nil
}
