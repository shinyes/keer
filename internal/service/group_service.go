package service

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type GroupService struct {
	store *store.SQLStore
}

type GroupWithMembers struct {
	Group   models.Group
	Members []models.User
}

type GroupMessageWithCreator struct {
	Message models.GroupMessage
	Creator models.User
}

func NewGroupService(s *store.SQLStore) *GroupService {
	return &GroupService{store: s}
}

func (s *GroupService) CreateGroup(
	ctx context.Context,
	creatorID int64,
	name string,
	description string,
) (GroupWithMembers, error) {
	normalizedName := strings.TrimSpace(name)
	if normalizedName == "" {
		return GroupWithMembers{}, fmt.Errorf("group name is required")
	}
	group, err := s.store.CreateGroup(ctx, creatorID, normalizedName, strings.TrimSpace(description))
	if err != nil {
		return GroupWithMembers{}, err
	}
	return s.loadGroupWithMembers(ctx, group.ID)
}

func (s *GroupService) JoinGroup(ctx context.Context, userID int64, groupID int64) (GroupWithMembers, error) {
	if _, err := s.store.GetGroupByID(ctx, groupID); err != nil {
		return GroupWithMembers{}, err
	}
	if err := s.store.AddGroupMember(ctx, groupID, userID); err != nil {
		return GroupWithMembers{}, err
	}
	return s.loadGroupWithMembers(ctx, groupID)
}

func (s *GroupService) UpdateGroup(
	ctx context.Context,
	userID int64,
	groupID int64,
	name *string,
	description *string,
) (GroupWithMembers, error) {
	group, err := s.store.GetGroupByID(ctx, groupID)
	if err != nil {
		return GroupWithMembers{}, err
	}
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return GroupWithMembers{}, err
	}

	nextName := group.GroupName
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed == "" {
			return GroupWithMembers{}, fmt.Errorf("group name is required")
		}
		nextName = trimmed
	}
	nextDescription := group.Description
	if description != nil {
		nextDescription = strings.TrimSpace(*description)
	}
	if _, err := s.store.UpdateGroup(ctx, groupID, nextName, nextDescription); err != nil {
		return GroupWithMembers{}, err
	}
	return s.loadGroupWithMembers(ctx, groupID)
}

func (s *GroupService) DeleteOrLeaveGroup(ctx context.Context, userID int64, groupID int64) error {
	group, err := s.store.GetGroupByID(ctx, groupID)
	if err != nil {
		return err
	}
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return err
	}
	if group.CreatorID == userID {
		return s.store.DeleteGroup(ctx, groupID)
	}
	return s.store.RemoveGroupMember(ctx, groupID, userID)
}

func (s *GroupService) ListGroups(ctx context.Context, userID int64) ([]GroupWithMembers, error) {
	groups, err := s.store.ListGroupsByUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	result := make([]GroupWithMembers, 0, len(groups))
	for _, group := range groups {
		members, err := s.store.ListGroupMembers(ctx, group.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, GroupWithMembers{
			Group:   group,
			Members: members,
		})
	}
	return result, nil
}

func (s *GroupService) ListGroupTags(ctx context.Context, userID int64, groupID int64) ([]string, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return nil, err
	}
	return s.store.ListGroupTags(ctx, groupID)
}

func (s *GroupService) AddGroupTag(ctx context.Context, userID int64, groupID int64, tag string) ([]string, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return nil, err
	}
	normalized := strings.TrimSpace(tag)
	if normalized == "" {
		return nil, fmt.Errorf("tag is required")
	}
	if err := s.store.UpsertGroupTags(ctx, groupID, userID, []string{normalized}); err != nil {
		return nil, err
	}
	return s.store.ListGroupTags(ctx, groupID)
}

func (s *GroupService) ListGroupMessages(
	ctx context.Context,
	userID int64,
	groupID int64,
	pageSize int,
	pageToken string,
) ([]GroupMessageWithCreator, string, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return nil, "", err
	}
	offset, err := parseGroupPageToken(pageToken)
	if err != nil {
		return nil, "", fmt.Errorf("invalid pageToken")
	}
	msgs, nextOffset, err := s.store.ListGroupMessagesPage(ctx, groupID, pageSize, offset)
	if err != nil {
		return nil, "", err
	}
	if len(msgs) == 0 {
		return []GroupMessageWithCreator{}, "", nil
	}

	creatorMap := make(map[int64]models.User)
	result := make([]GroupMessageWithCreator, 0, len(msgs))
	for _, msg := range msgs {
		creator, ok := creatorMap[msg.CreatorID]
		if !ok {
			user, err := s.store.GetUserByID(ctx, msg.CreatorID)
			if err != nil {
				return nil, "", err
			}
			creator = user
			creatorMap[msg.CreatorID] = user
		}
		result = append(result, GroupMessageWithCreator{
			Message: msg,
			Creator: creator,
		})
	}

	nextToken := ""
	if nextOffset >= 0 {
		nextToken = strconv.Itoa(nextOffset)
	}
	return result, nextToken, nil
}

func (s *GroupService) CreateGroupMessage(
	ctx context.Context,
	userID int64,
	groupID int64,
	content string,
	tags []string,
) (GroupMessageWithCreator, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return GroupMessageWithCreator{}, err
	}
	normalizedContent := strings.TrimSpace(content)
	if normalizedContent == "" {
		return GroupMessageWithCreator{}, fmt.Errorf("message content is required")
	}
	msg, err := s.store.CreateGroupMessage(ctx, groupID, userID, normalizedContent, tags)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	creator, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	return GroupMessageWithCreator{
		Message: msg,
		Creator: creator,
	}, nil
}

func (s *GroupService) ensureGroupMember(ctx context.Context, groupID int64, userID int64) error {
	member, err := s.store.IsGroupMember(ctx, groupID, userID)
	if err != nil {
		return err
	}
	if !member {
		return sql.ErrNoRows
	}
	return nil
}

func (s *GroupService) loadGroupWithMembers(ctx context.Context, groupID int64) (GroupWithMembers, error) {
	group, err := s.store.GetGroupByID(ctx, groupID)
	if err != nil {
		return GroupWithMembers{}, err
	}
	members, err := s.store.ListGroupMembers(ctx, groupID)
	if err != nil {
		return GroupWithMembers{}, err
	}
	return GroupWithMembers{
		Group:   group,
		Members: members,
	}, nil
}

func parseGroupPageToken(pageToken string) (int, error) {
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
