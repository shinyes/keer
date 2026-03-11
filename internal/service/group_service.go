package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type GroupService struct {
	store *store.SQLStore
}

var ErrGroupMessagePermissionDenied = errors.New("group message permission denied")

type GroupWithMembers struct {
	Group   models.Group
	Members []models.User
}

type GroupMessageWithCreator struct {
	Message     models.GroupMessage
	Creator     models.User
	Attachments []models.Attachment
}

type CreateGroupKeyVersionInput struct {
	Algorithm   string
	WrappedKeys []WrappedKeySlotInput
}

type WrappedKeySlotInput struct {
	SlotType      string
	SlotRef       string
	WrapAlgorithm string
	WrappedKey    string
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
	messageIDs := make([]int64, 0, len(msgs))
	result := make([]GroupMessageWithCreator, 0, len(msgs))
	for _, msg := range msgs {
		messageIDs = append(messageIDs, msg.ID)
		creator, ok := creatorMap[msg.CreatorID]
		if !ok {
			user, err := s.store.GetUserByID(ctx, msg.CreatorID)
			if err != nil {
				return nil, "", err
			}
			creator = user
			creatorMap[msg.CreatorID] = user
		}
		result = append(result, GroupMessageWithCreator{Message: msg, Creator: creator})
	}
	attachmentsByMessageID, err := s.store.ListAttachmentsByGroupMessageIDs(ctx, messageIDs)
	if err != nil {
		return nil, "", err
	}
	for i := range result {
		result[i].Attachments = attachmentsByMessageID[result[i].Message.ID]
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
	payloadEnvelope string,
	tags []string,
	attachmentBindings []AttachmentBindingInput,
) (GroupMessageWithCreator, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return GroupMessageWithCreator{}, err
	}
	normalizedContent := strings.TrimSpace(content)
	if normalizedContent == "" {
		return GroupMessageWithCreator{}, fmt.Errorf("message content is required")
	}
	attachments, err := s.resolveAttachmentBindingsFromGroupInput(ctx, userID, attachmentBindings)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	msg, err := s.store.CreateGroupMessage(ctx, groupID, userID, normalizedContent, strings.TrimSpace(payloadEnvelope), tags, attachments)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	creator, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	result := GroupMessageWithCreator{
		Message: msg,
		Creator: creator,
	}
	if len(attachments) > 0 {
		if err := s.store.SetGroupMessageAttachments(ctx, msg.ID, attachments); err != nil {
			return GroupMessageWithCreator{}, err
		}
		attachmentsByMessageID, err := s.store.ListAttachmentsByGroupMessageIDs(ctx, []int64{msg.ID})
		if err != nil {
			return GroupMessageWithCreator{}, err
		}
		result.Attachments = attachmentsByMessageID[msg.ID]
	}
	return result, nil
}

func (s *GroupService) UpdateGroupMessage(
	ctx context.Context,
	userID int64,
	groupID int64,
	messageID int64,
	content *string,
	payloadEnvelope *string,
	tags *[]string,
	attachmentBindings *[]AttachmentBindingInput,
) (GroupMessageWithCreator, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return GroupMessageWithCreator{}, err
	}
	existing, err := s.store.GetGroupMessageByID(ctx, messageID)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	if existing.GroupID != groupID {
		return GroupMessageWithCreator{}, sql.ErrNoRows
	}
	if !canManageGroupMessage(existing, userID) {
		return GroupMessageWithCreator{}, ErrGroupMessagePermissionDenied
	}

	nextContent := existing.Content
	if content != nil {
		nextContent = strings.TrimSpace(*content)
	}
	if strings.TrimSpace(nextContent) == "" {
		return GroupMessageWithCreator{}, fmt.Errorf("message content is required")
	}

	nextTags := existing.Tags
	if tags != nil {
		nextTags = *tags
	}
	nextPayloadEnvelope := existing.PayloadEnvelope
	if payloadEnvelope != nil {
		nextPayloadEnvelope = strings.TrimSpace(*payloadEnvelope)
	}
	var attachments []store.AttachmentBinding
	if attachmentBindings != nil {
		resolvedAttachments, err := s.resolveAttachmentBindingsFromGroupUpdate(ctx, userID, messageID, *attachmentBindings)
		if err != nil {
			return GroupMessageWithCreator{}, err
		}
		attachments = resolvedAttachments
	} else {
		currentAttachments, err := s.store.ListAttachmentsByGroupMessageIDs(ctx, []int64{messageID})
		if err != nil {
			return GroupMessageWithCreator{}, err
		}
		for _, attachment := range currentAttachments[messageID] {
			attachments = append(attachments, store.AttachmentBinding{
				AttachmentID:                  attachment.ID,
				AssociationEncryptionMetadata: attachment.AssociationEncryptionMetadata,
			})
		}
	}
	updated, err := s.store.UpdateGroupMessage(
		ctx,
		groupID,
		messageID,
		userID,
		nextContent,
		nextPayloadEnvelope,
		nextTags,
		attachments,
	)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	creator, err := s.store.GetUserByID(ctx, updated.CreatorID)
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	result := GroupMessageWithCreator{
		Message: updated,
		Creator: creator,
	}
	attachmentsByMessageID, err := s.store.ListAttachmentsByGroupMessageIDs(ctx, []int64{updated.ID})
	if err != nil {
		return GroupMessageWithCreator{}, err
	}
	result.Attachments = attachmentsByMessageID[updated.ID]
	return result, nil
}

func (s *GroupService) DeleteGroupMessage(
	ctx context.Context,
	userID int64,
	groupID int64,
	messageID int64,
) error {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return err
	}
	existing, err := s.store.GetGroupMessageByID(ctx, messageID)
	if err != nil {
		return err
	}
	if existing.GroupID != groupID {
		return sql.ErrNoRows
	}
	if !canManageGroupMessage(existing, userID) {
		return ErrGroupMessagePermissionDenied
	}
	return s.store.DeleteGroupMessage(ctx, groupID, messageID)
}

func (s *GroupService) GetCurrentGroupKeyVersion(ctx context.Context, userID int64, groupID int64) (models.GroupKeyVersion, []models.GroupKeyVersionRecipient, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	version, err := s.store.GetCurrentGroupKeyVersion(ctx, groupID)
	if err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	recipients, err := s.store.ListGroupKeyVersionRecipients(ctx, groupID, version.Version)
	if err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	return version, recipients, nil
}

func (s *GroupService) CreateGroupKeyVersion(ctx context.Context, userID int64, groupID int64, input CreateGroupKeyVersionInput) (models.GroupKeyVersion, []models.GroupKeyVersionRecipient, error) {
	if err := s.ensureGroupMember(ctx, groupID, userID); err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	algorithm := strings.TrimSpace(input.Algorithm)
	if algorithm == "" {
		return models.GroupKeyVersion{}, nil, fmt.Errorf("group key algorithm is required")
	}
	groupMembers, err := s.store.ListGroupMembers(ctx, groupID)
	if err != nil {
		return models.GroupKeyVersion{}, nil, err
	}
	memberIDs := make(map[int64]struct{}, len(groupMembers))
	for _, member := range groupMembers {
		memberIDs[member.ID] = struct{}{}
	}

	recipients := make([]models.GroupKeyVersionRecipient, 0, len(input.WrappedKeys))
	for _, wrappedKey := range input.WrappedKeys {
		slotType := strings.TrimSpace(wrappedKey.SlotType)
		if slotType != "account_public" {
			continue
		}
		targetUserID, err := parseWrappedKeyUserID(wrappedKey.SlotRef)
		if err != nil {
			return models.GroupKeyVersion{}, nil, err
		}
		if _, exists := memberIDs[targetUserID]; !exists {
			return models.GroupKeyVersion{}, nil, fmt.Errorf("wrapped key recipient is not a group member")
		}
		recipients = append(recipients, models.GroupKeyVersionRecipient{
			UserID:        targetUserID,
			SlotRef:       strings.TrimSpace(wrappedKey.SlotRef),
			WrapAlgorithm: strings.TrimSpace(wrappedKey.WrapAlgorithm),
			WrappedKey:    strings.TrimSpace(wrappedKey.WrappedKey),
		})
	}
	if len(recipients) == 0 {
		return models.GroupKeyVersion{}, nil, fmt.Errorf("wrapped group keys are required")
	}
	return s.store.CreateGroupKeyVersion(ctx, groupID, algorithm, recipients)
}

func (s *GroupService) resolveAttachmentBindingsFromGroupInput(ctx context.Context, userID int64, bindings []AttachmentBindingInput) ([]store.AttachmentBinding, error) {
	if len(bindings) == 0 {
		return []store.AttachmentBinding{}, nil
	}
	resolved := make([]store.AttachmentBinding, 0, len(bindings))
	seen := make(map[int64]struct{}, len(bindings))
	for _, binding := range bindings {
		id, err := parseResourceID(binding.Name)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		belongs, err := s.store.AttachmentBelongsToUser(ctx, id, userID)
		if err != nil {
			return nil, err
		}
		if !belongs {
			return nil, fmt.Errorf("attachment %d not found", id)
		}
		resolved = append(resolved, store.AttachmentBinding{
			AttachmentID:                  id,
			AssociationEncryptionMetadata: strings.TrimSpace(binding.AssociationEncryptionMetadata),
		})
	}
	return resolved, nil
}

func (s *GroupService) resolveAttachmentBindingsFromGroupUpdate(
	ctx context.Context,
	userID int64,
	messageID int64,
	bindings []AttachmentBindingInput,
) ([]store.AttachmentBinding, error) {
	if len(bindings) == 0 {
		return []store.AttachmentBinding{}, nil
	}
	resolved, err := s.resolveAttachmentBindingsFromGroupInput(ctx, userID, bindings)
	if err == nil {
		return resolved, nil
	}

	existingMap, existingErr := s.store.ListAttachmentsByGroupMessageIDs(ctx, []int64{messageID})
	if existingErr != nil {
		return nil, existingErr
	}
	existingAttachmentIDs := make(map[int64]struct{}, len(existingMap[messageID]))
	for _, attachment := range existingMap[messageID] {
		existingAttachmentIDs[attachment.ID] = struct{}{}
	}

	resolved = make([]store.AttachmentBinding, 0, len(bindings))
	seen := make(map[int64]struct{}, len(bindings))
	for _, binding := range bindings {
		id, parseErr := parseResourceID(binding.Name)
		if parseErr != nil {
			return nil, parseErr
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		if _, alreadyAttached := existingAttachmentIDs[id]; alreadyAttached {
			resolved = append(resolved, store.AttachmentBinding{
				AttachmentID:                  id,
				AssociationEncryptionMetadata: strings.TrimSpace(binding.AssociationEncryptionMetadata),
			})
			continue
		}
		belongs, belongsErr := s.store.AttachmentBelongsToUser(ctx, id, userID)
		if belongsErr != nil {
			return nil, belongsErr
		}
		if !belongs {
			return nil, fmt.Errorf("attachment %d not found", id)
		}
		resolved = append(resolved, store.AttachmentBinding{
			AttachmentID:                  id,
			AssociationEncryptionMetadata: strings.TrimSpace(binding.AssociationEncryptionMetadata),
		})
	}
	return resolved, nil
}

func parseWrappedKeyUserID(slotRef string) (int64, error) {
	slotRef = strings.TrimSpace(slotRef)
	if slotRef == "" {
		return 0, fmt.Errorf("wrapped key slotRef is required")
	}
	slotRef = strings.Trim(slotRef, "/")
	if idx := strings.LastIndex(slotRef, "/"); idx >= 0 {
		slotRef = slotRef[idx+1:]
	}
	userID, err := strconv.ParseInt(slotRef, 10, 64)
	if err != nil || userID <= 0 {
		return 0, fmt.Errorf("invalid wrapped key slotRef")
	}
	return userID, nil
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

func canManageGroupMessage(message models.GroupMessage, userID int64) bool {
	if message.CreatorID == userID {
		return true
	}
	targetUserID := models.Int64ToString(userID)
	for _, tag := range message.Tags {
		normalized := strings.TrimSpace(tag)
		if !strings.HasPrefix(normalized, "collab/") {
			continue
		}
		collaboratorID := strings.TrimSpace(strings.TrimPrefix(normalized, "collab/"))
		if collaboratorID == targetUserID {
			return true
		}
	}
	return false
}
