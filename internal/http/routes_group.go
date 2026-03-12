package http

import (
	"errors"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/service"
)

func registerGroupRoutes(
	api fiber.Router,
	userService *service.UserService,
	groupService *service.GroupService,
) {
	api.Post("/directs", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createDirectGroupRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		targetUser, err := userService.GetUserByIdentifier(c.Context(), req.User)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "user not found"); mapped != nil {
				return mapped
			}
			return internalError(c, err)
		}
		group, err := groupService.CreateDirectGroup(c.Context(), currentUser.ID, targetUser.ID)
		if err != nil {
			if errors.Is(err, service.ErrDirectGroupRequiresFriend) {
				return badRequest(c, err.Error())
			}
			return internalError(c, err)
		}
		return c.JSON(toAPIGroup(group))
	})

	api.Get("/groups", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groups, err := groupService.ListGroups(c.Context(), currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}

		resp := listGroupsResponse{
			Groups: make([]apiGroup, 0, len(groups)),
		}
		for _, group := range groups {
			resp.Groups = append(resp.Groups, toAPIGroup(group))
		}
		return c.JSON(resp)
	})

	api.Post("/groups", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createGroupRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		group, err := groupService.CreateGroup(
			c.Context(),
			currentUser.ID,
			req.Name,
			req.Description,
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIGroup(group))
	})

	api.Post("/groups/:id/members", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req addGroupMemberRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		targetUser, err := userService.GetUserByIdentifier(c.Context(), req.User)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "user not found"); mapped != nil {
				return mapped
			}
			return internalError(c, err)
		}
		group, err := groupService.AddGroupMember(c.Context(), currentUser.ID, groupID, targetUser.ID)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			if errors.Is(err, service.ErrGroupInviteRequiresFriend) || errors.Is(err, service.ErrDirectGroupMemberLimit) {
				return badRequest(c, err.Error())
			}
			return internalError(c, err)
		}
		return c.JSON(toAPIGroup(group))
	})

	api.Patch("/groups/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req updateGroupRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		group, err := groupService.UpdateGroup(c.Context(), currentUser.ID, groupID, req.Name, req.Description)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			if errors.Is(err, service.ErrDirectGroupImmutable) {
				return badRequest(c, err.Error())
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIGroup(group))
	})

	api.Delete("/groups/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		if err := groupService.DeleteOrLeaveGroup(c.Context(), currentUser.ID, groupID); err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return internalError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Get("/groups/:id/messages", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		pageSize, _ := strconv.Atoi(strings.TrimSpace(c.Query("pageSize", "50")))
		pageToken := c.Query("pageToken", "")
		messages, nextToken, err := groupService.ListGroupMessages(
			c.Context(),
			currentUser.ID,
			groupID,
			pageSize,
			pageToken,
		)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			if strings.Contains(strings.ToLower(err.Error()), "pagetoken") {
				return badRequest(c, "invalid pageToken")
			}
			return internalError(c, err)
		}
		resp := listGroupMessagesResponse{
			Messages:      make([]apiGroupMessage, 0, len(messages)),
			NextPageToken: nextToken,
		}
		for _, msg := range messages {
			resp.Messages = append(resp.Messages, toAPIGroupMessage(msg))
		}
		return c.JSON(resp)
	})

	api.Post("/groups/:id/read", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req markGroupReadRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		lastReadMessageID := int64(0)
		if trimmed := strings.TrimSpace(req.LastReadMessage); trimmed != "" {
			lastReadMessageID, err = parseMessageResourceID(trimmed)
			if err != nil {
				return badRequest(c, "invalid lastReadMessage")
			}
		}
		if err := groupService.MarkGroupRead(c.Context(), currentUser.ID, groupID, lastReadMessageID); err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return badRequest(c, err.Error())
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Post("/groups/:id/messages", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req createGroupMessageRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		encryptedPayload := strings.TrimSpace(req.EncryptedPayload)
		if encryptedPayload == "" {
			return badRequest(c, "encryptedPayload is required")
		}
		if err := validatePayloadEnvelope(&req.PayloadEnvelope); err != nil {
			return badRequest(c, "invalid payloadEnvelope")
		}
		attachmentBindings, err := attachmentBindingsFromAPI(req.Attachments)
		if err != nil {
			return badRequest(c, "invalid attachment bindings")
		}
		msg, err := groupService.CreateGroupMessage(
			c.Context(),
			currentUser.ID,
			groupID,
			encryptedPayload,
			mustMarshalPayloadEnvelope(&req.PayloadEnvelope),
			req.Tags,
			attachmentBindings,
		)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIGroupMessage(msg))
	})

	api.Patch("/groups/:id/messages/:messageId", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		messageID, err := parseRequiredIDParam(c, "messageId", "invalid message id")
		if err != nil {
			return err
		}
		var req updateGroupMessageRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		attachmentBindings, err := attachmentBindingsFromAPIPointer(req.Attachments)
		if err != nil {
			return badRequest(c, "invalid attachment bindings")
		}
		if req.EncryptedPayload != nil && strings.TrimSpace(*req.EncryptedPayload) == "" {
			return badRequest(c, "encryptedPayload must not be empty")
		}
		if req.PayloadEnvelope != nil {
			if err := validatePayloadEnvelope(req.PayloadEnvelope); err != nil {
				return badRequest(c, "invalid payloadEnvelope")
			}
		}

		updated, err := groupService.UpdateGroupMessage(
			c.Context(),
			currentUser.ID,
			groupID,
			messageID,
			trimUpdatedText(req.EncryptedPayload),
			req.PayloadEnvelope.asJSONStringPointer(),
			req.Tags,
			attachmentBindings,
		)
		if err != nil {
			if mapped := mapGroupMessageMutationError(c, err, "group message not found", true); mapped != nil {
				return mapped
			}
		}
		return c.JSON(toAPIGroupMessage(updated))
	})

	api.Delete("/groups/:id/messages/:messageId", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		messageID, err := parseRequiredIDParam(c, "messageId", "invalid message id")
		if err != nil {
			return err
		}

		if err := groupService.DeleteGroupMessage(
			c.Context(),
			currentUser.ID,
			groupID,
			messageID,
		); err != nil {
			if mapped := mapGroupMessageMutationError(c, err, "group message not found", false); mapped != nil {
				return mapped
			}
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Get("/groups/:id/tags", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		tags, err := groupService.ListGroupTags(c.Context(), currentUser.ID, groupID)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return internalError(c, err)
		}
		return c.JSON(listGroupTagsResponse{Tags: tags})
	})

	api.Post("/groups/:id/tags", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req addGroupTagRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		tags, err := groupService.AddGroupTag(c.Context(), currentUser.ID, groupID, req.Tag)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(listGroupTagsResponse{Tags: tags})
	})

	api.Get("/groups/:id/keyVersions/current", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		version, recipients, err := groupService.GetCurrentGroupKeyVersion(c.Context(), currentUser.ID, groupID)
		if err != nil {
			return mapGroupMessageMutationError(c, err, "group key version not found", false)
		}
		return c.JSON(groupKeyVersionResponse{
			GroupKeyVersion: toAPIGroupKeyVersion(version, recipients),
		})
	})

	api.Post("/groups/:id/keyVersions", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		var req createGroupKeyVersionRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if err := validateGroupKeyVersionWrappedKeys(req.GroupKeyVersion.WrappedKeys); err != nil {
			return badRequest(c, "invalid group key wrappedKeys")
		}
		wrappedKeys := make([]service.WrappedKeySlotInput, 0, len(req.GroupKeyVersion.WrappedKeys))
		for _, wrappedKey := range req.GroupKeyVersion.WrappedKeys {
			wrappedKeys = append(wrappedKeys, service.WrappedKeySlotInput{
				SlotType:      wrappedKey.SlotType,
				SlotRef:       wrappedKey.SlotRef,
				WrapAlgorithm: wrappedKey.WrapAlgorithm,
				WrappedKey:    wrappedKey.WrappedKey,
			})
		}
		version, recipients, err := groupService.CreateGroupKeyVersion(c.Context(), currentUser.ID, groupID, service.CreateGroupKeyVersionInput{
			Algorithm:   req.GroupKeyVersion.Algorithm,
			WrappedKeys: wrappedKeys,
		})
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(groupKeyVersionResponse{
			GroupKeyVersion: toAPIGroupKeyVersion(version, recipients),
		})
	})
}
