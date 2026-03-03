package http

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/service"
)

func registerGroupRoutes(
	api fiber.Router,
	groupService *service.GroupService,
) {
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

	api.Post("/groups/:id/join", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		groupID, err := parseRequiredIDParam(c, "id", "invalid group id")
		if err != nil {
			return err
		}
		group, err := groupService.JoinGroup(c.Context(), currentUser.ID, groupID)
		if err != nil {
			if mapped := mapNoRowsToNotFound(c, err, "group not found"); mapped != nil {
				return mapped
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
		msg, err := groupService.CreateGroupMessage(
			c.Context(),
			currentUser.ID,
			groupID,
			req.Content,
			req.Tags,
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

		updated, err := groupService.UpdateGroupMessage(
			c.Context(),
			currentUser.ID,
			groupID,
			messageID,
			req.Content,
			req.Tags,
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
}
