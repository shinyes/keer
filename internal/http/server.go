package http

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

func NewRouter(cfg config.Config, userService *service.UserService, memoService *service.MemoService, attachmentService *service.AttachmentService) *fiber.App {
	app := fiber.New()
	app.Use(cors.New())

	app.Get("/api/v1/instance/profile", func(c *fiber.Ctx) error {
		return c.JSON(profileResponse{
			Version: cfg.Version,
		})
	})

	app.Post("/api/v1/auth/signin", func(c *fiber.Ctx) error {
		var req signInRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if req.PasswordCredentials == nil {
			return badRequest(c, "passwordCredentials is required")
		}

		user, accessToken, err := userService.SignInWithPassword(
			c.Context(),
			req.PasswordCredentials.Username,
			req.PasswordCredentials.Password,
		)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrInvalidCredentials):
				return badRequest(c, "unmatched username and password")
			default:
				return internalError(c, err)
			}
		}

		return c.JSON(signInResponse{
			User:        toAPIUser(user),
			AccessToken: accessToken,
		})
	})

	app.Post("/api/v1/users", func(c *fiber.Ctx) error {
		var req createUserRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		creator, err := OptionalAuthenticateToken(c, userService)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"message": "invalid access token",
			})
		}

		allowRegistration, err := userService.ResolveAllowRegistration(c.Context(), cfg.AllowRegistration)
		if err != nil {
			return internalError(c, err)
		}

		user, err := userService.CreateUser(c.Context(), creator, service.CreateUserInput{
			Username:     req.User.Username,
			DisplayName:  req.User.DisplayName,
			Password:     req.User.Password,
			Role:         req.User.Role,
			ValidateOnly: req.ValidateOnly,
		}, allowRegistration)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrInvalidUsername):
				return badRequest(c, "invalid username")
			case errors.Is(err, service.ErrInvalidDisplayName):
				return badRequest(c, "invalid displayName")
			case errors.Is(err, service.ErrInvalidPassword):
				return badRequest(c, "invalid password")
			case errors.Is(err, service.ErrInvalidRole):
				return badRequest(c, "invalid role")
			case errors.Is(err, service.ErrUsernameAlreadyExists):
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{"message": "username already exists"})
			case errors.Is(err, service.ErrRegistrationDisabled):
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "user registration is not allowed"})
			default:
				return internalError(c, err)
			}
		}

		return c.JSON(toAPIUser(user))
	})

	api := app.Group("/api/v1", AuthMiddleware(userService))
	api.Get("/auth/me", func(c *fiber.Ctx) error {
		user := CurrentUser(c)
		return c.JSON(getCurrentUserResponse{
			User: toAPIUser(user),
		})
	})

	api.Get("/users/:name/settings/GENERAL", func(c *fiber.Ctx) error {
		name := strings.TrimSpace(c.Params("name"))
		if name == "" {
			return badRequest(c, "invalid user name")
		}
		user, err := userService.GetUserByIdentifier(c.Context(), name)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "user not found")
			}
			return internalError(c, err)
		}
		return c.JSON(userSettingResponse{
			GeneralSetting: generalSetting{
				MemoVisibility: string(user.DefaultVisibility),
			},
		})
	})

	api.Get("/users/:name\\:getStats", func(c *fiber.Ctx) error {
		name := strings.TrimSpace(c.Params("name"))
		if name == "" {
			return badRequest(c, "invalid user name")
		}
		requestedUser, err := userService.GetUserByIdentifier(c.Context(), name)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "user not found")
			}
			return internalError(c, err)
		}
		currentUser := CurrentUser(c)
		tagCount, err := memoService.GetUserTagCount(c.Context(), requestedUser.ID, currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}
		return c.JSON(userStatsResponse{
			TagCount: tagCount,
		})
	})

	api.Get("/users/:name", func(c *fiber.Ctx) error {
		name := strings.TrimSpace(c.Params("name"))
		if name == "" {
			return badRequest(c, "invalid user name")
		}
		user, err := userService.GetUserByIdentifier(c.Context(), name)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "user not found")
			}
			return internalError(c, err)
		}
		return c.JSON(toAPIUser(user))
	})

	api.Get("/memos", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		pageSize, _ := strconv.Atoi(strings.TrimSpace(c.Query("pageSize", "50")))
		pageToken := c.Query("pageToken", "")
		filter := c.Query("filter", "")
		var state *models.MemoState
		stateRaw := strings.TrimSpace(c.Query("state"))
		if stateRaw != "" {
			s := models.MemoState(stateRaw)
			if !s.IsValid() {
				return badRequest(c, "invalid state")
			}
			state = &s
		}

		memos, nextToken, err := memoService.ListMemos(c.Context(), currentUser.ID, state, filter, pageSize, pageToken)
		if err != nil {
			return badRequest(c, err.Error())
		}

		resp := listMemosResponse{
			Memos:         make([]apiMemo, 0, len(memos)),
			NextPageToken: nextToken,
		}
		for _, item := range memos {
			resp.Memos = append(resp.Memos, toAPIMemo(item))
		}
		return c.JSON(resp)
	})

	api.Post("/memos", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createMemoRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		attachmentNames := make([]string, 0, len(req.Attachments))
		for _, attachment := range req.Attachments {
			if attachment.Name == "" {
				continue
			}
			attachmentNames = append(attachmentNames, attachment.Name)
		}

		visibility := models.Visibility(req.Visibility)
		if req.Visibility == "" {
			visibility = currentUser.DefaultVisibility
		}
		created, err := memoService.CreateMemo(
			c.Context(),
			currentUser.ID,
			service.CreateMemoInput{
				Content:         req.Content,
				Visibility:      visibility,
				AttachmentNames: attachmentNames,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIMemo(created))
	})

	api.Patch("/memos/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		memoID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid memo id")
		}

		var req updateMemoRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		var visibility *models.Visibility
		if req.Visibility != nil {
			v := models.Visibility(*req.Visibility)
			visibility = &v
		}
		var state *models.MemoState
		if req.State != nil {
			s := models.MemoState(*req.State)
			state = &s
		}
		var attachmentNames *[]string
		if req.Attachments != nil {
			names := make([]string, 0, len(*req.Attachments))
			for _, attachment := range *req.Attachments {
				if attachment.Name == "" {
					continue
				}
				names = append(names, attachment.Name)
			}
			attachmentNames = &names
		}

		updated, err := memoService.UpdateMemo(
			c.Context(),
			currentUser.ID,
			memoID,
			service.UpdateMemoInput{
				Content:         req.Content,
				Visibility:      visibility,
				State:           state,
				Pinned:          req.Pinned,
				AttachmentNames: attachmentNames,
			},
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "memo not found")
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIMemo(updated))
	})

	api.Delete("/memos/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		memoID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid memo id")
		}
		if err := memoService.DeleteMemo(c.Context(), currentUser.ID, memoID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "memo not found")
			}
			return internalError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Get("/attachments", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachments, err := attachmentService.ListAttachments(c.Context(), currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}
		resp := listAttachmentsResponse{
			Attachments: make([]apiAttachment, 0, len(attachments)),
		}
		for _, attachment := range attachments {
			resp.Attachments = append(resp.Attachments, toAPIAttachment(attachment, ""))
		}
		return c.JSON(resp)
	})

	api.Post("/attachments", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createAttachmentRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		attachment, err := attachmentService.CreateAttachment(
			c.Context(),
			currentUser.ID,
			service.CreateAttachmentInput{
				Filename: req.Filename,
				Type:     req.Type,
				Content:  req.Content,
				MemoName: req.Memo,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIAttachment(attachment, ""))
	})

	api.Delete("/attachments/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachmentID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid attachment id")
		}
		if err := attachmentService.DeleteAttachment(c.Context(), currentUser.ID, attachmentID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "attachment not found")
			}
			return internalError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	app.Get("/file/attachments/:id/:filename", AuthMiddleware(userService), func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachmentID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid attachment id")
		}

		attachment, rc, err := attachmentService.OpenAttachment(c.Context(), attachmentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "attachment not found")
			}
			return internalError(c, err)
		}
		defer rc.Close()

		if attachment.CreatorID != currentUser.ID {
			return c.SendStatus(fiber.StatusForbidden)
		}

		c.Set(fiber.HeaderContentType, attachment.Type)
		c.Set(fiber.HeaderContentDisposition, fmt.Sprintf(`inline; filename="%s"`, attachment.Filename))
		return c.SendStream(rc, int(attachment.Size))
	})

	return app
}

func toAPIUser(user models.User) apiUser {
	role := strings.ToUpper(strings.TrimSpace(user.Role))
	switch role {
	case "HOST", "ADMIN":
		role = "ADMIN"
	case "USER":
	default:
		role = "ROLE_UNSPECIFIED"
	}
	name := ""
	if user.ID > 0 {
		name = user.Name()
	}
	return apiUser{
		Name:        name,
		Role:        role,
		Username:    user.Username,
		DisplayName: user.DisplayName,
		State:       "NORMAL",
		CreateTime:  formatMaybeTime(user.CreateTime),
		UpdateTime:  formatMaybeTime(user.UpdateTime),
	}
}

func toAPIMemo(memo service.MemoWithAttachments) apiMemo {
	attachments := make([]apiAttachment, 0, len(memo.Attachments))
	for _, attachment := range memo.Attachments {
		attachments = append(attachments, toAPIAttachment(attachment, memo.Memo.Name()))
	}
	tags := memo.Memo.Payload.Tags
	if tags == nil {
		tags = []string{}
	}
	return apiMemo{
		Name:        memo.Memo.Name(),
		State:       string(memo.Memo.State),
		Creator:     "users/" + models.Int64ToString(memo.Memo.CreatorID),
		CreateTime:  formatTime(memo.Memo.CreateTime),
		UpdateTime:  formatTime(memo.Memo.UpdateTime),
		DisplayTime: formatTime(memo.Memo.DisplayTime),
		Content:     memo.Memo.Content,
		Visibility:  string(memo.Memo.Visibility),
		Pinned:      memo.Memo.Pinned,
		Attachments: attachments,
		Tags:        tags,
	}
}

func toAPIAttachment(attachment models.Attachment, memoName string) apiAttachment {
	return apiAttachment{
		Name:         "attachments/" + models.Int64ToString(attachment.ID),
		CreateTime:   formatTime(attachment.CreateTime),
		Filename:     attachment.Filename,
		ExternalLink: attachment.ExternalLink,
		Type:         attachment.Type,
		Size:         models.Int64ToString(attachment.Size),
		Memo:         memoName,
	}
}

func parseID(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty id")
	}
	return strconv.ParseInt(raw, 10, 64)
}

func badRequest(c *fiber.Ctx, message string) error {
	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
		"message": message,
	})
}

func notFound(c *fiber.Ctx, message string) error {
	return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
		"message": message,
	})
}

func internalError(c *fiber.Ctx, err error) error {
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"message": err.Error(),
	})
}
