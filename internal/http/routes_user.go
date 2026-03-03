package http

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

func registerUserRoutes(
	api fiber.Router,
	userService *service.UserService,
	memoService *service.MemoService,
) {
	api.Get("/auth/me", func(c *fiber.Ctx) error {
		user := CurrentUser(c)
		return c.JSON(getCurrentUserResponse{
			User: toAPIUser(user),
		})
	})

	api.Get("/users/:name/settings/GENERAL", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
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
		if user.ID != currentUser.ID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "forbidden"})
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

	api.Get("/users/batch", func(c *fiber.Ctx) error {
		identifiers := parseBatchIdentifiers(c.Query("ids"))
		if len(identifiers) > 200 {
			return badRequest(c, "too many user ids")
		}

		resp := listUsersResponse{
			Users: make([]apiUser, 0, len(identifiers)),
		}
		if len(identifiers) == 0 {
			return c.JSON(resp)
		}

		seenUserIDs := make(map[int64]struct{}, len(identifiers))
		for _, identifier := range identifiers {
			user, err := userService.GetUserByIdentifier(c.Context(), identifier)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return internalError(c, err)
			}
			if _, exists := seenUserIDs[user.ID]; exists {
				continue
			}
			seenUserIDs[user.ID] = struct{}{}
			resp.Users = append(resp.Users, toAPIUserSync(user))
		}

		return c.JSON(resp)
	})

	api.Get("/users/changes", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)

		sinceRaw := strings.TrimSpace(c.Query("since"))
		if sinceRaw == "" {
			return badRequest(c, "since is required")
		}
		since, err := time.Parse(time.RFC3339Nano, sinceRaw)
		if err != nil {
			return badRequest(c, "invalid since")
		}

		identifiers := parseBatchIdentifiers(c.Query("ids"))
		if len(identifiers) == 0 {
			identifiers = []string{models.Int64ToString(currentUser.ID)}
		}
		if len(identifiers) > 200 {
			return badRequest(c, "too many user ids")
		}

		syncAnchor := time.Now().UTC()
		changes, err := userService.ListUserChanges(
			c.Context(),
			identifiers,
			since,
			syncAnchor,
		)
		if err != nil {
			return internalError(c, err)
		}

		resp := listUserChangesResponse{
			Users:      make([]apiUser, 0, len(changes.Users)),
			SyncAnchor: changes.SyncAnchor.Format(time.RFC3339Nano),
		}
		for _, user := range changes.Users {
			resp.Users = append(resp.Users, toAPIUserSync(user))
		}
		return c.JSON(resp)
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

	api.Patch("/users/:name", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		name := strings.TrimSpace(c.Params("name"))
		if name == "" {
			return badRequest(c, "invalid user name")
		}
		targetUser, err := userService.GetUserByIdentifier(c.Context(), name)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "user not found")
			}
			return internalError(c, err)
		}
		if targetUser.ID != currentUser.ID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "forbidden"})
		}

		var req updateUserRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if req.User.Avatar != nil && req.User.AvatarURL != nil {
			return badRequest(c, "avatar and avatarUrl cannot both be set")
		}
		var updatedUser models.User
		switch {
		case req.User.Avatar != nil:
			updatedUser, err = userService.UpdateUserAvatarThumbnail(
				c.Context(),
				targetUser.ID,
				req.User.Avatar.Content,
				req.User.Avatar.Type,
			)
		case req.User.AvatarURL != nil:
			if strings.TrimSpace(*req.User.AvatarURL) == "" {
				updatedUser, err = userService.ClearUserAvatar(c.Context(), targetUser.ID)
			} else {
				return badRequest(c, "avatarUrl update is not supported; use avatar content upload")
			}
		default:
			return badRequest(c, "avatar or avatarUrl is required")
		}
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(toAPIUser(updatedUser))
	})

}
