package http

import (
	"database/sql"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

const currentUserKey = "currentUser"

func AuthMiddleware(userService *service.UserService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authz := strings.TrimSpace(c.Get("Authorization"))
		if authz == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"message": "missing authorization",
			})
		}

		if !strings.HasPrefix(strings.ToLower(authz), "bearer ") {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"message": "invalid authorization header",
			})
		}
		token := strings.TrimSpace(authz[len("Bearer "):])
		user, err := userService.AuthenticateToken(c.Context(), token)
		if err != nil {
			if err == sql.ErrNoRows {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"message": "invalid access token",
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "failed to authenticate",
			})
		}
		c.Locals(currentUserKey, user)
		return c.Next()
	}
}

func CurrentUser(c *fiber.Ctx) models.User {
	raw := c.Locals(currentUserKey)
	if raw == nil {
		return models.User{}
	}
	user, _ := raw.(models.User)
	return user
}

func OptionalAuthenticateToken(c *fiber.Ctx, userService *service.UserService) (*models.User, error) {
	authz := strings.TrimSpace(c.Get("Authorization"))
	if authz == "" {
		return nil, nil
	}
	if !strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return nil, sql.ErrNoRows
	}
	token := strings.TrimSpace(authz[len("Bearer "):])
	user, err := userService.AuthenticateToken(c.Context(), token)
	if err != nil {
		return nil, err
	}
	return &user, nil
}
