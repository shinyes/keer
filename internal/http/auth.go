package http

import (
	"database/sql"
	"errors"
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
			return writeError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "missing authorization")
		}
		if !strings.HasPrefix(strings.ToLower(authz), "bearer ") {
			return writeError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "invalid authorization header")
		}

		user, err := authenticateBearerUser(c, userService)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return writeError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "invalid access token")
			}
			return writeError(c, fiber.StatusInternalServerError, "INTERNAL_ERROR", "failed to authenticate")
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
	user, err := authenticateBearerUser(c, userService)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func authenticateBearerUser(c *fiber.Ctx, userService *service.UserService) (models.User, error) {
	authz := strings.TrimSpace(c.Get("Authorization"))
	if authz == "" {
		return models.User{}, sql.ErrNoRows
	}
	if !strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return models.User{}, sql.ErrNoRows
	}
	token := strings.TrimSpace(authz[len("Bearer "):])
	if token == "" {
		return models.User{}, sql.ErrNoRows
	}

	user, err := userService.AuthenticateAccessToken(c.Context(), token)
	if err == nil {
		return user, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return models.User{}, err
	}
	return userService.AuthenticateToken(c.Context(), token)
}
