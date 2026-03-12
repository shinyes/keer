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

	api.Get("/friends", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		friends, err := userService.ListFriends(c.Context(), currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}
		resp := listUsersResponse{
			Users: make([]apiUser, 0, len(friends)),
		}
		for _, friend := range friends {
			resp.Users = append(resp.Users, toAPIUserSync(friend))
		}
		return c.JSON(resp)
	})

	api.Post("/friends", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req addFriendRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		friend, err := userService.AddFriend(c.Context(), currentUser.ID, req.User)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrCannotFriendSelf):
				return badRequest(c, err.Error())
			case errors.Is(err, service.ErrFriendNotFound):
				return notFound(c, "user not found")
			default:
				return internalError(c, err)
			}
		}
		return c.JSON(toAPIUserSync(friend))
	})

	api.Delete("/friends/:name", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		name := strings.TrimSpace(c.Params("name"))
		if name == "" {
			return badRequest(c, "invalid user name")
		}
		if err := userService.RemoveFriend(c.Context(), currentUser.ID, name); err != nil {
			switch {
			case errors.Is(err, service.ErrCannotFriendSelf):
				return badRequest(c, err.Error())
			case errors.Is(err, service.ErrFriendNotFound):
				return notFound(c, "friend not found")
			default:
				return internalError(c, err)
			}
		}
		return c.SendStatus(fiber.StatusNoContent)
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

	api.Get("/users/:name/settings/ENCRYPTION", func(c *fiber.Ctx) error {
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

		encryptionKey, err := userService.GetUserEncryptionKey(c.Context(), user.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "encryption setting not found")
			}
			return internalError(c, err)
		}
		return c.JSON(userEncryptionSettingResponse{
			EncryptionSetting: toAPIUserEncryptionSetting(encryptionKey),
		})
	})

	api.Put("/users/:name/settings/ENCRYPTION", func(c *fiber.Ctx) error {
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

		var req updateUserEncryptionSettingRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		encryptionKey, err := userService.UpsertUserEncryptionKey(c.Context(), user.ID, service.UpsertUserEncryptionKeyInput{
			Version:                  req.EncryptionSetting.RecoveryBundle.Version,
			KDFAlgorithm:             req.EncryptionSetting.RecoveryBundle.KDFAlgorithm,
			KDFSalt:                  req.EncryptionSetting.RecoveryBundle.KDFSalt,
			KDFTimeCost:              req.EncryptionSetting.RecoveryBundle.KDFTimeCost,
			KDFMemoryKiB:             req.EncryptionSetting.RecoveryBundle.KDFMemoryKiB,
			KDFParallelism:           req.EncryptionSetting.RecoveryBundle.KDFParallelism,
			WrapAlgorithm:            req.EncryptionSetting.RecoveryBundle.WrapAlgorithm,
			WrappedAccountKey:        req.EncryptionSetting.RecoveryBundle.WrappedAccountKey,
			SharingPublicKey:         req.EncryptionSetting.SharingPublicKey,
			WrappedSharingPrivateKey: req.EncryptionSetting.WrappedSharingPrivateKey,
			KeyVersion:               req.EncryptionSetting.KeyVersion,
			Algorithms:               req.EncryptionSetting.Algorithms,
		})
		if err != nil {
			return badRequest(c, err.Error())
		}

		return c.JSON(userEncryptionSettingResponse{
			EncryptionSetting: toAPIUserEncryptionSetting(encryptionKey),
		})
	})

	api.Put("/users/:name/password", func(c *fiber.Ctx) error {
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

		var req changeUserPasswordRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		_, err = userService.ChangePassword(c.Context(), user.ID, req.CurrentPassword, req.NewPassword, service.UpsertUserEncryptionKeyInput{
			Version:                  req.EncryptionSetting.RecoveryBundle.Version,
			KDFAlgorithm:             req.EncryptionSetting.RecoveryBundle.KDFAlgorithm,
			KDFSalt:                  req.EncryptionSetting.RecoveryBundle.KDFSalt,
			KDFTimeCost:              req.EncryptionSetting.RecoveryBundle.KDFTimeCost,
			KDFMemoryKiB:             req.EncryptionSetting.RecoveryBundle.KDFMemoryKiB,
			KDFParallelism:           req.EncryptionSetting.RecoveryBundle.KDFParallelism,
			WrapAlgorithm:            req.EncryptionSetting.RecoveryBundle.WrapAlgorithm,
			WrappedAccountKey:        req.EncryptionSetting.RecoveryBundle.WrappedAccountKey,
			SharingPublicKey:         req.EncryptionSetting.SharingPublicKey,
			WrappedSharingPrivateKey: req.EncryptionSetting.WrappedSharingPrivateKey,
			KeyVersion:               req.EncryptionSetting.KeyVersion,
			Algorithms:               req.EncryptionSetting.Algorithms,
		})
		if err != nil {
			switch {
			case errors.Is(err, service.ErrInvalidCurrentPassword):
				return badRequest(c, err.Error())
			case errors.Is(err, service.ErrInvalidPassword), errors.Is(err, service.ErrInvalidEncryptionKey):
				return badRequest(c, err.Error())
			default:
				return internalError(c, err)
			}
		}

		return c.SendStatus(fiber.StatusNoContent)
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

	api.Get("/users/keys/batch", func(c *fiber.Ctx) error {
		identifiers := parseBatchIdentifiers(c.Query("ids"))
		if len(identifiers) > 200 {
			return badRequest(c, "too many user ids")
		}

		resp := listUserPublicKeysResponse{
			Users: make([]apiUserPublicKey, 0, len(identifiers)),
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

			encryptionKey, err := userService.GetUserEncryptionKey(c.Context(), user.ID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return internalError(c, err)
			}
			if strings.TrimSpace(encryptionKey.SharingPublicKey) == "" {
				continue
			}

			resp.Users = append(resp.Users, apiUserPublicKey{
				Name:             user.Name(),
				SharingPublicKey: encryptionKey.SharingPublicKey,
				KeyVersion:       encryptionKey.KeyVersion,
			})
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
