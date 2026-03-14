package http

import (
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/service"
)

func registerAdminRoutes(
	api fiber.Router,
	attachmentService *service.AttachmentService,
) {
	api.Post("/admin/storage/cleanup-orphans", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		if !isAdminRole(currentUser.Role) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "forbidden"})
		}

		result, err := attachmentService.CleanupOrphanFiles(c.Context())
		if err != nil {
			return internalError(c, err)
		}

		return c.JSON(storageCleanupResponse{
			Cleanup: storageCleanupResult{
				ScannedKeys: result.ScannedKeys,
				DeletedKeys: result.DeletedKeys,
				FailedKeys:  result.FailedKeys,
			},
		})
	})
}

func isAdminRole(role string) bool {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "HOST", "ADMIN":
		return true
	default:
		return false
	}
}
