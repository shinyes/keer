package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"mime"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

func NewRouter(cfg config.Config, userService *service.UserService, memoService *service.MemoService, attachmentService *service.AttachmentService) *fiber.App {
	bodyLimit := cfg.BodyLimitMB * 1024 * 1024
	if bodyLimit <= 0 {
		bodyLimit = 64 * 1024 * 1024
	}
	app := fiber.New(fiber.Config{
		BodyLimit: bodyLimit,
	})
	app.Use(recover.New())
	app.Use(requestid.New(requestid.Config{
		Header: "X-Request-ID",
	}))
	app.Use(httpAccessLogMiddleware())
	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.BaseURL,
	}))
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
		Next: func(c *fiber.Ctx) bool {
			return strings.HasPrefix(c.Path(), "/file/")
		},
	}))

	buildAPIAttachment := func(attachment models.Attachment, memoName string) apiAttachment {
		return toAPIAttachment(attachment, memoName, "", "")
	}

	buildAPIMemo := func(memo service.MemoWithAttachments) apiMemo {
		return toAPIMemo(memo, func(attachment models.Attachment, memoName string) apiAttachment {
			return buildAPIAttachment(attachment, memoName)
		})
	}

	app.Get("/api/v1/instance/profile", func(c *fiber.Ctx) error {
		return c.JSON(profileResponse{
			KeerAPIVersion: cfg.KeerAPIVersion,
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
			if errors.Is(err, sql.ErrNoRows) {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"message": "invalid access token",
				})
			}
			return internalError(c, fmt.Errorf("authenticate optional token: %w", err))
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
			resp.Memos = append(resp.Memos, buildAPIMemo(item))
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
		var createTime *time.Time
		if req.CreateTime != nil {
			if t, err := time.Parse(time.RFC3339Nano, *req.CreateTime); err == nil {
				createTime = &t
			}
		}
		created, err := memoService.CreateMemo(
			c.Context(),
			currentUser.ID,
			service.CreateMemoInput{
				Content:         req.Content,
				Visibility:      visibility,
				Tags:            req.Tags,
				AttachmentNames: attachmentNames,
				CreateTime:      createTime,
				Latitude:        req.Latitude,
				Longitude:       req.Longitude,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(buildAPIMemo(created))
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
				Tags:            req.Tags,
				State:           state,
				Pinned:          req.Pinned,
				AttachmentNames: attachmentNames,
				LatitudeSet:     req.Latitude.Set,
				Latitude:        req.Latitude.Value,
				LongitudeSet:    req.Longitude.Set,
				Longitude:       req.Longitude.Value,
			},
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "memo not found")
			}
			return badRequest(c, err.Error())
		}
		return c.JSON(buildAPIMemo(updated))
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
			resp.Attachments = append(resp.Attachments, buildAPIAttachment(attachment, ""))
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
		return c.JSON(buildAPIAttachment(attachment, ""))
	})

	api.Post("/attachments/uploads", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createAttachmentUploadSessionRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		var thumbnail *service.CreateAttachmentUploadSessionThumbnailInput
		if req.Thumbnail != nil {
			thumbnail = &service.CreateAttachmentUploadSessionThumbnailInput{
				Filename: req.Thumbnail.Filename,
				Type:     req.Thumbnail.Type,
				Content:  req.Thumbnail.Content,
			}
		}

		session, err := attachmentService.CreateAttachmentUploadSession(
			c.Context(),
			currentUser.ID,
			service.CreateAttachmentUploadSessionInput{
				Filename:  req.Filename,
				Type:      req.Type,
				Size:      req.Size,
				MemoName:  req.Memo,
				Thumbnail: thumbnail,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		progress, err := attachmentService.GetAttachmentUploadSessionProgress(c.Context(), session)
		if err != nil {
			return internalError(c, err)
		}
		directUploadSession, err := attachmentService.GetDirectUploadSession(c.Context(), session)
		if err != nil {
			return internalError(c, err)
		}
		multipartSession, err := attachmentService.GetMultipartUploadPartSession(session)
		if err != nil {
			return internalError(c, err)
		}

		c.Set("Upload-Offset", models.Int64ToString(progress))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
		if multipartSession != nil {
			c.Set("Upload-Mode", "DIRECT_MULTIPART")
			c.Set("Upload-Part-Size", models.Int64ToString(multipartSession.PartSize))
		} else if directUploadSession != nil {
			c.Set("Upload-Mode", "DIRECT_PRESIGNED_PUT")
		} else {
			c.Set("Upload-Mode", "RESUMABLE")
		}
		return c.Status(fiber.StatusCreated).JSON(toAttachmentUploadSessionResponse(session, progress, directUploadSession, multipartSession))
	})

	api.Head("/attachments/uploads/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		uploadID := strings.TrimSpace(c.Params("id"))
		if uploadID == "" {
			return badRequest(c, "invalid upload id")
		}

		session, err := attachmentService.GetAttachmentUploadSession(c.Context(), currentUser.ID, uploadID)
		if err != nil {
			if errors.Is(err, service.ErrUploadSessionNotFound) || errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "upload session not found")
			}
			return internalError(c, err)
		}
		progress, err := attachmentService.GetAttachmentUploadSessionProgress(c.Context(), session)
		if err != nil {
			return internalError(c, err)
		}
		c.Set("Upload-Offset", models.Int64ToString(progress))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
		multipartSession, err := attachmentService.GetMultipartUploadPartSession(session)
		if err != nil {
			return internalError(c, err)
		}
		if multipartSession != nil {
			c.Set("Upload-Mode", "DIRECT_MULTIPART")
			c.Set("Upload-Part-Size", models.Int64ToString(multipartSession.PartSize))
		} else if attachmentService.IsDirectUploadSession(session) {
			c.Set("Upload-Mode", "DIRECT_PRESIGNED_PUT")
		} else {
			c.Set("Upload-Mode", "RESUMABLE")
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Get("/attachments/uploads/:id/parts/:partNumber", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		uploadID := strings.TrimSpace(c.Params("id"))
		if uploadID == "" {
			return badRequest(c, "invalid upload id")
		}
		partNumberRaw := strings.TrimSpace(c.Params("partNumber"))
		partNumber64, err := strconv.ParseInt(partNumberRaw, 10, 32)
		if err != nil || partNumber64 <= 0 {
			return badRequest(c, "invalid part number")
		}
		expectedOffset, err := parseNonNegativeInt64(c.Query("offset"))
		if err != nil {
			return badRequest(c, "invalid offset")
		}
		requestedSize, err := parseNonNegativeInt64(c.Query("size"))
		if err != nil || requestedSize <= 0 {
			return badRequest(c, "invalid size")
		}

		session, err := attachmentService.GetAttachmentUploadSession(c.Context(), currentUser.ID, uploadID)
		if err != nil {
			if errors.Is(err, service.ErrUploadSessionNotFound) || errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "upload session not found")
			}
			return internalError(c, err)
		}
		multipartUploadURL, err := attachmentService.CreateMultipartPartUploadURL(
			c.Context(),
			session,
			expectedOffset,
			int32(partNumber64),
			requestedSize,
		)
		if err != nil {
			var mismatch *service.UploadOffsetMismatchError
			if errors.As(err, &mismatch) {
				c.Set("Upload-Offset", models.Int64ToString(mismatch.CurrentOffset))
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{
					"message":       "upload offset mismatch",
					"currentOffset": models.Int64ToString(mismatch.CurrentOffset),
				})
			}
			if errors.Is(err, service.ErrMultipartPartInvalid) || errors.Is(err, service.ErrUploadExceedsTotalSize) {
				return badRequest(c, err.Error())
			}
			if errors.Is(err, service.ErrUploadNotComplete) || errors.Is(err, service.ErrUploadChunkUnsupported) {
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{
					"message": err.Error(),
				})
			}
			return internalError(c, err)
		}
		if multipartUploadURL == nil {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"message": "upload session is not multipart mode",
			})
		}
		return c.JSON(attachmentMultipartPartUploadResponse{
			UploadID:   session.ID,
			PartNumber: multipartUploadURL.PartNumber,
			Offset:     models.Int64ToString(multipartUploadURL.Offset),
			Size:       models.Int64ToString(multipartUploadURL.Size),
			UploadURL:  multipartUploadURL.UploadURL,
			Method:     multipartUploadURL.Method,
		})
	})

	api.Patch("/attachments/uploads/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		uploadID := strings.TrimSpace(c.Params("id"))
		if uploadID == "" {
			return badRequest(c, "invalid upload id")
		}

		expectedOffset, err := parseNonNegativeInt64(c.Get("Upload-Offset"))
		if err != nil {
			return badRequest(c, "invalid Upload-Offset header")
		}
		chunk := c.Body()

		session, err := attachmentService.AppendAttachmentUploadChunk(
			c.Context(),
			currentUser.ID,
			uploadID,
			expectedOffset,
			chunk,
		)
		if err != nil {
			var mismatch *service.UploadOffsetMismatchError
			if errors.As(err, &mismatch) {
				c.Set("Upload-Offset", models.Int64ToString(mismatch.CurrentOffset))
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{
					"message":       "upload offset mismatch",
					"currentOffset": models.Int64ToString(mismatch.CurrentOffset),
				})
			}
			if errors.Is(err, service.ErrUploadSessionNotFound) || errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "upload session not found")
			}
			if errors.Is(err, service.ErrUploadExceedsTotalSize) {
				return badRequest(c, err.Error())
			}
			if errors.Is(err, service.ErrUploadChunkUnsupported) {
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{
					"message": "upload chunk is not supported for this upload session",
				})
			}
			return internalError(c, err)
		}

		c.Set("Upload-Offset", models.Int64ToString(session.ReceivedSize))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
		c.Set("Upload-Mode", "RESUMABLE")
		return c.SendStatus(fiber.StatusNoContent)
	})

	api.Post("/attachments/uploads/:id/complete", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		uploadID := strings.TrimSpace(c.Params("id"))
		if uploadID == "" {
			return badRequest(c, "invalid upload id")
		}

		attachment, err := attachmentService.CompleteAttachmentUploadSession(c.Context(), currentUser.ID, uploadID)
		if err != nil {
			if errors.Is(err, service.ErrUploadSessionNotFound) || errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "upload session not found")
			}
			if errors.Is(err, service.ErrUploadNotComplete) {
				return c.Status(fiber.StatusConflict).JSON(fiber.Map{
					"message": "upload not complete",
				})
			}
			return internalError(c, err)
		}
		return c.JSON(buildAPIAttachment(attachment, ""))
	})

	api.Delete("/attachments/uploads/:id", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		uploadID := strings.TrimSpace(c.Params("id"))
		if uploadID == "" {
			return badRequest(c, "invalid upload id")
		}

		err := attachmentService.CancelAttachmentUploadSession(c.Context(), currentUser.ID, uploadID)
		if err != nil {
			if errors.Is(err, service.ErrUploadSessionNotFound) || errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "upload session not found")
			}
			return internalError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
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

	app.Get("/file/attachments/:id/thumbnail/:filename", AuthMiddleware(userService), func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachmentID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid attachment id")
		}

		attachment, err := attachmentService.GetAttachment(c.Context(), attachmentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "attachment not found")
			}
			return internalError(c, err)
		}

		if attachment.CreatorID != currentUser.ID {
			return c.SendStatus(fiber.StatusForbidden)
		}
		if strings.TrimSpace(attachment.ThumbnailStorageKey) == "" {
			return notFound(c, "thumbnail not found")
		}
		if directURL, ok, err := attachmentService.PresignAttachmentThumbnailURL(c.Context(), attachment); err != nil {
			return internalError(c, err)
		} else if ok {
			return c.Redirect(directURL, fiber.StatusTemporaryRedirect)
		}

		thumbnailStream, err := attachmentService.OpenAttachmentThumbnailStream(c.Context(), attachment)
		if err != nil {
			return notFound(c, "thumbnail not found")
		}

		thumbnailType := strings.TrimSpace(attachment.ThumbnailType)
		if thumbnailType == "" {
			thumbnailType = "image/jpeg"
		}
		thumbnailFilename := strings.TrimSpace(attachment.ThumbnailFilename)
		if thumbnailFilename == "" {
			thumbnailFilename = attachment.Filename
		}
		c.Set(fiber.HeaderContentType, thumbnailType)
		c.Set(fiber.HeaderContentDisposition, inlineContentDisposition(thumbnailFilename))
		if attachment.ThumbnailSize > 0 {
			c.Set(fiber.HeaderContentLength, models.Int64ToString(attachment.ThumbnailSize))
			return c.SendStream(thumbnailStream, int(attachment.ThumbnailSize))
		}
		return c.SendStream(thumbnailStream)
	})

	app.Get("/file/avatars/:id", AuthMiddleware(userService), func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		userID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid user id")
		}
		if userID != currentUser.ID {
			return c.SendStatus(fiber.StatusForbidden)
		}

		user, err := userService.GetUser(c.Context(), userID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "user not found")
			}
			return internalError(c, err)
		}
		if strings.TrimSpace(user.AvatarURL) == "" {
			return notFound(c, "avatar not found")
		}

		if directURL, ok, err := userService.PresignUserAvatarURL(c.Context(), userID); err != nil {
			return internalError(c, err)
		} else if ok {
			return c.Redirect(directURL, fiber.StatusTemporaryRedirect)
		}

		avatarStream, err := userService.OpenUserAvatarStream(c.Context(), userID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return notFound(c, "avatar not found")
			}
			return internalError(c, err)
		}
		c.Set(fiber.HeaderContentType, "image/jpeg")
		c.Set(fiber.HeaderContentDisposition, inlineContentDisposition(fmt.Sprintf("%d.jpg", userID)))
		return c.SendStream(avatarStream)
	})

	app.Get("/file/attachments/:id/:filename", AuthMiddleware(userService), func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachmentID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid attachment id")
		}

		attachment, err := attachmentService.GetAttachment(c.Context(), attachmentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return notFound(c, "attachment not found")
			}
			return internalError(c, err)
		}

		if attachment.CreatorID != currentUser.ID {
			return c.SendStatus(fiber.StatusForbidden)
		}
		if directURL, ok, err := attachmentService.PresignAttachmentURL(c.Context(), attachment); err != nil {
			return internalError(c, err)
		} else if ok {
			return c.Redirect(directURL, fiber.StatusTemporaryRedirect)
		}

		start, end, hasRange, err := parseSingleByteRange(c.Get(fiber.HeaderRange), attachment.Size)
		if err != nil {
			c.Set(fiber.HeaderAcceptRanges, "bytes")
			c.Set(fiber.HeaderContentRange, fmt.Sprintf("bytes */%d", attachment.Size))
			return c.SendStatus(fiber.StatusRequestedRangeNotSatisfiable)
		}

		c.Set(fiber.HeaderAcceptRanges, "bytes")
		c.Set(fiber.HeaderContentType, attachment.Type)
		c.Set(fiber.HeaderContentDisposition, inlineContentDisposition(attachment.Filename))

		if hasRange {
			rangedStream, err := attachmentService.OpenAttachmentRangeStream(c.Context(), attachment, start, end)
			if err != nil {
				return internalError(c, err)
			}

			length := end - start + 1
			c.Set(fiber.HeaderContentRange, fmt.Sprintf("bytes %d-%d/%d", start, end, attachment.Size))
			c.Set(fiber.HeaderContentLength, models.Int64ToString(length))
			c.Status(fiber.StatusPartialContent)
			return c.SendStream(rangedStream, int(length))
		}

		rc, err := attachmentService.OpenAttachmentStream(c.Context(), attachment)
		if err != nil {
			return internalError(c, err)
		}
		// Do not close rc here. Fiber/fasthttp sends the stream after the handler
		// returns, and early close can truncate the response on the client side.
		c.Set(fiber.HeaderContentLength, models.Int64ToString(attachment.Size))
		return c.SendStream(rc, int(attachment.Size))
	})

	return app
}

func httpAccessLogMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		startedAt := time.Now()
		err := c.Next()

		status := c.Response().StatusCode()
		if err != nil {
			var fiberErr *fiber.Error
			if errors.As(err, &fiberErr) {
				status = fiberErr.Code
			} else if status < fiber.StatusBadRequest {
				status = fiber.StatusInternalServerError
			}
		}
		if status == 0 {
			status = fiber.StatusOK
		}

		path := strings.TrimSpace(c.OriginalURL())
		if path == "" {
			path = c.Path()
		}
		log.Printf("http request method=%s path=%s status=%d duration=%s ip=%s request_id=%s", c.Method(), path, status, time.Since(startedAt).Round(time.Millisecond), c.IP(), requestID(c))
		return err
	}
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
		AvatarURL:   user.AvatarURL,
		State:       "NORMAL",
		CreateTime:  formatMaybeTime(user.CreateTime),
		UpdateTime:  formatMaybeTime(user.UpdateTime),
	}
}

func toAPIMemo(
	memo service.MemoWithAttachments,
	attachmentMapper func(attachment models.Attachment, memoName string) apiAttachment,
) apiMemo {
	attachments := make([]apiAttachment, 0, len(memo.Attachments))
	for _, attachment := range memo.Attachments {
		if attachmentMapper != nil {
			attachments = append(attachments, attachmentMapper(attachment, memo.Memo.Name()))
			continue
		}
		attachments = append(attachments, toAPIAttachment(attachment, memo.Memo.Name(), "", ""))
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
		Content:     memo.Memo.Content,
		Visibility:  string(memo.Memo.Visibility),
		Pinned:      memo.Memo.Pinned,
		Latitude:    memo.Memo.Latitude,
		Longitude:   memo.Memo.Longitude,
		Attachments: attachments,
		Tags:        tags,
	}
}

func toAPIAttachment(attachment models.Attachment, memoName string, directLink string, directThumbnailLink string) apiAttachment {
	thumbnailName := ""
	if strings.TrimSpace(attachment.ThumbnailStorageKey) != "" {
		thumbnailName = "attachments/" + models.Int64ToString(attachment.ID) + "/thumbnail"
	}
	externalLink := strings.TrimSpace(directLink)
	if externalLink == "" {
		externalLink = strings.TrimSpace(attachment.ExternalLink)
	}
	thumbnailExternalLink := strings.TrimSpace(directThumbnailLink)
	return apiAttachment{
		Name:                  "attachments/" + models.Int64ToString(attachment.ID),
		CreateTime:            formatTime(attachment.CreateTime),
		Filename:              attachment.Filename,
		ExternalLink:          externalLink,
		Type:                  attachment.Type,
		Size:                  models.Int64ToString(attachment.Size),
		ThumbnailName:         thumbnailName,
		ThumbnailExternalLink: thumbnailExternalLink,
		ThumbnailFilename:     attachment.ThumbnailFilename,
		ThumbnailType:         attachment.ThumbnailType,
		Memo:                  memoName,
	}
}

func toAttachmentUploadSessionResponse(
	session models.AttachmentUploadSession,
	uploadedSize int64,
	directUpload *service.DirectUploadSession,
	multipart *service.MultipartUploadPartSession,
) attachmentUploadSessionResponse {
	resp := attachmentUploadSessionResponse{
		UploadID:     session.ID,
		Filename:     session.Filename,
		Type:         session.Type,
		Size:         models.Int64ToString(session.Size),
		UploadedSize: models.Int64ToString(uploadedSize),
		Memo:         session.MemoName,
	}
	if multipart != nil {
		resp.UploadMode = "DIRECT_MULTIPART"
		resp.MultipartPartSize = models.Int64ToString(multipart.PartSize)
		return resp
	}
	if directUpload != nil {
		resp.UploadMode = "DIRECT_PRESIGNED_PUT"
		resp.DirectUploadURL = directUpload.UploadURL
		resp.DirectUploadMethod = directUpload.Method
		return resp
	}
	resp.UploadMode = "RESUMABLE"
	return resp
}

func parseID(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty id")
	}
	return strconv.ParseInt(raw, 10, 64)
}

func parseNonNegativeInt64(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty integer")
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid integer")
	}
	return v, nil
}

func parseSingleByteRange(raw string, size int64) (start int64, end int64, hasRange bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, false, nil
	}
	hasRange = true

	if size <= 0 {
		return 0, 0, true, fmt.Errorf("invalid resource size")
	}
	if !strings.HasPrefix(raw, "bytes=") {
		return 0, 0, true, fmt.Errorf("unsupported range unit")
	}

	spec := strings.TrimSpace(strings.TrimPrefix(raw, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return 0, 0, true, fmt.Errorf("invalid range")
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, true, fmt.Errorf("invalid range")
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])

	if left == "" {
		// Suffix-byte-range-spec: bytes=-N
		suffixLength, parseErr := strconv.ParseInt(right, 10, 64)
		if parseErr != nil || suffixLength <= 0 {
			return 0, 0, true, fmt.Errorf("invalid suffix range")
		}
		if suffixLength > size {
			suffixLength = size
		}
		return size - suffixLength, size - 1, true, nil
	}

	rangeStart, parseErr := strconv.ParseInt(left, 10, 64)
	if parseErr != nil || rangeStart < 0 {
		return 0, 0, true, fmt.Errorf("invalid range start")
	}
	if rangeStart >= size {
		return 0, 0, true, fmt.Errorf("range start out of bounds")
	}

	if right == "" {
		return rangeStart, size - 1, true, nil
	}

	rangeEnd, parseErr := strconv.ParseInt(right, 10, 64)
	if parseErr != nil || rangeEnd < rangeStart {
		return 0, 0, true, fmt.Errorf("invalid range end")
	}
	if rangeEnd >= size {
		rangeEnd = size - 1
	}
	return rangeStart, rangeEnd, true, nil
}

func badRequest(c *fiber.Ctx, message string) error {
	return writeError(c, fiber.StatusBadRequest, "BAD_REQUEST", message)
}

func notFound(c *fiber.Ctx, message string) error {
	return writeError(c, fiber.StatusNotFound, "NOT_FOUND", message)
}

func internalError(c *fiber.Ctx, err error) error {
	log.Printf("internal error method=%s path=%s request_id=%s err=%v", c.Method(), c.Path(), requestID(c), err)
	return writeError(c, fiber.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
}

func writeError(c *fiber.Ctx, status int, code string, message string) error {
	return c.Status(status).JSON(fiber.Map{
		"code":      code,
		"message":   message,
		"requestId": requestID(c),
	})
}

func requestID(c *fiber.Ctx) string {
	if id := strings.TrimSpace(c.GetRespHeader("X-Request-ID")); id != "" {
		return id
	}
	if raw := c.Locals("requestid"); raw != nil {
		if id, ok := raw.(string); ok && strings.TrimSpace(id) != "" {
			return strings.TrimSpace(id)
		}
	}
	return ""
}

func inlineContentDisposition(filename string) string {
	filename = sanitizeContentDispositionFilename(filename)
	if filename == "" {
		return "inline"
	}
	value := mime.FormatMediaType("inline", map[string]string{"filename": filename})
	if value == "" {
		return "inline"
	}
	return value
}

func sanitizeContentDispositionFilename(filename string) string {
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return ""
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '"' || r == '\\' || r == ';' {
			return '_'
		}
		return r
	}, filename)
}
