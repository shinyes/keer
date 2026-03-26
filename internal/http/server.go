package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

func NewRouter(
	cfg config.Config,
	sqlStore *store.SQLStore,
	userService *service.UserService,
	memoService *service.MemoService,
	groupService *service.GroupService,
	attachmentService *service.AttachmentService,
) *fiber.App {
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
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
		Next: func(c *fiber.Ctx) bool {
			return strings.HasPrefix(c.Path(), "/file/")
		},
	}))

	buildAPIAttachment := func(attachment models.Attachment, memoName string) apiAttachment {
		return toAPIAttachment(attachment, memoName, "", "", false)
	}

	buildAPIMemo := func(memo service.MemoWithAttachments) apiMemo {
		return toAPIMemo(memo, func(attachment models.Attachment, memoName string) apiAttachment {
			return toAPIAttachment(attachment, memoName, "", "", true)
		})
	}
	pullProcessor := newSyncPullProcessor(sqlStore, userService, memoService, groupService, buildAPIMemo)

	app.Post("/api/v1/auth/signin", func(c *fiber.Ctx) error {
		var req signInRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if req.PasswordCredentials == nil {
			return badRequest(c, "passwordCredentials is required")
		}

		user, tokens, err := userService.SignInWithPassword(
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
			User:                  toAPIUser(user),
			AccessToken:           tokens.AccessToken,
			AccessTokenExpiresAt:  formatTime(tokens.AccessTokenExpiresAt),
			RefreshToken:          tokens.RefreshToken,
			RefreshTokenExpiresAt: formatTime(tokens.RefreshTokenExpiresAt),
		})
	})

	app.Post("/api/v1/auth/refresh", func(c *fiber.Ctx) error {
		var req refreshSessionRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if strings.TrimSpace(req.RefreshToken) == "" {
			return badRequest(c, "refreshToken is required")
		}

		user, tokens, err := userService.RefreshSession(c.Context(), req.RefreshToken)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrInvalidRefreshToken):
				return writeError(c, fiber.StatusUnauthorized, "UNAUTHORIZED", "invalid refresh token")
			default:
				return internalError(c, err)
			}
		}

		return c.JSON(signInResponse{
			User:                  toAPIUser(user),
			AccessToken:           tokens.AccessToken,
			AccessTokenExpiresAt:  formatTime(tokens.AccessTokenExpiresAt),
			RefreshToken:          tokens.RefreshToken,
			RefreshTokenExpiresAt: formatTime(tokens.RefreshTokenExpiresAt),
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

		user, err := userService.CreateUser(c.Context(), creator, service.CreateUserInput{
			Username:     req.User.Username,
			Password:     req.User.Password,
			Role:         req.User.Role,
			ValidateOnly: req.ValidateOnly,
		}, cfg.AllowRegistration)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrInvalidUsername):
				return badRequest(c, "invalid username")
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
	registerUserRoutes(api, userService, memoService)
	registerMemoRoutes(api, memoService, buildAPIMemo)
	registerGroupRoutes(api, userService, groupService)
	registerAdminRoutes(api, attachmentService)
	apiV2 := app.Group("/api/v2", AuthMiddleware(userService))
	registerSyncStreamRoutes(apiV2, pullProcessor)

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
		if strings.TrimSpace(req.DescriptorCiphertext) == "" {
			return badRequest(c, "descriptorCiphertext is required")
		}
		if err := validatePayloadEnvelope(req.DescriptorEnvelope); err != nil {
			return badRequest(c, "invalid descriptorEnvelope")
		}
		if strings.TrimSpace(req.BlobEncryption) == "" {
			return badRequest(c, "blobEncryption is required")
		}
		encryptionMetadata, err := marshalAttachmentEncryptionMetadata(
			req.DescriptorCiphertext,
			req.DescriptorEnvelope,
			req.BlobEncryption,
			req.ThumbnailBlobEncryption,
		)
		if err != nil {
			return badRequest(c, "invalid attachment encryption metadata")
		}
		attachment, err := attachmentService.CreateAttachment(
			c.Context(),
			currentUser.ID,
			service.CreateAttachmentInput{
				Filename:           req.Filename,
				Type:               req.Type,
				Content:            req.Content,
				EncryptionMetadata: encryptionMetadata,
				MemoName:           req.Memo,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}
		return c.JSON(buildAPIAttachment(attachment, ""))
	})

	api.Post("/attachments/:id/thumbnail", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		attachmentID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid attachment id")
		}

		var req updateAttachmentThumbnailRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		attachment, err := attachmentService.UpdateAttachmentThumbnail(
			c.Context(),
			currentUser.ID,
			attachmentID,
			service.UpdateAttachmentThumbnailInput{
				Filename:                req.Filename,
				Type:                    req.Type,
				Content:                 req.Content,
				ThumbnailBlobEncryption: req.ThumbnailBlobEncryption,
			},
		)
		if err != nil {
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return notFound(c, "attachment not found")
			case errors.Is(err, service.ErrAttachmentPermissionDenied):
				return c.SendStatus(fiber.StatusForbidden)
			case errors.Is(err, service.ErrInvalidAttachmentThumbnail):
				return badRequest(c, err.Error())
			default:
				return internalError(c, err)
			}
		}
		return c.JSON(buildAPIAttachment(attachment, ""))
	})

	api.Post("/attachments/uploads", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createAttachmentUploadSessionRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}
		if strings.TrimSpace(req.DescriptorCiphertext) == "" {
			return badRequest(c, "descriptorCiphertext is required")
		}
		if err := validatePayloadEnvelope(req.DescriptorEnvelope); err != nil {
			return badRequest(c, "invalid descriptorEnvelope")
		}
		if strings.TrimSpace(req.BlobEncryption) == "" {
			return badRequest(c, "blobEncryption is required")
		}
		var thumbnail *service.CreateAttachmentUploadSessionThumbnailInput
		if req.Thumbnail != nil {
			thumbnail = &service.CreateAttachmentUploadSessionThumbnailInput{
				Filename: req.Thumbnail.Filename,
				Type:     req.Thumbnail.Type,
				Content:  req.Thumbnail.Content,
			}
		}
		encryptionMetadata, err := marshalAttachmentEncryptionMetadata(
			req.DescriptorCiphertext,
			req.DescriptorEnvelope,
			req.BlobEncryption,
			req.ThumbnailBlobEncryption,
		)
		if err != nil {
			return badRequest(c, "invalid attachment encryption metadata")
		}

		session, err := attachmentService.CreateAttachmentUploadSession(
			c.Context(),
			currentUser.ID,
			service.CreateAttachmentUploadSessionInput{
				Filename:           req.Filename,
				Type:               req.Type,
				Size:               req.Size,
				EncryptionMetadata: encryptionMetadata,
				MemoName:           req.Memo,
				Thumbnail:          thumbnail,
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

		allowed, err := attachmentService.AttachmentVisibleToUser(c.Context(), attachmentID, currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}
		if !allowed {
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
		userID, err := parseID(c.Params("id"))
		if err != nil {
			return badRequest(c, "invalid user id")
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

		allowed, err := attachmentService.AttachmentVisibleToUser(c.Context(), attachmentID, currentUser.ID)
		if err != nil {
			return internalError(c, err)
		}
		if !allowed {
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
