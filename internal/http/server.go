package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"mime"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

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
	app.Use(httpAccessLogMiddleware())
	app.Use(cors.New())

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

	api.Post("/attachments/uploads", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		var req createAttachmentUploadSessionRequest
		if err := c.BodyParser(&req); err != nil {
			return badRequest(c, "invalid request body")
		}

		session, err := attachmentService.CreateAttachmentUploadSession(
			c.Context(),
			currentUser.ID,
			service.CreateAttachmentUploadSessionInput{
				Filename: req.Filename,
				Type:     req.Type,
				Size:     req.Size,
				MemoName: req.Memo,
			},
		)
		if err != nil {
			return badRequest(c, err.Error())
		}

		c.Set("Upload-Offset", models.Int64ToString(session.ReceivedSize))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
		return c.Status(fiber.StatusCreated).JSON(toAttachmentUploadSessionResponse(session))
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
		c.Set("Upload-Offset", models.Int64ToString(session.ReceivedSize))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
		return c.SendStatus(fiber.StatusNoContent)
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
			return internalError(c, err)
		}

		c.Set("Upload-Offset", models.Int64ToString(session.ReceivedSize))
		c.Set("Upload-Length", models.Int64ToString(session.Size))
		c.Set("Upload-Id", session.ID)
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
		return c.JSON(toAPIAttachment(attachment, ""))
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
		log.Printf("http request method=%s path=%s status=%d duration=%s ip=%s", c.Method(), path, status, time.Since(startedAt).Round(time.Millisecond), c.IP())
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

func toAttachmentUploadSessionResponse(session models.AttachmentUploadSession) attachmentUploadSessionResponse {
	return attachmentUploadSessionResponse{
		UploadID:     session.ID,
		Filename:     session.Filename,
		Type:         session.Type,
		Size:         models.Int64ToString(session.Size),
		UploadedSize: models.Int64ToString(session.ReceivedSize),
		Memo:         session.MemoName,
	}
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
	log.Printf("internal error method=%s path=%s err=%v", c.Method(), c.Path(), err)
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"message": "internal server error",
	})
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
