package http

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

func registerMemoRoutes(
	api fiber.Router,
	memoService *service.MemoService,
	buildAPIMemo func(service.MemoWithAttachments) apiMemo,
) {
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

	api.Get("/memos/changes", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		filter := c.Query("filter", "")

		sinceRaw := strings.TrimSpace(c.Query("since"))
		if sinceRaw == "" {
			return badRequest(c, "since is required")
		}
		since, err := time.Parse(time.RFC3339Nano, sinceRaw)
		if err != nil {
			return badRequest(c, "invalid since")
		}

		var state *models.MemoState
		stateRaw := strings.TrimSpace(c.Query("state"))
		if stateRaw != "" {
			s := models.MemoState(stateRaw)
			if !s.IsValid() {
				return badRequest(c, "invalid state")
			}
			state = &s
		}

		syncAnchor := time.Now().UTC()
		changes, err := memoService.ListMemoChanges(
			c.Context(),
			currentUser.ID,
			state,
			filter,
			since,
			syncAnchor,
		)
		if err != nil {
			return badRequest(c, err.Error())
		}

		resp := listMemoChangesResponse{
			Memos:            make([]apiMemo, 0, len(changes.Memos)),
			DeletedMemoNames: changes.DeletedMemoNames,
			SyncAnchor:       changes.SyncAnchor.Format(time.RFC3339Nano),
		}
		for _, item := range changes.Memos {
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

}
