package http

import (
	"bufio"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

func registerSyncStreamRoutes(api fiber.Router, pullProcessor syncPullProcessor) {
	api.Get("/sync/stream", func(c *fiber.Ctx) error {
		currentUser := CurrentUser(c)
		req := normalizeSyncStreamRequest(c)

		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")

		cursor := req.ResumeCursor
		if cursor == "" {
			cursor = "0"
		}
		sessionID := streamSessionID()

		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			if err := writeSSEEvent(w, "bootstrap_begin", syncStreamEventEnvelope{
				SessionID: sessionID,
				Cursor:    cursor,
			}); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}

			bootstrapEnded := false
			for {
				response, err := pullProcessor.Compute(
					c.Context(),
					currentUser,
					syncPullRequest{
						Cursor:      cursor,
						Domains:     req.Domains,
						GroupScopes: req.GroupScopes,
						Limit:       syncStreamChunkLimit,
					},
				)
				if err != nil {
					if writeErr := writeSSEEvent(w, "heartbeat", syncStreamEventEnvelope{
						SessionID: sessionID,
						Cursor:    cursor,
					}); writeErr != nil {
						return
					}
					if flushErr := w.Flush(); flushErr != nil {
						return
					}
					time.Sleep(syncStreamTailPollInterval)
					continue
				}

				cursor = strings.TrimSpace(response.NextCursor)
				if cursor == "" {
					cursor = "0"
				}

				if !isSyncPatchEmpty(response.Patches) {
					if err := writeSSEEvent(w, "patch", syncStreamEventEnvelope{
						SessionID: sessionID,
						Cursor:    cursor,
						Patches:   response.Patches,
					}); err != nil {
						return
					}
				}

				if err := writeSSEEvent(w, "checkpoint", syncStreamEventEnvelope{
					SessionID: sessionID,
					Cursor:    cursor,
				}); err != nil {
					return
				}

				if !response.HasMore && !bootstrapEnded {
					bootstrapEnded = true
					if err := writeSSEEvent(w, "bootstrap_end", syncStreamEventEnvelope{
						SessionID: sessionID,
						Cursor:    cursor,
					}); err != nil {
						return
					}
					if err := w.Flush(); err != nil {
						return
					}
					if req.Mode == "bootstrap" {
						return
					}
				} else if err := w.Flush(); err != nil {
					return
				}

				if !response.HasMore {
					if err := writeSSEEvent(w, "heartbeat", syncStreamEventEnvelope{
						SessionID: sessionID,
						Cursor:    cursor,
					}); err != nil {
						return
					}
					if err := w.Flush(); err != nil {
						return
					}
					time.Sleep(syncStreamTailPollInterval)
				}
			}
		})
		return nil
	})
}

func normalizeSyncStreamRequest(c *fiber.Ctx) syncStreamRequest {
	domains := parseCSVQuery(c.Query("domains"))
	scopes := parseCSVQuery(c.Query("groupScopes"))
	mode := strings.ToLower(strings.TrimSpace(c.Query("mode")))
	if mode != "tail" {
		mode = "bootstrap"
	}

	return syncStreamRequest{
		Domains:      domains,
		GroupScopes:  scopes,
		ResumeCursor: strings.TrimSpace(c.Query("resumeCursor")),
		Mode:         mode,
	}
}

func parseCSVQuery(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func writeSSEEvent(w *bufio.Writer, event string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := w.WriteString("event: " + strings.TrimSpace(event) + "\n"); err != nil {
		return err
	}
	if _, err := w.WriteString("data: " + string(raw) + "\n\n"); err != nil {
		return err
	}
	return nil
}

func isSyncPatchEmpty(patches syncPullPatches) bool {
	return len(patches.Memos.Upserts) == 0 &&
		len(patches.Memos.Deletes) == 0 &&
		len(patches.Users.Upserts) == 0 &&
		len(patches.Friendships.Upserts) == 0 &&
		len(patches.Friendships.Deletes) == 0 &&
		len(patches.Groups.Upserts) == 0 &&
		len(patches.Groups.Deletes) == 0 &&
		len(patches.GroupMessages.Groups) == 0 &&
		len(patches.Attachments.Upserts) == 0 &&
		len(patches.Attachments.Deletes) == 0 &&
		patches.Settings.GeneralSetting == nil &&
		patches.Settings.Encryption == nil &&
		len(patches.GroupKeys.Upserts) == 0 &&
		len(patches.GroupKeys.Deletes) == 0
}

func streamSessionID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

const (
	syncStreamChunkLimit       = 200
	syncStreamTailPollInterval = 2 * time.Second
)
