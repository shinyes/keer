package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"mime"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
)

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

func toAPIUserSync(user models.User) apiUser {
	name := ""
	if user.ID > 0 {
		name = user.Name()
	}
	return apiUser{
		Name:        name,
		Username:    user.Username,
		DisplayName: user.DisplayName,
		AvatarURL:   user.AvatarURL,
		UpdateTime:  formatMaybeTime(user.UpdateTime),
	}
}

func toAPIGroup(group service.GroupWithMembers) apiGroup {
	members := make([]apiGroupMember, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, apiGroupMember{
			Name:        member.Name(),
			Username:    member.Username,
			DisplayName: member.DisplayName,
		})
	}
	return apiGroup{
		Name:        group.Group.Name(),
		Creator:     "users/" + models.Int64ToString(group.Group.CreatorID),
		CreateTime:  formatMaybeTime(group.Group.CreateTime),
		UpdateTime:  formatMaybeTime(group.Group.UpdateTime),
		GroupName:   group.Group.GroupName,
		Description: group.Group.Description,
		Members:     members,
	}
}

func toAPIGroupMessage(msg service.GroupMessageWithCreator) apiGroupMessage {
	tags := msg.Message.Tags
	if tags == nil {
		tags = []string{}
	}
	return apiGroupMessage{
		Name:       msg.Message.Name(),
		Group:      "groups/" + models.Int64ToString(msg.Message.GroupID),
		Creator:    msg.Creator.Name(),
		CreateTime: formatMaybeTime(msg.Message.CreateTime),
		UpdateTime: formatMaybeTime(msg.Message.UpdateTime),
		Content:    msg.Message.Content,
		Tags:       tags,
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
	quote := toAPIMemoQuote(memo.Quote, attachmentMapper)
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
		Quote:       quote,
	}
}

func toAPIMemoQuote(
	quote *service.MemoQuote,
	attachmentMapper func(attachment models.Attachment, memoName string) apiAttachment,
) *apiMemoQuote {
	if quote == nil {
		return nil
	}
	out := &apiMemoQuote{
		SourceKind: string(quote.SourceKind),
		Source:     quote.Source,
	}
	if quote.Memo == nil {
		return out
	}

	referenced := quote.Memo
	referencedAttachments := make([]apiAttachment, 0, len(referenced.Attachments))
	for _, attachment := range referenced.Attachments {
		if attachmentMapper != nil {
			referencedAttachments = append(referencedAttachments, attachmentMapper(attachment, referenced.Memo.Name()))
			continue
		}
		referencedAttachments = append(referencedAttachments, toAPIAttachment(attachment, referenced.Memo.Name(), "", ""))
	}
	tags := referenced.Memo.Payload.Tags
	if tags == nil {
		tags = []string{}
	}
	out.Memo = &apiMemoQuoteMemo{
		Name:        referenced.Memo.Name(),
		Creator:     "users/" + models.Int64ToString(referenced.Memo.CreatorID),
		CreateTime:  formatMaybeTime(referenced.Memo.CreateTime),
		UpdateTime:  formatMaybeTime(referenced.Memo.UpdateTime),
		Content:     referenced.Memo.Content,
		Visibility:  string(referenced.Memo.Visibility),
		Attachments: referencedAttachments,
		Tags:        tags,
	}
	return out
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

func parseRequiredIDParam(c *fiber.Ctx, param string, invalidMessage string) (int64, error) {
	id, err := parseID(c.Params(param))
	if err != nil {
		return 0, badRequest(c, invalidMessage)
	}
	return id, nil
}

func mapNoRowsToNotFound(c *fiber.Ctx, err error, message string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return notFound(c, message)
	}
	return nil
}

func mapGroupMessageMutationError(
	c *fiber.Ctx,
	err error,
	notFoundMessage string,
	badRequestFallback bool,
) error {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return notFound(c, notFoundMessage)
	case errors.Is(err, service.ErrGroupMessagePermissionDenied):
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "forbidden"})
	case badRequestFallback:
		return badRequest(c, err.Error())
	default:
		return internalError(c, err)
	}
}

func parseBatchIdentifiers(raw string) []string {
	items := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		identifier := strings.TrimSpace(item)
		if identifier == "" {
			continue
		}
		if _, exists := seen[identifier]; exists {
			continue
		}
		seen[identifier] = struct{}{}
		result = append(result, identifier)
	}
	return result
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
