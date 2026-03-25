package http

import (
	"database/sql"
	"encoding/json"
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
		Name:       name,
		Role:       role,
		Username:   user.Username,
		AvatarURL:  user.AvatarURL,
		State:      "NORMAL",
		CreateTime: formatMaybeTime(user.CreateTime),
		UpdateTime: formatMaybeTime(user.UpdateTime),
	}
}

func toAPIUserSync(user models.User) apiUser {
	name := ""
	if user.ID > 0 {
		name = user.Name()
	}
	return apiUser{
		Name:       name,
		Username:   user.Username,
		AvatarURL:  user.AvatarURL,
		UpdateTime: formatMaybeTime(user.UpdateTime),
	}
}

func toAPIUserEncryptionSetting(encryptionKey models.UserEncryptionKey) apiUserEncryptionSetting {
	return apiUserEncryptionSetting{
		RecoveryBundle: apiRecoveryBundle{
			Version:           encryptionKey.Version,
			KDFAlgorithm:      encryptionKey.KDFAlgorithm,
			KDFSalt:           encryptionKey.KDFSalt,
			KDFTimeCost:       encryptionKey.KDFTimeCost,
			KDFMemoryKiB:      encryptionKey.KDFMemoryKiB,
			KDFParallelism:    encryptionKey.KDFParallelism,
			WrapAlgorithm:     encryptionKey.WrapAlgorithm,
			WrappedAccountKey: encryptionKey.WrappedAccountKey,
		},
		SharingPublicKey:         encryptionKey.SharingPublicKey,
		WrappedSharingPrivateKey: encryptionKey.WrappedSharingPrivateKey,
		KeyVersion:               encryptionKey.KeyVersion,
		Algorithms:               encryptionKey.Algorithms,
		CreateTime:               formatMaybeTime(encryptionKey.CreateTime),
		UpdateTime:               formatMaybeTime(encryptionKey.UpdateTime),
	}
}

func toAPIGroup(group service.GroupWithMembers) apiGroup {
	members := make([]apiGroupMember, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, apiGroupMember{
			Name:     member.Name(),
			Username: member.Username,
		})
	}
	return apiGroup{
		Name:        group.Group.Name(),
		Creator:     "users/" + models.Int64ToString(group.Group.CreatorID),
		CreateTime:  formatMaybeTime(group.Group.CreateTime),
		UpdateTime:  formatMaybeTime(group.Group.UpdateTime),
		Type:        string(group.Group.Type),
		HasUnread:   group.Group.HasUnread,
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
	attachments := make([]apiAttachment, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		attachments = append(attachments, toAPIAttachment(attachment, "", "", "", true))
	}
	return apiGroupMessage{
		Name:             msg.Message.Name(),
		Group:            "groups/" + models.Int64ToString(msg.Message.GroupID),
		Creator:          msg.Creator.Name(),
		CreateTime:       formatMaybeTime(msg.Message.CreateTime),
		UpdateTime:       formatMaybeTime(msg.Message.UpdateTime),
		EncryptedPayload: msg.Message.Content,
		PayloadEnvelope:  parsePayloadEnvelope(msg.Message.PayloadEnvelope),
		Tags:             tags,
		Attachments:      attachments,
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
		attachments = append(attachments, toAPIAttachment(attachment, memo.Memo.Name(), "", "", true))
	}
	tags := memo.Memo.Payload.Tags
	if tags == nil {
		tags = []string{}
	}
	return apiMemo{
		Name:             memo.Memo.Name(),
		State:            string(memo.Memo.State),
		Creator:          "users/" + models.Int64ToString(memo.Memo.CreatorID),
		CreateTime:       formatTime(memo.Memo.CreateTime),
		UpdateTime:       formatTime(memo.Memo.UpdateTime),
		EncryptedPayload: memo.Memo.Content,
		PayloadEnvelope:  parsePayloadEnvelope(memo.Memo.PayloadEnvelope),
		Visibility:       string(memo.Memo.Visibility),
		Pinned:           memo.Memo.Pinned,
		Latitude:         memo.Memo.Latitude,
		Longitude:        memo.Memo.Longitude,
		Attachments:      attachments,
		Tags:             tags,
		Quote:            toAPIMemoQuote(memo.Quote, attachmentMapper),
	}
}

func toAPIMemoQuote(
	quote *service.MemoQuote,
	attachmentMapper func(attachment models.Attachment, memoName string) apiAttachment,
) *apiMemoQuote {
	if quote == nil {
		return nil
	}

	apiQuote := &apiMemoQuote{
		SourceKind: string(quote.SourceKind),
		Source:     quote.Source,
	}
	if quote.Memo == nil {
		return apiQuote
	}

	attachments := make([]apiAttachment, 0, len(quote.Memo.Attachments))
	for _, attachment := range quote.Memo.Attachments {
		if attachmentMapper != nil {
			attachments = append(attachments, attachmentMapper(attachment, quote.Memo.Memo.Name()))
			continue
		}
		attachments = append(attachments, toAPIAttachment(attachment, quote.Memo.Memo.Name(), "", "", true))
	}

	tags := quote.Memo.Memo.Payload.Tags
	if tags == nil {
		tags = []string{}
	}

	apiQuote.Memo = &apiMemoQuoteMemo{
		Name:             quote.Memo.Memo.Name(),
		Creator:          "users/" + models.Int64ToString(quote.Memo.Memo.CreatorID),
		CreateTime:       formatTime(quote.Memo.Memo.CreateTime),
		UpdateTime:       formatTime(quote.Memo.Memo.UpdateTime),
		EncryptedPayload: quote.Memo.Memo.Content,
		PayloadEnvelope:  parsePayloadEnvelope(quote.Memo.Memo.PayloadEnvelope),
		Visibility:       string(quote.Memo.Memo.Visibility),
		Attachments:      attachments,
		Tags:             tags,
	}
	return apiQuote
}

func toAPIAttachment(
	attachment models.Attachment,
	memoName string,
	directLink string,
	directThumbnailLink string,
	preferAssociationMetadata bool,
) apiAttachment {
	thumbnailName := ""
	if strings.TrimSpace(attachment.ThumbnailStorageKey) != "" {
		thumbnailName = "attachments/" + models.Int64ToString(attachment.ID) + "/thumbnail"
	}
	externalLink := strings.TrimSpace(directLink)
	if externalLink == "" {
		externalLink = strings.TrimSpace(attachment.ExternalLink)
	}
	thumbnailExternalLink := strings.TrimSpace(directThumbnailLink)
	attachmentDescriptor := parseAttachmentEncryptionMetadata(strings.TrimSpace(attachment.EncryptionMetadata))
	effectiveEncryptionMetadata := strings.TrimSpace(attachment.EncryptionMetadata)
	if preferAssociationMetadata {
		effectiveEncryptionMetadata = strings.TrimSpace(attachment.AssociationEncryptionMetadata)
	}
	descriptor := parseAttachmentEncryptionMetadata(effectiveEncryptionMetadata)
	if preferAssociationMetadata && strings.TrimSpace(descriptor.ThumbnailBlobEncryption) == "" {
		descriptor.ThumbnailBlobEncryption = strings.TrimSpace(attachmentDescriptor.ThumbnailBlobEncryption)
	}
	return apiAttachment{
		Name:                    "attachments/" + models.Int64ToString(attachment.ID),
		CreateTime:              formatTime(attachment.CreateTime),
		DescriptorCiphertext:    descriptor.DescriptorCiphertext,
		DescriptorEnvelope:      descriptor.DescriptorEnvelope,
		BlobEncryption:          descriptor.BlobEncryption,
		ThumbnailBlobEncryption: descriptor.ThumbnailBlobEncryption,
		Filename:                attachment.Filename,
		ExternalLink:            externalLink,
		Type:                    attachment.Type,
		Size:                    models.Int64ToString(attachment.Size),
		ThumbnailName:           thumbnailName,
		ThumbnailExternalLink:   thumbnailExternalLink,
		ThumbnailFilename:       attachment.ThumbnailFilename,
		ThumbnailType:           attachment.ThumbnailType,
		Memo:                    memoName,
	}
}

type storedAttachmentEncryptionMetadata struct {
	DescriptorCiphertext    string              `json:"descriptorCiphertext"`
	DescriptorEnvelope      *apiPayloadEnvelope `json:"descriptorEnvelope"`
	BlobEncryption          string              `json:"blobEncryption"`
	ThumbnailBlobEncryption string              `json:"thumbnailBlobEncryption"`
}

const (
	payloadEnvelopeSlotTypeAccountMaster = "account_master"
	payloadEnvelopeSlotTypeAccountPublic = "account_public"
	payloadEnvelopeSlotTypeGroupKeyVer   = "group_key_version"

	payloadWrapAlgorithmAccountMaster = "AES_GCM_ACCOUNT_MASTER_KEY_V1"
	payloadWrapAlgorithmAccountPublic = "RSA_OAEP_SHA256_V1"
	payloadWrapAlgorithmGroupKeyVer   = "AES_GCM_GROUP_KEY_V1"
)

func parseAttachmentEncryptionMetadata(raw string) storedAttachmentEncryptionMetadata {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return storedAttachmentEncryptionMetadata{}
	}
	var metadata storedAttachmentEncryptionMetadata
	if err := json.Unmarshal([]byte(trimmed), &metadata); err != nil {
		return storedAttachmentEncryptionMetadata{}
	}
	return metadata
}

func marshalAttachmentEncryptionMetadata(
	descriptorCiphertext string,
	descriptorEnvelope *apiPayloadEnvelope,
	blobEncryption string,
	thumbnailBlobEncryption string,
) (string, error) {
	raw, err := json.Marshal(storedAttachmentEncryptionMetadata{
		DescriptorCiphertext:    strings.TrimSpace(descriptorCiphertext),
		DescriptorEnvelope:      descriptorEnvelope,
		BlobEncryption:          strings.TrimSpace(blobEncryption),
		ThumbnailBlobEncryption: strings.TrimSpace(thumbnailBlobEncryption),
	})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func parsePayloadEnvelope(raw string) *apiPayloadEnvelope {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var envelope apiPayloadEnvelope
	if err := json.Unmarshal([]byte(trimmed), &envelope); err != nil {
		return nil
	}
	return &envelope
}

func mustMarshalPayloadEnvelope(envelope *apiPayloadEnvelope) string {
	if envelope == nil {
		return ""
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return ""
	}
	return string(raw)
}

func toAPIGroupKeyVersion(version models.GroupKeyVersion, recipients []models.GroupKeyVersionRecipient) apiGroupKeyVersion {
	wrappedKeys := make([]apiWrappedKeySlot, 0, len(recipients))
	for _, recipient := range recipients {
		wrappedKeys = append(wrappedKeys, apiWrappedKeySlot{
			SlotType:      "account_public",
			SlotRef:       recipient.SlotRef,
			WrapAlgorithm: recipient.WrapAlgorithm,
			WrappedKey:    recipient.WrappedKey,
		})
	}
	return apiGroupKeyVersion{
		Name:        fmt.Sprintf("groups/%d/keyVersions/%d", version.GroupID, version.Version),
		Group:       fmt.Sprintf("groups/%d", version.GroupID),
		Version:     version.Version,
		Algorithm:   version.Algorithm,
		WrappedKeys: wrappedKeys,
		CreateTime:  formatMaybeTime(version.CreateTime),
		UpdateTime:  formatMaybeTime(version.UpdateTime),
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

func parseMessageResourceID(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty id")
	}
	raw = strings.Trim(raw, "/")
	if idx := strings.LastIndex(raw, "/"); idx >= 0 {
		raw = raw[idx+1:]
	}
	return parseID(raw)
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

func trimUpdatedText(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}

func validatePayloadEnvelope(envelope *apiPayloadEnvelope) error {
	if envelope == nil {
		return fmt.Errorf("payload envelope is required")
	}
	if len(envelope.WrappedKeys) == 0 {
		return fmt.Errorf("payload envelope wrappedKeys is required")
	}
	for _, wrappedKey := range envelope.WrappedKeys {
		slotType := strings.TrimSpace(wrappedKey.SlotType)
		if slotType == "" ||
			strings.TrimSpace(wrappedKey.SlotRef) == "" ||
			strings.TrimSpace(wrappedKey.WrapAlgorithm) == "" ||
			strings.TrimSpace(wrappedKey.WrappedKey) == "" {
			return fmt.Errorf("payload envelope wrapped key is invalid")
		}
		if !isSupportedKeySlotType(slotType) {
			return fmt.Errorf("payload envelope wrapped key slot type is invalid")
		}
		if !isSupportedWrapAlgorithmForSlot(slotType, strings.TrimSpace(wrappedKey.WrapAlgorithm)) {
			return fmt.Errorf("payload envelope wrapped key algorithm is invalid")
		}
	}
	return nil
}

func isSupportedKeySlotType(slotType string) bool {
	switch slotType {
	case payloadEnvelopeSlotTypeAccountMaster, payloadEnvelopeSlotTypeAccountPublic, payloadEnvelopeSlotTypeGroupKeyVer:
		return true
	default:
		return false
	}
}

func isSupportedWrapAlgorithmForSlot(slotType string, wrapAlgorithm string) bool {
	switch slotType {
	case payloadEnvelopeSlotTypeAccountMaster:
		return wrapAlgorithm == payloadWrapAlgorithmAccountMaster
	case payloadEnvelopeSlotTypeAccountPublic:
		return wrapAlgorithm == payloadWrapAlgorithmAccountPublic
	case payloadEnvelopeSlotTypeGroupKeyVer:
		return wrapAlgorithm == payloadWrapAlgorithmGroupKeyVer
	default:
		return false
	}
}

func validateGroupKeyVersionWrappedKeys(wrappedKeys []apiWrappedKeySlot) error {
	if err := validatePayloadEnvelope(&apiPayloadEnvelope{WrappedKeys: wrappedKeys}); err != nil {
		return err
	}
	for _, wrappedKey := range wrappedKeys {
		if strings.TrimSpace(wrappedKey.SlotType) != payloadEnvelopeSlotTypeAccountPublic {
			return fmt.Errorf("group key wrapped key slot type is invalid")
		}
	}
	return nil
}

func (e *apiPayloadEnvelope) asJSONStringPointer() *string {
	if e == nil {
		return nil
	}
	raw := mustMarshalPayloadEnvelope(e)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return &raw
}

func attachmentNamesFromAPI(attachments []apiAttachment) []string {
	names := make([]string, 0, len(attachments))
	for _, attachment := range attachments {
		if strings.TrimSpace(attachment.Name) == "" {
			continue
		}
		names = append(names, attachment.Name)
	}
	return names
}

func attachmentNamesFromAPIPointer(attachments *[]apiAttachment) *[]string {
	if attachments == nil {
		return nil
	}
	names := attachmentNamesFromAPI(*attachments)
	return &names
}

func attachmentBindingsFromAPI(attachments []apiAttachment) ([]service.AttachmentBindingInput, error) {
	bindings := make([]service.AttachmentBindingInput, 0, len(attachments))
	for _, attachment := range attachments {
		name := strings.TrimSpace(attachment.Name)
		if name == "" {
			continue
		}
		descriptorCiphertext := strings.TrimSpace(attachment.DescriptorCiphertext)
		blobEncryption := strings.TrimSpace(attachment.BlobEncryption)
		if descriptorCiphertext == "" || attachment.DescriptorEnvelope == nil || blobEncryption == "" {
			return nil, fmt.Errorf("missing attachment encryption metadata")
		}
		if err := validatePayloadEnvelope(attachment.DescriptorEnvelope); err != nil {
			return nil, fmt.Errorf("invalid descriptor envelope")
		}
		raw, err := marshalAttachmentEncryptionMetadata(
			descriptorCiphertext,
			attachment.DescriptorEnvelope,
			blobEncryption,
			attachment.ThumbnailBlobEncryption,
		)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, service.AttachmentBindingInput{
			Name:                          name,
			AssociationEncryptionMetadata: raw,
		})
	}
	return bindings, nil
}

func attachmentBindingsFromAPIPointer(attachments *[]apiAttachment) (*[]service.AttachmentBindingInput, error) {
	if attachments == nil {
		return nil, nil
	}
	bindings, err := attachmentBindingsFromAPI(*attachments)
	if err != nil {
		return nil, err
	}
	return &bindings, nil
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
