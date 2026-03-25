package http

import (
	"testing"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func TestToAPIAttachment_FallbackThumbnailBlobEncryptionFromAttachmentMetadata(t *testing.T) {
	attachmentMetadata, err := marshalAttachmentEncryptionMetadata(
		"descriptor-main",
		&apiPayloadEnvelope{WrappedKeys: []apiWrappedKeySlot{
			{
				SlotType:      "account_master",
				SlotRef:       "users/1",
				WrapAlgorithm: "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				WrappedKey:    "main-wrap",
			},
		}},
		"blob-main",
		"thumb-main",
	)
	if err != nil {
		t.Fatalf("marshal attachment metadata failed: %v", err)
	}
	associationMetadata, err := marshalAttachmentEncryptionMetadata(
		"descriptor-association",
		&apiPayloadEnvelope{WrappedKeys: []apiWrappedKeySlot{
			{
				SlotType:      "group_key_version",
				SlotRef:       "groups/1/keyVersions/1",
				WrapAlgorithm: "AES_GCM_GROUP_KEY_V1",
				WrappedKey:    "assoc-wrap",
			},
		}},
		"blob-association",
		"",
	)
	if err != nil {
		t.Fatalf("marshal association metadata failed: %v", err)
	}

	apiModel := toAPIAttachment(
		models.Attachment{
			ID:                            8,
			Filename:                      "demo.jpg",
			Type:                          "image/jpeg",
			Size:                          1024,
			EncryptionMetadata:            attachmentMetadata,
			AssociationEncryptionMetadata: associationMetadata,
			ThumbnailStorageKey:           "attachments/8/demo.thumb.bin",
			ThumbnailFilename:             "demo.thumb.bin",
			ThumbnailType:                 "image/jpeg",
			CreateTime:                    time.Now().UTC(),
		},
		"",
		"",
		"",
		true,
	)

	if apiModel.BlobEncryption != "blob-association" {
		t.Fatalf("expected association blobEncryption, got %q", apiModel.BlobEncryption)
	}
	if apiModel.ThumbnailBlobEncryption != "thumb-main" {
		t.Fatalf("expected fallback thumbnailBlobEncryption from attachment metadata, got %q", apiModel.ThumbnailBlobEncryption)
	}
}
