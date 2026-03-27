package http

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

func TestSyncPullProcessor_AttachmentPatchUsesAttachmentMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "sync_pull_processor_attachment.db")
	sqliteDB, err := db.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteDB.Close()
	})
	if err := db.Migrate(sqliteDB); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	sqlStore := store.New(sqliteDB)
	userService := service.NewUserService(sqlStore)
	memoService := service.NewMemoService(sqlStore)
	groupService := service.NewGroupService(sqlStore)

	user, err := sqlStore.CreateUser(ctx, "sync-attachment-user", "HOST")
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	encryptionMetadata, err := marshalAttachmentEncryptionMetadata(
		"descriptor-main",
		&apiPayloadEnvelope{WrappedKeys: []apiWrappedKeySlot{
			{
				SlotType:      "account_master",
				SlotRef:       "users/" + models.Int64ToString(user.ID),
				WrapAlgorithm: "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				WrappedKey:    "main-wrap",
			},
		}},
		"blob-main",
		"thumb-main",
	)
	if err != nil {
		t.Fatalf("marshalAttachmentEncryptionMetadata() error = %v", err)
	}

	attachment, err := sqlStore.CreateAttachment(
		ctx,
		user.ID,
		"camera.jpg",
		"",
		"image/jpeg",
		1024,
		"sync-pull-attachment-hash",
		encryptionMetadata,
		"LOCAL",
		"attachments/test/camera.jpg",
	)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	processor := newSyncPullProcessor(sqlStore, userService, memoService, groupService, nil)
	response, err := processor.Compute(ctx, user, syncPullRequest{
		Cursor:  "0",
		Domains: []string{string(models.SyncDomainAttachments)},
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("Compute() error = %v", err)
	}

	if len(response.Patches.Attachments.Upserts) != 1 {
		t.Fatalf("expected 1 attachment upsert, got %d", len(response.Patches.Attachments.Upserts))
	}

	apiAttachment := response.Patches.Attachments.Upserts[0]
	if apiAttachment.Name != "attachments/"+models.Int64ToString(attachment.ID) {
		t.Fatalf("unexpected attachment name: %s", apiAttachment.Name)
	}
	if apiAttachment.Filename != "camera.jpg" {
		t.Fatalf("expected filename camera.jpg, got %q", apiAttachment.Filename)
	}
	if apiAttachment.Type != "image/jpeg" {
		t.Fatalf("expected type image/jpeg, got %q", apiAttachment.Type)
	}
	if apiAttachment.DescriptorCiphertext != "descriptor-main" {
		t.Fatalf("expected descriptor ciphertext from attachment metadata, got %q", apiAttachment.DescriptorCiphertext)
	}
	if apiAttachment.BlobEncryption != "blob-main" {
		t.Fatalf("expected blob encryption from attachment metadata, got %q", apiAttachment.BlobEncryption)
	}
	if apiAttachment.ThumbnailBlobEncryption != "thumb-main" {
		t.Fatalf("expected thumbnail blob encryption from attachment metadata, got %q", apiAttachment.ThumbnailBlobEncryption)
	}
}
