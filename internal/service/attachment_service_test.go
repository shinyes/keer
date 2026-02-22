package service

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shinyes/keer/internal/storage"
)

func TestParseMemoID_CompatibilityFormats(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "simple", input: "memos/9", want: 9},
		{name: "plain id", input: "9", want: 9},
		{name: "with suffix", input: "memos/9|local-uuid", want: 9},
		{name: "full path", input: "/api/v1/memos/9", want: 9},
		{name: "trailing slash", input: "memos/9/", want: 9},
		{name: "invalid", input: "memos/", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseMemoID(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: parseMemoID() error = %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: parseMemoID() got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestBuildAttachmentStorageKey_Format(t *testing.T) {
	const hash = "9eeb410cd359fd0d435f6914adf86000f5b8ffac9d5ec100ba9f70f5cd5cb788"
	key, err := buildAttachmentStorageKey(1, "16848.jpg", hash)
	if err != nil {
		t.Fatalf("buildAttachmentStorageKey() error = %v", err)
	}

	if !strings.HasPrefix(key, "attachments/1/9e/") {
		t.Fatalf("unexpected key prefix: %s", key)
	}
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		t.Fatalf("unexpected key format: %s", key)
	}

	filePart := parts[3]
	if filePart != hash+"_16848.jpg" {
		t.Fatalf("unexpected file part: %s", filePart)
	}
}

func TestBuildAttachmentStorageKey_InvalidHash(t *testing.T) {
	_, err := buildAttachmentStorageKey(1, "16848.jpg", "bad-hash")
	if err != nil {
		return
	}
	t.Fatal("expected invalid content hash error")
}

func TestCreateAttachment_DeduplicateSameContent(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-dedupe")

	content := base64.StdEncoding.EncodeToString([]byte("same-image-bytes"))
	first, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "test.jpg",
		Type:     "image/jpeg",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("first CreateAttachment() error = %v", err)
	}
	second, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "test.jpg",
		Type:     "image/jpeg",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("second CreateAttachment() error = %v", err)
	}

	if first.ID != second.ID {
		t.Fatalf("expected deduplicated attachment id, got first=%d second=%d", first.ID, second.ID)
	}
	list, err := services.store.ListAttachmentsByCreator(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("ListAttachmentsByCreator() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected single stored attachment, got %d", len(list))
	}
}

func TestCreateAttachment_DedupForDifferentFilename(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-no-dedupe")

	content := base64.StdEncoding.EncodeToString([]byte("same-image-bytes"))
	first, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "a.jpg",
		Type:     "image/jpeg",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("first CreateAttachment() error = %v", err)
	}
	second, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "b.jpg",
		Type:     "image/jpeg",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("second CreateAttachment() error = %v", err)
	}

	if first.ID != second.ID {
		t.Fatalf("expected deduplicated attachment id for same content, got first=%d second=%d", first.ID, second.ID)
	}
	list, err := services.store.ListAttachmentsByCreator(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("ListAttachmentsByCreator() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected single stored attachment, got %d", len(list))
	}
}
