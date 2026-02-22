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
	key, err := buildAttachmentStorageKey(1, "16848.jpg")
	if err != nil {
		t.Fatalf("buildAttachmentStorageKey() error = %v", err)
	}

	if !strings.HasPrefix(key, "attachments/1/") {
		t.Fatalf("unexpected key prefix: %s", key)
	}
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		t.Fatalf("unexpected key format: %s", key)
	}

	filePart := parts[2]
	if !strings.HasSuffix(filePart, "_16848.jpg") {
		t.Fatalf("unexpected file part: %s", filePart)
	}
	nanoid := strings.TrimSuffix(filePart, "_16848.jpg")
	if len(nanoid) != 5 {
		t.Fatalf("unexpected nanoid length: got %d, key=%s", len(nanoid), key)
	}
	for _, ch := range nanoid {
		if !(ch >= '0' && ch <= '9') &&
			!(ch >= 'a' && ch <= 'z') &&
			!(ch >= 'A' && ch <= 'Z') &&
			ch != '-' &&
			ch != '_' {
			t.Fatalf("unexpected nanoid char %q in key=%s", ch, key)
		}
	}
}

func TestGenerateShortNanoID_Length(t *testing.T) {
	id, err := generateShortNanoID(5)
	if err != nil {
		t.Fatalf("generateShortNanoID() error = %v", err)
	}
	if len(id) != 5 {
		t.Fatalf("nanoid length got %d, want 5", len(id))
	}
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

func TestCreateAttachment_NoDedupForDifferentFilename(t *testing.T) {
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

	if first.ID == second.ID {
		t.Fatalf("expected different attachment id for different filename")
	}
}
