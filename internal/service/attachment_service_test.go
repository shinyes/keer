package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/jpeg"
	"path/filepath"
	"regexp"
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
	key := buildAttachmentStorageKey(1, "a1B2cD3e", "16848.jpg")
	if key != "attachments/1/a1B2cD3e_16848.jpg" {
		t.Fatalf("unexpected key format: %s", key)
	}
}

func TestGenerateNanoID_Format(t *testing.T) {
	id, err := generateNanoID(attachmentNanoIDLength)
	if err != nil {
		t.Fatalf("generateNanoID() error = %v", err)
	}
	if len(id) != attachmentNanoIDLength {
		t.Fatalf("unexpected nano id length: got %d", len(id))
	}
	if ok, _ := regexp.MatchString(`^[0-9A-Za-z]{8}$`, id); !ok {
		t.Fatalf("unexpected nano id format: %s", id)
	}
}

func TestGenerateNanoID_InvalidLength(t *testing.T) {
	if _, err := generateNanoID(0); err == nil {
		t.Fatal("expected invalid length error")
	}
}

func TestCreateAttachment_DeduplicateStorageSameContent(t *testing.T) {
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

	if first.ID == second.ID {
		t.Fatalf("expected distinct attachment ids, got same id=%d", first.ID)
	}
	if first.StorageKey != second.StorageKey {
		t.Fatalf("expected shared storage key for same content, got first=%q second=%q", first.StorageKey, second.StorageKey)
	}
	if !strings.HasPrefix(first.StorageKey, "attachments/") {
		t.Fatalf("unexpected storage key prefix: %q", first.StorageKey)
	}
	if ok, _ := regexp.MatchString(`^attachments/\d+/[0-9A-Za-z]{8}_`, first.StorageKey); !ok {
		t.Fatalf("unexpected storage key format: %q", first.StorageKey)
	}
	list, err := services.store.ListAttachmentsByCreator(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("ListAttachmentsByCreator() error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected two attachment records, got %d", len(list))
	}
}

func TestCreateAttachment_DedupStorageForDifferentFilename(t *testing.T) {
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
		t.Fatalf("expected distinct attachment ids for different uploads")
	}
	if first.StorageKey != second.StorageKey {
		t.Fatalf("expected shared storage key for same content, got first=%q second=%q", first.StorageKey, second.StorageKey)
	}
	list, err := services.store.ListAttachmentsByCreator(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("ListAttachmentsByCreator() error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected two attachment records, got %d", len(list))
	}
}

func TestDeleteAttachment_KeepFileWhenSharedStorageKey(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-delete-shared")

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
	if first.StorageKey != second.StorageKey {
		t.Fatalf("expected shared storage key, got first=%q second=%q", first.StorageKey, second.StorageKey)
	}

	if err := attachmentService.DeleteAttachment(context.Background(), user.ID, first.ID); err != nil {
		t.Fatalf("DeleteAttachment() error = %v", err)
	}

	_, rc, err := attachmentService.OpenAttachment(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("OpenAttachment() error = %v", err)
	}
	_ = rc.Close()

	list, err := services.store.ListAttachmentsByCreator(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("ListAttachmentsByCreator() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected one remaining attachment record, got %d", len(list))
	}
}

func TestCreateAttachment_GeneratesThumbnailForImage(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-thumbnail-image")

	content := base64.StdEncoding.EncodeToString(generateTestJPEGBytes(t, 1200, 900))
	attachment, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "large.jpg",
		Type:     "image/jpeg",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	if attachment.ThumbnailStorageKey == "" {
		t.Fatalf("expected thumbnail storage key to be populated")
	}

	thumbnailReader, err := localStore.Open(context.Background(), attachment.ThumbnailStorageKey)
	if err != nil {
		t.Fatalf("expected thumbnail to exist, open error = %v", err)
	}
	_ = thumbnailReader.Close()
}

func TestCreateAttachment_DoesNotGenerateThumbnailForNonImage(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-thumbnail-text")

	content := base64.StdEncoding.EncodeToString([]byte("plain text data"))
	attachment, err := attachmentService.CreateAttachment(context.Background(), user.ID, CreateAttachmentInput{
		Filename: "notes.txt",
		Type:     "text/plain",
		Content:  content,
	})
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	if attachment.ThumbnailStorageKey != "" {
		t.Fatalf("unexpected thumbnail exists for non-image attachment")
	}
}

func generateTestJPEGBytes(t *testing.T, width int, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(x % 256),
				G: uint8(y % 256),
				B: uint8((x + y) % 256),
				A: 255,
			})
		}
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatalf("jpeg.Encode() error = %v", err)
	}
	return buf.Bytes()
}
