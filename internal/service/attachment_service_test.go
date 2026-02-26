package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/jpeg"
	"os"
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

func TestCompleteAttachmentUploadSession_UsesClientProvidedThumbnail(t *testing.T) {
	services := setupTestServices(t)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := NewAttachmentService(services.store, localStore)
	user := mustCreateUser(t, services.store, "attach-upload-thumbnail-video")

	videoData := []byte("video binary data")
	thumbnailData := generateTestJPEGBytes(t, 800, 450)
	session, err := attachmentService.CreateAttachmentUploadSession(
		context.Background(),
		user.ID,
		CreateAttachmentUploadSessionInput{
			Filename: "clip.mp4",
			Type:     "video/mp4",
			Size:     int64(len(videoData)),
			Thumbnail: &CreateAttachmentUploadSessionThumbnailInput{
				Filename: "clip_preview.jpg",
				Type:     "image/jpeg",
				Content:  base64.StdEncoding.EncodeToString(thumbnailData),
			},
		},
	)
	if err != nil {
		t.Fatalf("CreateAttachmentUploadSession() error = %v", err)
	}
	if strings.TrimSpace(session.ThumbnailTempPath) == "" {
		t.Fatalf("expected thumbnail temp path to be populated")
	}

	session, err = attachmentService.AppendAttachmentUploadChunk(
		context.Background(),
		user.ID,
		session.ID,
		0,
		videoData,
	)
	if err != nil {
		t.Fatalf("AppendAttachmentUploadChunk() error = %v", err)
	}
	if session.ReceivedSize != int64(len(videoData)) {
		t.Fatalf("unexpected upload offset, got %d", session.ReceivedSize)
	}

	attachment, err := attachmentService.CompleteAttachmentUploadSession(context.Background(), user.ID, session.ID)
	if err != nil {
		t.Fatalf("CompleteAttachmentUploadSession() error = %v", err)
	}
	if attachment.ThumbnailStorageKey == "" {
		t.Fatalf("expected provided thumbnail to be stored")
	}
	if attachment.ThumbnailFilename != "clip_preview.jpg" {
		t.Fatalf("unexpected thumbnail filename: %s", attachment.ThumbnailFilename)
	}
	if attachment.ThumbnailType != "image/jpeg" {
		t.Fatalf("unexpected thumbnail type: %s", attachment.ThumbnailType)
	}

	thumbnailReader, err := localStore.Open(context.Background(), attachment.ThumbnailStorageKey)
	if err != nil {
		t.Fatalf("expected thumbnail object to exist, open error = %v", err)
	}
	_ = thumbnailReader.Close()

	if _, err := os.Stat(session.ThumbnailTempPath); !os.IsNotExist(err) {
		t.Fatalf("expected thumbnail temp file removed, stat err=%v", err)
	}
}

func TestMultipartSessionPathEncodeDecode_RoundTrip(t *testing.T) {
	encoded := encodeMultipartSessionPath(
		"attachments/1/demo|video.mp4",
		"upload|session|id",
		8*1024*1024,
	)
	got, ok := decodeMultipartSessionPath(encoded)
	if !ok {
		t.Fatalf("decodeMultipartSessionPath() ok = false, encoded=%q", encoded)
	}
	if got.StorageKey != "attachments/1/demo|video.mp4" {
		t.Fatalf("unexpected storage key: %q", got.StorageKey)
	}
	if got.MultipartUploadID != "upload|session|id" {
		t.Fatalf("unexpected multipart upload id: %q", got.MultipartUploadID)
	}
	if got.PartSize != 8*1024*1024 {
		t.Fatalf("unexpected part size: %d", got.PartSize)
	}
}

func TestDecodeMultipartSessionPath_LegacyFormat(t *testing.T) {
	legacy := multipartSessionPathPrefix + "attachments/1/video.mp4|legacy-upload-id|8388608"
	got, ok := decodeMultipartSessionPath(legacy)
	if !ok {
		t.Fatalf("decodeMultipartSessionPath() ok = false, legacy=%q", legacy)
	}
	if got.StorageKey != "attachments/1/video.mp4" {
		t.Fatalf("unexpected storage key: %q", got.StorageKey)
	}
	if got.MultipartUploadID != "legacy-upload-id" {
		t.Fatalf("unexpected multipart upload id: %q", got.MultipartUploadID)
	}
	if got.PartSize != 8388608 {
		t.Fatalf("unexpected part size: %d", got.PartSize)
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
