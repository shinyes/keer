package http

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAttachmentThumbnailServing(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	imageBytes := generateThumbnailTestJPEG(t, 1400, 900)
	createPayload := map[string]any{
		"filename": "scene.jpg",
		"type":     "image/jpeg",
		"content":  base64.StdEncoding.EncodeToString(imageBytes),
	}
	createBody, _ := json.Marshal(createPayload)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create attachment request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 200, got %d body=%s", createResp.StatusCode, string(body))
	}

	var created apiAttachment
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create attachment response failed: %v", err)
	}
	if created.ThumbnailName == "" || created.ThumbnailFilename == "" {
		t.Fatalf("expected attachment thumbnail metadata, got name=%q filename=%q", created.ThumbnailName, created.ThumbnailFilename)
	}

	thumbnailPath := "/file/" + created.ThumbnailName + "/" + created.ThumbnailFilename
	thumbnailReq := httptest.NewRequest(http.MethodGet, thumbnailPath, nil)
	thumbnailReq.Header.Set("Authorization", "Bearer "+token)
	thumbnailResp, err := app.Test(thumbnailReq, 5000)
	if err != nil {
		t.Fatalf("thumbnail request failed: %v", err)
	}
	defer thumbnailResp.Body.Close()
	if thumbnailResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(thumbnailResp.Body)
		t.Fatalf("expected thumbnail 200, got %d body=%s", thumbnailResp.StatusCode, string(body))
	}
	if got := strings.ToLower(thumbnailResp.Header.Get("Content-Type")); !strings.HasPrefix(got, "image/jpeg") {
		t.Fatalf("expected image/jpeg content type, got %q", got)
	}

	thumbnailBody, err := io.ReadAll(thumbnailResp.Body)
	if err != nil {
		t.Fatalf("read thumbnail body failed: %v", err)
	}
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(thumbnailBody))
	if err != nil {
		t.Fatalf("decode thumbnail jpeg config failed: %v", err)
	}
	if cfg.Width > 640 || cfg.Height > 640 {
		t.Fatalf("expected thumbnail max dimensions <= 640x640, got %dx%d", cfg.Width, cfg.Height)
	}
	if cfg.Width >= 1400 || cfg.Height >= 900 {
		t.Fatalf("expected thumbnail to be resized from original size, got %dx%d", cfg.Width, cfg.Height)
	}
}

func generateThumbnailTestJPEG(t *testing.T, width int, height int) []byte {
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
