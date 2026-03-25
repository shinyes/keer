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
	"strconv"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestAttachmentThumbnailServing(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	imageBytes := generateThumbnailTestJPEG(t, 1400, 900)
	createPayload := map[string]any{
		"filename":             "scene.jpg",
		"type":                 "image/jpeg",
		"content":              base64.StdEncoding.EncodeToString(imageBytes),
		"descriptorCiphertext": "scene-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "scene-wrapped-key",
			},
		}},
		"blobEncryption": "scene-blob",
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

func TestUpdateAttachmentThumbnailRoute_Success(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createPayload := map[string]any{
		"filename":             "scene.jpg",
		"type":                 "image/jpeg",
		"content":              base64.StdEncoding.EncodeToString(generateThumbnailTestJPEG(t, 900, 640)),
		"descriptorCiphertext": "scene-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "scene-wrapped-key",
			},
		}},
		"blobEncryption":          "scene-blob",
		"thumbnailBlobEncryption": "old-thumb",
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
		t.Fatalf("expected create 200, got %d body=%s", createResp.StatusCode, string(body))
	}

	var created apiAttachment
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create attachment response failed: %v", err)
	}
	attachmentID, err := parseAttachmentIDFromName(created.Name)
	if err != nil {
		t.Fatalf("parse attachment id failed: %v", err)
	}

	updatePayload := map[string]any{
		"filename":                "scene.thumb.bin",
		"type":                    "image/jpeg",
		"content":                 base64.StdEncoding.EncodeToString(generateThumbnailTestJPEG(t, 320, 180)),
		"thumbnailBlobEncryption": "new-thumb",
	}
	updateBody, _ := json.Marshal(updatePayload)
	updateReq := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/attachments/"+strconv.FormatInt(attachmentID, 10)+"/thumbnail",
		bytes.NewReader(updateBody),
	)
	updateReq.Header.Set("Authorization", "Bearer "+token)
	updateReq.Header.Set("Content-Type", "application/json")
	updateResp, err := app.Test(updateReq, 5000)
	if err != nil {
		t.Fatalf("update attachment thumbnail request failed: %v", err)
	}
	defer updateResp.Body.Close()
	if updateResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(updateResp.Body)
		t.Fatalf("expected update 200, got %d body=%s", updateResp.StatusCode, string(body))
	}

	var updated apiAttachment
	if err := json.NewDecoder(updateResp.Body).Decode(&updated); err != nil {
		t.Fatalf("decode update attachment response failed: %v", err)
	}
	if updated.ThumbnailBlobEncryption != "new-thumb" {
		t.Fatalf("expected thumbnailBlobEncryption=new-thumb, got %q", updated.ThumbnailBlobEncryption)
	}
	if updated.ThumbnailFilename != "scene.thumb.bin" {
		t.Fatalf("expected thumbnail filename updated, got %q", updated.ThumbnailFilename)
	}
	if updated.ThumbnailName == "" {
		t.Fatalf("expected thumbnail name in response")
	}

	thumbnailReq := httptest.NewRequest(
		http.MethodGet,
		"/file/"+updated.ThumbnailName+"/"+updated.ThumbnailFilename,
		nil,
	)
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
}

func TestUpdateAttachmentThumbnailRoute_ForbiddenForNonCreator(t *testing.T) {
	app := newTestApp(t, true, true)
	creatorToken := "demo-token"
	otherToken := mustCreateAndSignInTestUser(t, app, "thumb-route-other")

	createPayload := map[string]any{
		"filename":             "scene.jpg",
		"type":                 "image/jpeg",
		"content":              base64.StdEncoding.EncodeToString(generateThumbnailTestJPEG(t, 400, 300)),
		"descriptorCiphertext": "scene-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "scene-wrapped-key",
			},
		}},
		"blobEncryption": "scene-blob",
	}
	createBody, _ := json.Marshal(createPayload)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+creatorToken)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create attachment request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected create 200, got %d body=%s", createResp.StatusCode, string(body))
	}
	var created apiAttachment
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create attachment response failed: %v", err)
	}
	attachmentID, err := parseAttachmentIDFromName(created.Name)
	if err != nil {
		t.Fatalf("parse attachment id failed: %v", err)
	}

	updatePayload := map[string]any{
		"filename": "scene.thumb.bin",
		"type":     "image/jpeg",
		"content":  base64.StdEncoding.EncodeToString(generateThumbnailTestJPEG(t, 100, 100)),
	}
	updateBody, _ := json.Marshal(updatePayload)
	updateReq := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/attachments/"+strconv.FormatInt(attachmentID, 10)+"/thumbnail",
		bytes.NewReader(updateBody),
	)
	updateReq.Header.Set("Authorization", "Bearer "+otherToken)
	updateReq.Header.Set("Content-Type", "application/json")
	updateResp, err := app.Test(updateReq, 5000)
	if err != nil {
		t.Fatalf("update attachment thumbnail request failed: %v", err)
	}
	defer updateResp.Body.Close()
	if updateResp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(updateResp.Body)
		t.Fatalf("expected 403, got %d body=%s", updateResp.StatusCode, string(body))
	}
}

func TestUpdateAttachmentThumbnailRoute_BadRequest(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createPayload := map[string]any{
		"filename":             "scene.jpg",
		"type":                 "image/jpeg",
		"content":              base64.StdEncoding.EncodeToString(generateThumbnailTestJPEG(t, 800, 600)),
		"descriptorCiphertext": "scene-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "scene-wrapped-key",
			},
		}},
		"blobEncryption": "scene-blob",
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
		t.Fatalf("expected create 200, got %d body=%s", createResp.StatusCode, string(body))
	}
	var created apiAttachment
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create attachment response failed: %v", err)
	}
	attachmentID, err := parseAttachmentIDFromName(created.Name)
	if err != nil {
		t.Fatalf("parse attachment id failed: %v", err)
	}

	cases := []struct {
		name    string
		content string
	}{
		{
			name:    "invalid base64",
			content: "not-base64",
		},
		{
			name:    "payload too large",
			content: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 8*1024*1024+1)),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updatePayload := map[string]any{
				"filename": "scene.thumb.bin",
				"type":     "image/jpeg",
				"content":  tc.content,
			}
			updateBody, _ := json.Marshal(updatePayload)
			updateReq := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/attachments/"+strconv.FormatInt(attachmentID, 10)+"/thumbnail",
				bytes.NewReader(updateBody),
			)
			updateReq.Header.Set("Authorization", "Bearer "+token)
			updateReq.Header.Set("Content-Type", "application/json")
			updateResp, err := app.Test(updateReq, 5000)
			if err != nil {
				t.Fatalf("update attachment thumbnail request failed: %v", err)
			}
			defer updateResp.Body.Close()
			if updateResp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(updateResp.Body)
				t.Fatalf("expected 400, got %d body=%s", updateResp.StatusCode, string(body))
			}
		})
	}
}

func parseAttachmentIDFromName(name string) (int64, error) {
	raw := strings.TrimSpace(name)
	raw = strings.TrimPrefix(raw, "attachments/")
	return strconv.ParseInt(raw, 10, 64)
}

func mustCreateAndSignInTestUser(t *testing.T, app *fiber.App, username string) string {
	t.Helper()

	createUserBody := map[string]any{
		"user": map[string]any{
			"username": username,
			"password": "register-password",
		},
	}
	createUserPayload, _ := json.Marshal(createUserBody)
	createUserReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createUserPayload))
	createUserReq.Header.Set("Content-Type", "application/json")
	createUserResp, err := app.Test(createUserReq, 5000)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer createUserResp.Body.Close()
	if createUserResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createUserResp.Body)
		t.Fatalf("expected create user 200, got %d body=%s", createUserResp.StatusCode, string(body))
	}

	signInBody := map[string]any{
		"passwordCredentials": map[string]any{
			"username": username,
			"password": "register-password",
		},
	}
	signInPayload, _ := json.Marshal(signInBody)
	signInReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/signin", bytes.NewReader(signInPayload))
	signInReq.Header.Set("Content-Type", "application/json")
	signInResp, err := app.Test(signInReq, 5000)
	if err != nil {
		t.Fatalf("signin request failed: %v", err)
	}
	defer signInResp.Body.Close()
	if signInResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(signInResp.Body)
		t.Fatalf("expected signin 200, got %d body=%s", signInResp.StatusCode, string(body))
	}

	var signInResult signInResponse
	if err := json.NewDecoder(signInResp.Body).Decode(&signInResult); err != nil {
		t.Fatalf("decode signin response failed: %v", err)
	}
	if strings.TrimSpace(signInResult.AccessToken) == "" {
		t.Fatalf("expected non-empty access token")
	}
	return signInResult.AccessToken
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
