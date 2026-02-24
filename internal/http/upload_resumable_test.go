package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAttachmentResumableUploadFlow(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createPayload := map[string]any{
		"filename": "video.mp4",
		"type":     "video/mp4",
		"size":     12,
	}
	createBody, _ := json.Marshal(createPayload)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments/uploads", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create upload session request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d body=%s", createResp.StatusCode, string(body))
	}

	var session attachmentUploadSessionResponse
	if err := json.NewDecoder(createResp.Body).Decode(&session); err != nil {
		t.Fatalf("decode create upload session response failed: %v", err)
	}
	if session.UploadID == "" {
		t.Fatalf("expected non-empty upload id")
	}

	chunk1 := []byte("hello ")
	patch1Req := httptest.NewRequest(http.MethodPatch, "/api/v1/attachments/uploads/"+session.UploadID, bytes.NewReader(chunk1))
	patch1Req.Header.Set("Authorization", "Bearer "+token)
	patch1Req.Header.Set("Upload-Offset", "0")
	patch1Resp, err := app.Test(patch1Req, 5000)
	if err != nil {
		t.Fatalf("patch chunk #1 request failed: %v", err)
	}
	defer patch1Resp.Body.Close()
	if patch1Resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(patch1Resp.Body)
		t.Fatalf("expected 204, got %d body=%s", patch1Resp.StatusCode, string(body))
	}
	if got := patch1Resp.Header.Get("Upload-Offset"); got != "6" {
		t.Fatalf("expected Upload-Offset=6, got %s", got)
	}

	wrongOffsetReq := httptest.NewRequest(http.MethodPatch, "/api/v1/attachments/uploads/"+session.UploadID, bytes.NewReader([]byte("x")))
	wrongOffsetReq.Header.Set("Authorization", "Bearer "+token)
	wrongOffsetReq.Header.Set("Upload-Offset", "0")
	wrongOffsetResp, err := app.Test(wrongOffsetReq, 5000)
	if err != nil {
		t.Fatalf("patch wrong offset request failed: %v", err)
	}
	defer wrongOffsetResp.Body.Close()
	if wrongOffsetResp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(wrongOffsetResp.Body)
		t.Fatalf("expected 409, got %d body=%s", wrongOffsetResp.StatusCode, string(body))
	}
	if got := wrongOffsetResp.Header.Get("Upload-Offset"); got != "6" {
		t.Fatalf("expected mismatch Upload-Offset=6, got %s", got)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/api/v1/attachments/uploads/"+session.UploadID, nil)
	headReq.Header.Set("Authorization", "Bearer "+token)
	headResp, err := app.Test(headReq, 5000)
	if err != nil {
		t.Fatalf("head upload session request failed: %v", err)
	}
	defer headResp.Body.Close()
	if headResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected HEAD 204, got %d", headResp.StatusCode)
	}
	if got := headResp.Header.Get("Upload-Offset"); got != "6" {
		t.Fatalf("expected HEAD Upload-Offset=6, got %s", got)
	}

	chunk2 := []byte("world!")
	patch2Req := httptest.NewRequest(http.MethodPatch, "/api/v1/attachments/uploads/"+session.UploadID, bytes.NewReader(chunk2))
	patch2Req.Header.Set("Authorization", "Bearer "+token)
	patch2Req.Header.Set("Upload-Offset", "6")
	patch2Resp, err := app.Test(patch2Req, 5000)
	if err != nil {
		t.Fatalf("patch chunk #2 request failed: %v", err)
	}
	defer patch2Resp.Body.Close()
	if patch2Resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(patch2Resp.Body)
		t.Fatalf("expected patch #2 204, got %d body=%s", patch2Resp.StatusCode, string(body))
	}
	if got := patch2Resp.Header.Get("Upload-Offset"); got != "12" {
		t.Fatalf("expected Upload-Offset=12, got %s", got)
	}

	completeReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments/uploads/"+session.UploadID+"/complete", nil)
	completeReq.Header.Set("Authorization", "Bearer "+token)
	completeResp, err := app.Test(completeReq, 5000)
	if err != nil {
		t.Fatalf("complete upload request failed: %v", err)
	}
	defer completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("expected complete 200, got %d body=%s", completeResp.StatusCode, string(body))
	}

	var attachment apiAttachment
	if err := json.NewDecoder(completeResp.Body).Decode(&attachment); err != nil {
		t.Fatalf("decode complete response failed: %v", err)
	}
	if attachment.Name == "" {
		t.Fatalf("expected attachment name")
	}
	if attachment.Filename != "video.mp4" {
		t.Fatalf("expected filename video.mp4, got %s", attachment.Filename)
	}
	if attachment.Size != "12" {
		t.Fatalf("expected size=12, got %s", attachment.Size)
	}
}
