package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCleanupOrphanFilesRoute_AdminCanRun(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/storage/cleanup-orphans", bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer demo-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("cleanup orphan files request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}

	var payload storageCleanupResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode cleanup orphan files response failed: %v", err)
	}
	if payload.Cleanup.ScannedKeys != 0 || payload.Cleanup.DeletedKeys != 0 || payload.Cleanup.FailedKeys != 0 {
		t.Fatalf("expected zero cleanup counts, got %#v", payload.Cleanup)
	}
}

func TestCleanupOrphanFilesRoute_ForbiddenForNonAdmin(t *testing.T) {
	app := newTestApp(t, true, false)

	createAdminBody := map[string]any{
		"user": map[string]any{
			"username": "owner01",
			"password": "register-password",
		},
	}
	createAdminPayload, _ := json.Marshal(createAdminBody)
	createAdminReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createAdminPayload))
	createAdminReq.Header.Set("Content-Type", "application/json")
	createAdminResp, err := app.Test(createAdminReq, 5000)
	if err != nil {
		t.Fatalf("create admin user request failed: %v", err)
	}
	defer createAdminResp.Body.Close()
	if createAdminResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createAdminResp.Body)
		t.Fatalf("expected create admin 200, got %d body=%s", createAdminResp.StatusCode, string(body))
	}

	createUserBody := map[string]any{
		"user": map[string]any{
			"username": "member01",
			"password": "register-password",
		},
	}
	createUserPayload, _ := json.Marshal(createUserBody)
	createUserReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createUserPayload))
	createUserReq.Header.Set("Content-Type", "application/json")
	createUserResp, err := app.Test(createUserReq, 5000)
	if err != nil {
		t.Fatalf("create member user request failed: %v", err)
	}
	defer createUserResp.Body.Close()
	if createUserResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createUserResp.Body)
		t.Fatalf("expected create member 200, got %d body=%s", createUserResp.StatusCode, string(body))
	}

	signInBody := map[string]any{
		"passwordCredentials": map[string]any{
			"username": "member01",
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

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/storage/cleanup-orphans", bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer "+signInResult.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("cleanup orphan files request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403, got %d body=%s", resp.StatusCode, string(body))
	}
}
