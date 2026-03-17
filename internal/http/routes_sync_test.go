package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSyncPullEndpoint_InitialCursor(t *testing.T) {
	app := newTestApp(t, true, true)

	body := syncPullRequest{
		Cursor:  "0",
		Domains: []string{"MEMOS", "SETTINGS"},
		Limit:   50,
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync/pull", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer demo-token")

	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("sync pull request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result syncPullResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode sync pull response failed: %v", err)
	}
	if result.NextCursor == "" {
		t.Fatalf("expected non-empty nextCursor")
	}
	if result.Patches.Memos.Upserts == nil {
		t.Fatalf("expected memos patch list to be initialized")
	}
	if result.Patches.Settings.GeneralSetting == nil {
		t.Fatalf("expected settings patch to include generalSetting on initial pull")
	}
}

func TestSyncPullEndpoint_InvalidCursor(t *testing.T) {
	app := newTestApp(t, true, true)

	body := syncPullRequest{
		Cursor:  "abc",
		Domains: []string{"MEMOS"},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync/pull", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer demo-token")

	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("sync pull request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}
