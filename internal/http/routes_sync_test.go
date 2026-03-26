package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSyncPullEndpoint_Removed(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync/pull", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer demo-token")

	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("legacy sync pull request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for removed endpoint, got %d", resp.StatusCode)
	}
}
