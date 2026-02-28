package http

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

func TestListMemoChanges_ReturnsChangedAndDeletedMemos(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	beforeCreate := time.Now().UTC().Format(time.RFC3339Nano)
	created := createMemoWithCoordinates(t, app, token, 40.7128, -74.0060)

	createChangesResp := getMemoChanges(t, app, token, beforeCreate)
	if len(createChangesResp.Memos) == 0 {
		t.Fatalf("expected create window to include changed memos")
	}
	if createChangesResp.SyncAnchor == "" {
		t.Fatalf("expected syncAnchor to be populated")
	}

	memoID := strings.TrimPrefix(created.Name, "memos/")
	if memoID == "" || memoID == created.Name {
		t.Fatalf("unexpected memo name: %q", created.Name)
	}

	beforeDelete := time.Now().UTC().Format(time.RFC3339Nano)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/memos/"+memoID, nil)
	deleteReq.Header.Set("Authorization", "Bearer "+token)
	deleteResp, err := app.Test(deleteReq, 5000)
	if err != nil {
		t.Fatalf("delete memo request failed: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected delete memo 204, got %d body=%s", deleteResp.StatusCode, string(body))
	}

	deleteChangesResp := getMemoChanges(t, app, token, beforeDelete)
	if !containsMemoName(deleteChangesResp.DeletedMemoNames, created.Name) {
		t.Fatalf("expected deleted memo names to contain %q, got %v", created.Name, deleteChangesResp.DeletedMemoNames)
	}
}

func getMemoChanges(t *testing.T, app *fiber.App, token string, since string) listMemoChangesResponse {
	t.Helper()
	endpoint := "/api/v1/memos/changes?since=" + url.QueryEscape(since)
	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("list memo changes request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected list memo changes 200, got %d body=%s", resp.StatusCode, string(body))
	}

	var out listMemoChangesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode memo changes response failed: %v", err)
	}
	return out
}

func containsMemoName(items []string, name string) bool {
	for _, item := range items {
		if item == name {
			return true
		}
	}
	return false
}
