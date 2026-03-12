package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetCurrentGroupKeyVersion_ReturnsNotFoundWhenMissing(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createBody, _ := json.Marshal(map[string]any{
		"name":        "group without key version",
		"description": "no key yet",
	})
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create group request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected create group 200, got %d body=%s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created apiGroup
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create group response failed: %v", err)
	}

	groupID := created.Name
	if idx := strings.LastIndex(groupID, "/"); idx >= 0 {
		groupID = groupID[idx+1:]
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/groups/"+groupID+"/keyVersions/current", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("get current group key version request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 404, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}
