package http

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestPatchMemo_CanClearCoordinatesWithNull(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	created := createMemoWithCoordinates(t, app, token, 31.2304, 121.4737)
	memoID := strings.TrimPrefix(created.Name, "memos/")
	if memoID == created.Name || memoID == "" {
		t.Fatalf("unexpected memo name: %q", created.Name)
	}

	patchPayload := map[string]any{
		"latitude":  nil,
		"longitude": nil,
	}
	patchBody, _ := json.Marshal(patchPayload)
	patchReq := httptest.NewRequest(http.MethodPatch, "/api/v1/memos/"+memoID, bytes.NewReader(patchBody))
	patchReq.Header.Set("Authorization", "Bearer "+token)
	patchReq.Header.Set("Content-Type", "application/json")

	patchResp, err := app.Test(patchReq, 5000)
	if err != nil {
		t.Fatalf("patch memo request failed: %v", err)
	}
	defer patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(patchResp.Body)
		t.Fatalf("expected 200, got %d body=%s", patchResp.StatusCode, string(body))
	}

	var updated apiMemo
	if err := json.NewDecoder(patchResp.Body).Decode(&updated); err != nil {
		t.Fatalf("decode patch memo response failed: %v", err)
	}
	if updated.Latitude != nil || updated.Longitude != nil {
		t.Fatalf("expected coordinates cleared to null, got latitude=%v longitude=%v", updated.Latitude, updated.Longitude)
	}
}

func TestPatchMemo_OmittedCoordinatesKeepOriginal(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	initialLatitude := 39.9042
	initialLongitude := 116.4074
	created := createMemoWithCoordinates(t, app, token, initialLatitude, initialLongitude)
	memoID := strings.TrimPrefix(created.Name, "memos/")
	if memoID == created.Name || memoID == "" {
		t.Fatalf("unexpected memo name: %q", created.Name)
	}

	patchPayload := map[string]any{
		"pinned": true,
	}
	patchBody, _ := json.Marshal(patchPayload)
	patchReq := httptest.NewRequest(http.MethodPatch, "/api/v1/memos/"+memoID, bytes.NewReader(patchBody))
	patchReq.Header.Set("Authorization", "Bearer "+token)
	patchReq.Header.Set("Content-Type", "application/json")

	patchResp, err := app.Test(patchReq, 5000)
	if err != nil {
		t.Fatalf("patch memo request failed: %v", err)
	}
	defer patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(patchResp.Body)
		t.Fatalf("expected 200, got %d body=%s", patchResp.StatusCode, string(body))
	}

	var updated apiMemo
	if err := json.NewDecoder(patchResp.Body).Decode(&updated); err != nil {
		t.Fatalf("decode patch memo response failed: %v", err)
	}
	if !updated.Pinned {
		t.Fatalf("expected pinned=true after patch")
	}
	if updated.Latitude == nil || !almostEqual(*updated.Latitude, initialLatitude) {
		t.Fatalf("expected latitude unchanged %v, got %v", initialLatitude, updated.Latitude)
	}
	if updated.Longitude == nil || !almostEqual(*updated.Longitude, initialLongitude) {
		t.Fatalf("expected longitude unchanged %v, got %v", initialLongitude, updated.Longitude)
	}
}

func createMemoWithCoordinates(t *testing.T, app *fiber.App, token string, latitude float64, longitude float64) apiMemo {
	t.Helper()

	createPayload := map[string]any{
		"content":     "memo with coordinates",
		"visibility":  "PRIVATE",
		"attachments": []any{},
		"latitude":    latitude,
		"longitude":   longitude,
	}
	createBody, _ := json.Marshal(createPayload)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/memos", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create memo request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected create memo 200, got %d body=%s", createResp.StatusCode, string(body))
	}

	var created apiMemo
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create memo response failed: %v", err)
	}
	if created.Name == "" {
		t.Fatalf("expected created memo name")
	}
	return created
}

func almostEqual(a float64, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
