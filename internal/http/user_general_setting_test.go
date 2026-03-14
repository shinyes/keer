package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetUserGeneralSetting_Defaults(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/1/settings/GENERAL", nil)
	req.Header.Set("Authorization", "Bearer demo-token")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("get general setting request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}

	var payload userSettingResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode general setting response failed: %v", err)
	}
	if payload.GeneralSetting.MemoVisibility != "PRIVATE" {
		t.Fatalf("expected default visibility PRIVATE, got %q", payload.GeneralSetting.MemoVisibility)
	}
	if payload.GeneralSetting.MemoEditGesture != "NONE" {
		t.Fatalf("expected default edit gesture NONE, got %q", payload.GeneralSetting.MemoEditGesture)
	}
	if len(payload.GeneralSetting.MemoColumns) != 0 {
		t.Fatalf("expected empty default memo columns, got %d", len(payload.GeneralSetting.MemoColumns))
	}
}

func TestPutUserGeneralSetting_RoundTrip(t *testing.T) {
	app := newTestApp(t, true, true)

	body := map[string]any{
		"generalSetting": map[string]any{
			"memoVisibility":  "PROTECTED",
			"memoEditGesture": "DOUBLE",
			"memoColumns": []map[string]any{
				{
					"id":              "work",
					"name":            "Work",
					"requiredTags":    []string{"work", "urgent", "work", " "},
					"visibleInDrawer": true,
				},
				{
					"id":              "life",
					"name":            "Life",
					"requiredTags":    []string{"life"},
					"visibleInDrawer": false,
				},
			},
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/users/1/settings/GENERAL", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer demo-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("put general setting request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}

	var putPayload userSettingResponse
	if err := json.NewDecoder(resp.Body).Decode(&putPayload); err != nil {
		t.Fatalf("decode put general setting response failed: %v", err)
	}
	if putPayload.GeneralSetting.MemoVisibility != "PROTECTED" {
		t.Fatalf("expected visibility PROTECTED, got %q", putPayload.GeneralSetting.MemoVisibility)
	}
	if putPayload.GeneralSetting.MemoEditGesture != "DOUBLE" {
		t.Fatalf("expected edit gesture DOUBLE, got %q", putPayload.GeneralSetting.MemoEditGesture)
	}
	if len(putPayload.GeneralSetting.MemoColumns) != 2 {
		t.Fatalf("expected 2 memo columns, got %d", len(putPayload.GeneralSetting.MemoColumns))
	}
	if got := putPayload.GeneralSetting.MemoColumns[0].RequiredTags; len(got) != 2 || got[0] != "work" || got[1] != "urgent" {
		t.Fatalf("expected normalized tags [work urgent], got %#v", got)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/users/1/settings/GENERAL", nil)
	getReq.Header.Set("Authorization", "Bearer demo-token")
	getResp, err := app.Test(getReq, 5000)
	if err != nil {
		t.Fatalf("get general setting request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("expected 200, got %d body=%s", getResp.StatusCode, string(body))
	}

	var getPayload userSettingResponse
	if err := json.NewDecoder(getResp.Body).Decode(&getPayload); err != nil {
		t.Fatalf("decode get general setting response failed: %v", err)
	}
	if getPayload.GeneralSetting.MemoEditGesture != "DOUBLE" {
		t.Fatalf("expected persisted edit gesture DOUBLE, got %q", getPayload.GeneralSetting.MemoEditGesture)
	}
	if len(getPayload.GeneralSetting.MemoColumns) != 2 {
		t.Fatalf("expected persisted 2 memo columns, got %d", len(getPayload.GeneralSetting.MemoColumns))
	}
}

func TestPutUserGeneralSetting_ForbiddenForOtherUser(t *testing.T) {
	app := newTestApp(t, true, true)

	createBody := map[string]any{
		"user": map[string]any{
			"username": "general02",
			"password": "register-password",
		},
	}
	createPayload, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createPayload))
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer createResp.Body.Close()

	body := map[string]any{
		"generalSetting": map[string]any{
			"memoVisibility":  "PRIVATE",
			"memoEditGesture": "NONE",
			"memoColumns":     []any{},
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/users/2/settings/GENERAL", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer demo-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("put general setting request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403, got %d body=%s", resp.StatusCode, string(body))
	}
}

func TestPutUserGeneralSetting_InvalidColumns(t *testing.T) {
	app := newTestApp(t, true, true)

	body := map[string]any{
		"generalSetting": map[string]any{
			"memoVisibility":  "PRIVATE",
			"memoEditGesture": "SINGLE",
			"memoColumns": []map[string]any{
				{
					"id":              "dup",
					"name":            "One",
					"requiredTags":    []string{"a"},
					"visibleInDrawer": true,
				},
				{
					"id":              "dup",
					"name":            "Two",
					"requiredTags":    []string{"b"},
					"visibleInDrawer": true,
				},
			},
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/users/1/settings/GENERAL", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer demo-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("put general setting request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, string(body))
	}
}
