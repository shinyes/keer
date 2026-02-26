package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type errorEnvelope struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

func TestAuthError_IncludesCodeAndRequestID(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("auth/me request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	responseRequestID := strings.TrimSpace(resp.Header.Get("X-Request-ID"))
	if responseRequestID == "" {
		t.Fatalf("expected X-Request-ID header")
	}

	var body errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body failed: %v", err)
	}
	if body.Code != "UNAUTHORIZED" {
		t.Fatalf("expected code=UNAUTHORIZED, got %q", body.Code)
	}
	if body.Message != "missing authorization" {
		t.Fatalf("expected message missing authorization, got %q", body.Message)
	}
	if strings.TrimSpace(body.RequestID) == "" {
		t.Fatalf("expected requestId in error body")
	}
	if body.RequestID != responseRequestID {
		t.Fatalf("requestId mismatch header=%q body=%q", responseRequestID, body.RequestID)
	}
}

func TestBadRequest_IncludesCodeAndRequestID(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/signin", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("signin request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	responseRequestID := strings.TrimSpace(resp.Header.Get("X-Request-ID"))
	if responseRequestID == "" {
		t.Fatalf("expected X-Request-ID header")
	}

	var body errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body failed: %v", err)
	}
	if body.Code != "BAD_REQUEST" {
		t.Fatalf("expected code=BAD_REQUEST, got %q", body.Code)
	}
	if body.Message != "passwordCredentials is required" {
		t.Fatalf("expected message passwordCredentials is required, got %q", body.Message)
	}
	if strings.TrimSpace(body.RequestID) == "" {
		t.Fatalf("expected requestId in error body")
	}
	if body.RequestID != responseRequestID {
		t.Fatalf("requestId mismatch header=%q body=%q", responseRequestID, body.RequestID)
	}
}
