package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/markdown"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

func TestCreateUserEndpoint_FirstUserAdmin(t *testing.T) {
	app := newTestApp(t, false, false)

	body := map[string]any{
		"user": map[string]any{
			"username":    "register01",
			"displayName": "Register User",
			"password":    "register-password",
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var created apiUser
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create user response: %v", err)
	}
	if created.Role != "ADMIN" {
		t.Fatalf("expected first user role=ADMIN, got %s", created.Role)
	}
	if created.Username != "register01" {
		t.Fatalf("unexpected username: %s", created.Username)
	}
}

func TestCreateUserEndpoint_DisabledForSecondUser(t *testing.T) {
	app := newTestApp(t, false, true)

	body := map[string]any{
		"user": map[string]any{
			"username": "register02",
			"password": "register-password",
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestCreateUserEndpoint_ValidateOnly(t *testing.T) {
	app := newTestApp(t, true, false)

	body := map[string]any{
		"user": map[string]any{
			"username": "preview01",
			"password": "register-password",
		},
		"validateOnly": true,
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var created apiUser
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create user response: %v", err)
	}
	if created.Name != "" {
		t.Fatalf("expected empty name in validateOnly mode, got %s", created.Name)
	}
}

func TestSignInEndpoint_ThenAuthMe(t *testing.T) {
	app := newTestApp(t, true, false)

	createBody := map[string]any{
		"user": map[string]any{
			"username": "signin01",
			"password": "register-password",
		},
	}
	createPayload, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createPayload))
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := app.Test(createReq)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("expected create user 200, got %d", createResp.StatusCode)
	}

	signInBody := map[string]any{
		"passwordCredentials": map[string]any{
			"username": "signin01",
			"password": "register-password",
		},
	}
	signInPayload, _ := json.Marshal(signInBody)
	signInReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/signin", bytes.NewReader(signInPayload))
	signInReq.Header.Set("Content-Type", "application/json")
	signInResp, err := app.Test(signInReq)
	if err != nil {
		t.Fatalf("signin request failed: %v", err)
	}
	defer signInResp.Body.Close()
	if signInResp.StatusCode != http.StatusOK {
		t.Fatalf("expected signin 200, got %d", signInResp.StatusCode)
	}

	var signInResult signInResponse
	if err := json.NewDecoder(signInResp.Body).Decode(&signInResult); err != nil {
		t.Fatalf("decode signin response failed: %v", err)
	}
	if signInResult.AccessToken == "" {
		t.Fatalf("expected non-empty accessToken")
	}
	if signInResult.User.Username != "signin01" {
		t.Fatalf("expected username signin01, got %s", signInResult.User.Username)
	}

	meReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	meReq.Header.Set("Authorization", "Bearer "+signInResult.AccessToken)
	meResp, err := app.Test(meReq)
	if err != nil {
		t.Fatalf("auth/me request failed: %v", err)
	}
	defer meResp.Body.Close()
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("expected auth/me 200, got %d", meResp.StatusCode)
	}
}

func TestGetUserByUsernameEndpoint(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/demo", nil)
	req.Header.Set("Authorization", "Bearer demo-token")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("get user by username request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var user apiUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		t.Fatalf("decode user response failed: %v", err)
	}
	if user.Username != "demo" {
		t.Fatalf("expected username demo, got %s", user.Username)
	}
}

func TestGetUserStatsByUsernameEndpoint(t *testing.T) {
	app := newTestApp(t, true, true)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/demo:getStats", nil)
	req.Header.Set("Authorization", "Bearer demo-token")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("get user stats by username request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestCreateUserEndpoint_RegistrationSettingOverridesEnv(t *testing.T) {
	app, userService := newTestAppWithUserService(t, true, false)

	firstBody := map[string]any{
		"user": map[string]any{
			"username": "owner01",
			"password": "register-password",
		},
	}
	firstPayload, _ := json.Marshal(firstBody)
	firstReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(firstPayload))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp, err := app.Test(firstReq)
	if err != nil {
		t.Fatalf("create first user request failed: %v", err)
	}
	defer firstResp.Body.Close()
	if firstResp.StatusCode != http.StatusOK {
		t.Fatalf("expected first create user 200, got %d", firstResp.StatusCode)
	}

	if err := userService.SetAllowRegistration(context.Background(), false); err != nil {
		t.Fatalf("SetAllowRegistration(false) error = %v", err)
	}

	blockedBody := map[string]any{
		"user": map[string]any{
			"username": "blocked02",
			"password": "register-password",
		},
	}
	blockedPayload, _ := json.Marshal(blockedBody)
	blockedReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(blockedPayload))
	blockedReq.Header.Set("Content-Type", "application/json")
	blockedResp, err := app.Test(blockedReq)
	if err != nil {
		t.Fatalf("create blocked user request failed: %v", err)
	}
	defer blockedResp.Body.Close()
	if blockedResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected blocked create user 403, got %d", blockedResp.StatusCode)
	}

	if err := userService.SetAllowRegistration(context.Background(), true); err != nil {
		t.Fatalf("SetAllowRegistration(true) error = %v", err)
	}

	allowedBody := map[string]any{
		"user": map[string]any{
			"username": "allowed03",
			"password": "register-password",
		},
	}
	allowedPayload, _ := json.Marshal(allowedBody)
	allowedReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(allowedPayload))
	allowedReq.Header.Set("Content-Type", "application/json")
	allowedResp, err := app.Test(allowedReq)
	if err != nil {
		t.Fatalf("create allowed user request failed: %v", err)
	}
	defer allowedResp.Body.Close()
	if allowedResp.StatusCode != http.StatusOK {
		t.Fatalf("expected allowed create user 200, got %d", allowedResp.StatusCode)
	}
}

func newTestApp(t *testing.T, allowRegistration bool, withBootstrap bool) *fiber.App {
	app, _ := newTestAppWithUserService(t, allowRegistration, withBootstrap)
	return app
}

func newTestAppWithUserService(t *testing.T, allowRegistration bool, withBootstrap bool) (*fiber.App, *service.UserService) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "http_test.db")
	sqliteDB, err := db.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteDB.Close()
	})
	if err := db.Migrate(sqliteDB); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	sqlStore := store.New(sqliteDB)
	userService := service.NewUserService(sqlStore)
	if withBootstrap {
		if err := userService.EnsureBootstrap(context.Background(), "demo", "demo-token"); err != nil {
			t.Fatalf("EnsureBootstrap() error = %v", err)
		}
	}
	markdownSvc := markdown.NewService()
	memoService := service.NewMemoService(sqlStore, markdownSvc)
	localStore, err := storage.NewLocalStore(filepath.Join(t.TempDir(), "uploads"))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	attachmentService := service.NewAttachmentService(sqlStore, localStore)

	cfg := config.Config{
		Version:           "0.26.1",
		AllowRegistration: allowRegistration,
	}
	return NewRouter(cfg, userService, memoService, attachmentService), userService
}
