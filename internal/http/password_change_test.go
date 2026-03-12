package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestChangePasswordEndpoint(t *testing.T) {
	app := newTestApp(t, true, false)

	createBody := map[string]any{
		"user": map[string]any{
			"username":    "pwchange01",
			"displayName": "Password Change",
			"password":    "old-password",
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
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("expected create user 200, got %d", createResp.StatusCode)
	}

	oldToken := signInAndReturnToken(t, app, "pwchange01", "old-password")

	initialSettingBody := map[string]any{
		"encryptionSetting": map[string]any{
			"recoveryBundle": map[string]any{
				"version":           2,
				"kdfAlgorithm":      "ARGON2ID",
				"kdfSalt":           "salt-old",
				"kdfTimeCost":       3,
				"kdfMemoryKiB":      32768,
				"kdfParallelism":    1,
				"wrapAlgorithm":     "AES_GCM",
				"wrappedAccountKey": "iv-old:cipher-old",
			},
			"sharingPublicKey":         "sharing-public",
			"wrappedSharingPrivateKey": "sharing-private",
			"keyVersion":               1,
			"algorithms":               "{\"sharing\":\"rsa-oaep-sha256\"}",
		},
	}
	initialSettingPayload, _ := json.Marshal(initialSettingBody)
	initialSettingReq := httptest.NewRequest(http.MethodPut, "/api/v1/users/1/settings/ENCRYPTION", bytes.NewReader(initialSettingPayload))
	initialSettingReq.Header.Set("Authorization", "Bearer "+oldToken)
	initialSettingReq.Header.Set("Content-Type", "application/json")
	initialSettingResp, err := app.Test(initialSettingReq, 5000)
	if err != nil {
		t.Fatalf("put encryption setting request failed: %v", err)
	}
	defer initialSettingResp.Body.Close()
	if initialSettingResp.StatusCode != http.StatusOK {
		t.Fatalf("expected put encryption setting 200, got %d", initialSettingResp.StatusCode)
	}

	changeBody := map[string]any{
		"currentPassword": "old-password",
		"newPassword":     "new-password",
		"encryptionSetting": map[string]any{
			"recoveryBundle": map[string]any{
				"version":           2,
				"kdfAlgorithm":      "ARGON2ID",
				"kdfSalt":           "salt-new",
				"kdfTimeCost":       3,
				"kdfMemoryKiB":      32768,
				"kdfParallelism":    1,
				"wrapAlgorithm":     "AES_GCM",
				"wrappedAccountKey": "iv-new:cipher-new",
			},
			"sharingPublicKey":         "sharing-public",
			"wrappedSharingPrivateKey": "sharing-private",
			"keyVersion":               1,
			"algorithms":               "{\"sharing\":\"rsa-oaep-sha256\"}",
		},
	}
	changePayload, _ := json.Marshal(changeBody)
	changeReq := httptest.NewRequest(http.MethodPut, "/api/v1/users/1/password", bytes.NewReader(changePayload))
	changeReq.Header.Set("Authorization", "Bearer "+oldToken)
	changeReq.Header.Set("Content-Type", "application/json")
	changeResp, err := app.Test(changeReq, 5000)
	if err != nil {
		t.Fatalf("change password request failed: %v", err)
	}
	defer changeResp.Body.Close()
	if changeResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected change password 204, got %d", changeResp.StatusCode)
	}

	oldPasswordSignInBody := map[string]any{
		"passwordCredentials": map[string]any{
			"username": "pwchange01",
			"password": "old-password",
		},
	}
	oldPasswordSignInPayload, _ := json.Marshal(oldPasswordSignInBody)
	oldPasswordSignInReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/signin", bytes.NewReader(oldPasswordSignInPayload))
	oldPasswordSignInReq.Header.Set("Content-Type", "application/json")
	oldPasswordSignInResp, err := app.Test(oldPasswordSignInReq, 5000)
	if err != nil {
		t.Fatalf("signin with old password request failed: %v", err)
	}
	defer oldPasswordSignInResp.Body.Close()
	if oldPasswordSignInResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected old password signin 400, got %d", oldPasswordSignInResp.StatusCode)
	}

	newToken := signInAndReturnToken(t, app, "pwchange01", "new-password")
	if newToken == "" {
		t.Fatalf("expected non-empty token after password change")
	}

	getSettingReq := httptest.NewRequest(http.MethodGet, "/api/v1/users/1/settings/ENCRYPTION", nil)
	getSettingReq.Header.Set("Authorization", "Bearer "+newToken)
	getSettingResp, err := app.Test(getSettingReq, 5000)
	if err != nil {
		t.Fatalf("get encryption setting request failed: %v", err)
	}
	defer getSettingResp.Body.Close()
	if getSettingResp.StatusCode != http.StatusOK {
		t.Fatalf("expected get encryption setting 200, got %d", getSettingResp.StatusCode)
	}

	var getSettingResult userEncryptionSettingResponse
	if err := json.NewDecoder(getSettingResp.Body).Decode(&getSettingResult); err != nil {
		t.Fatalf("decode get encryption setting response failed: %v", err)
	}
	if got := getSettingResult.EncryptionSetting.RecoveryBundle.WrappedAccountKey; got != "iv-new:cipher-new" {
		t.Fatalf("expected wrapped account key to be updated, got %q", got)
	}
}

func signInAndReturnToken(t *testing.T, app *fiber.App, username string, password string) string {
	t.Helper()

	signInBody := map[string]any{
		"passwordCredentials": map[string]any{
			"username": username,
			"password": password,
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
		t.Fatalf("expected signin 200, got %d", signInResp.StatusCode)
	}

	var signInResult signInResponse
	if err := json.NewDecoder(signInResp.Body).Decode(&signInResult); err != nil {
		t.Fatalf("decode signin response failed: %v", err)
	}
	return signInResult.AccessToken
}
