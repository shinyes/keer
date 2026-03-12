package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUserEncryptionSettingResponse_IncludesEmptySharingFields(t *testing.T) {
	app := newTestApp(t, true, true)

	requestBody := map[string]any{
		"encryptionSetting": map[string]any{
			"recoveryBundle": map[string]any{
				"version":           1,
				"kdfAlgorithm":      "PBKDF2_HMAC_SHA256",
				"kdfSalt":           "salt",
				"kdfIterations":     210000,
				"wrapAlgorithm":     "AES_GCM",
				"wrappedAccountKey": "wrapped-account-key",
			},
			"sharingPublicKey":         "",
			"wrappedSharingPrivateKey": "",
			"keyVersion":               1,
			"algorithms":               "",
		},
	}
	payload, _ := json.Marshal(requestBody)

	putReq := httptest.NewRequest(http.MethodPut, "/api/v1/users/1/settings/ENCRYPTION", bytes.NewReader(payload))
	putReq.Header.Set("Authorization", "Bearer demo-token")
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := app.Test(putReq, 5000)
	if err != nil {
		t.Fatalf("put encryption setting request failed: %v", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("expected 200, got %d body=%s", putResp.StatusCode, string(body))
	}

	var putPayload map[string]any
	if err := json.NewDecoder(putResp.Body).Decode(&putPayload); err != nil {
		t.Fatalf("decode put encryption setting response failed: %v", err)
	}
	encryptionSetting, ok := putPayload["encryptionSetting"].(map[string]any)
	if !ok {
		t.Fatalf("expected encryptionSetting object in put response")
	}
	if _, exists := encryptionSetting["sharingPublicKey"]; !exists {
		t.Fatalf("expected sharingPublicKey in put response")
	}
	if _, exists := encryptionSetting["wrappedSharingPrivateKey"]; !exists {
		t.Fatalf("expected wrappedSharingPrivateKey in put response")
	}
	if _, exists := encryptionSetting["algorithms"]; !exists {
		t.Fatalf("expected algorithms in put response")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/users/1/settings/ENCRYPTION", nil)
	getReq.Header.Set("Authorization", "Bearer demo-token")
	getResp, err := app.Test(getReq, 5000)
	if err != nil {
		t.Fatalf("get encryption setting request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("expected 200, got %d body=%s", getResp.StatusCode, string(body))
	}

	var getPayload map[string]any
	if err := json.NewDecoder(getResp.Body).Decode(&getPayload); err != nil {
		t.Fatalf("decode get encryption setting response failed: %v", err)
	}
	encryptionSetting, ok = getPayload["encryptionSetting"].(map[string]any)
	if !ok {
		t.Fatalf("expected encryptionSetting object in get response")
	}
	if _, exists := encryptionSetting["sharingPublicKey"]; !exists {
		t.Fatalf("expected sharingPublicKey in get response")
	}
	if _, exists := encryptionSetting["wrappedSharingPrivateKey"]; !exists {
		t.Fatalf("expected wrappedSharingPrivateKey in get response")
	}
	if _, exists := encryptionSetting["algorithms"]; !exists {
		t.Fatalf("expected algorithms in get response")
	}
}
