package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestDirectGroupRoute_HasUnreadAndMarkRead(t *testing.T) {
	app, _ := newTestAppWithUserService(t, true, true)

	createUserBody, _ := json.Marshal(map[string]any{
		"user": map[string]any{
			"username": "friend-read",
			"password": "register-password",
		},
	})
	createUserReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createUserBody))
	createUserReq.Header.Set("Content-Type", "application/json")
	createUserResp, err := app.Test(createUserReq, 5000)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	createUserResp.Body.Close()
	if createUserResp.StatusCode != http.StatusOK {
		t.Fatalf("expected create user 200, got %d", createUserResp.StatusCode)
	}

	addFriendBody, _ := json.Marshal(map[string]any{"user": "friend-read"})
	addFriendReq := httptest.NewRequest(http.MethodPost, "/api/v1/friends", bytes.NewReader(addFriendBody))
	addFriendReq.Header.Set("Authorization", "Bearer demo-token")
	addFriendReq.Header.Set("Content-Type", "application/json")
	addFriendResp, err := app.Test(addFriendReq, 5000)
	if err != nil {
		t.Fatalf("add friend request failed: %v", err)
	}
	addFriendResp.Body.Close()
	if addFriendResp.StatusCode != http.StatusOK {
		t.Fatalf("expected add friend 200, got %d", addFriendResp.StatusCode)
	}

	createDirectBody, _ := json.Marshal(map[string]any{"user": "friend-read"})
	createDirectReq := httptest.NewRequest(http.MethodPost, "/api/v1/directs", bytes.NewReader(createDirectBody))
	createDirectReq.Header.Set("Authorization", "Bearer demo-token")
	createDirectReq.Header.Set("Content-Type", "application/json")
	createDirectResp, err := app.Test(createDirectReq, 5000)
	if err != nil {
		t.Fatalf("create direct request failed: %v", err)
	}
	defer createDirectResp.Body.Close()
	if createDirectResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createDirectResp.Body)
		t.Fatalf("expected create direct 200, got %d body=%s", createDirectResp.StatusCode, strings.TrimSpace(string(body)))
	}
	var directGroup apiGroup
	if err := json.NewDecoder(createDirectResp.Body).Decode(&directGroup); err != nil {
		t.Fatalf("decode direct group failed: %v", err)
	}
	groupID := directGroup.Name[strings.LastIndex(directGroup.Name, "/")+1:]

	createRegularGroupBody, _ := json.Marshal(map[string]any{
		"name":        "older-activity",
		"description": "baseline group",
	})
	createRegularGroupReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(createRegularGroupBody))
	createRegularGroupReq.Header.Set("Authorization", "Bearer demo-token")
	createRegularGroupReq.Header.Set("Content-Type", "application/json")
	createRegularGroupResp, err := app.Test(createRegularGroupReq, 5000)
	if err != nil {
		t.Fatalf("create regular group request failed: %v", err)
	}
	createRegularGroupResp.Body.Close()
	if createRegularGroupResp.StatusCode != http.StatusOK {
		t.Fatalf("expected create regular group 200, got %d", createRegularGroupResp.StatusCode)
	}

	friendToken := signInTestUser(t, app, "friend-read", "register-password")

	createMessageBody, _ := json.Marshal(map[string]any{
		"encryptedPayload": "ciphertext",
		"payloadEnvelope": map[string]any{
			"wrappedKeys": []map[string]any{
				{
					"slotType":      "account_public",
					"slotRef":       "users/1",
					"wrapAlgorithm": "RSA_OAEP_SHA256_V1",
					"wrappedKey":    "wrapped",
				},
			},
		},
	})
	createMessageReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+groupID+"/messages", bytes.NewReader(createMessageBody))
	createMessageReq.Header.Set("Authorization", "Bearer "+friendToken)
	createMessageReq.Header.Set("Content-Type", "application/json")
	createMessageResp, err := app.Test(createMessageReq, 5000)
	if err != nil {
		t.Fatalf("create direct message request failed: %v", err)
	}
	defer createMessageResp.Body.Close()
	if createMessageResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createMessageResp.Body)
		t.Fatalf("expected create direct message 200, got %d body=%s", createMessageResp.StatusCode, strings.TrimSpace(string(body)))
	}
	var directMessage apiGroupMessage
	if err := json.NewDecoder(createMessageResp.Body).Decode(&directMessage); err != nil {
		t.Fatalf("decode direct message failed: %v", err)
	}

	listGroups := func() listGroupsResponse {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/groups", nil)
		req.Header.Set("Authorization", "Bearer demo-token")
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatalf("list groups request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected list groups 200, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var payload listGroupsResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode list groups response failed: %v", err)
		}
		return payload
	}

	beforeRead := listGroups()
	if len(beforeRead.Groups) == 0 || !beforeRead.Groups[0].HasUnread {
		t.Fatalf("expected direct group unread before mark read")
	}
	if beforeRead.Groups[0].Name != directGroup.Name {
		t.Fatalf("expected direct group to move to top after new message, got %s", beforeRead.Groups[0].Name)
	}

	markReadBody, _ := json.Marshal(map[string]any{
		"lastReadMessage": directMessage.Name,
	})
	markReadReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+groupID+"/read", bytes.NewReader(markReadBody))
	markReadReq.Header.Set("Authorization", "Bearer demo-token")
	markReadReq.Header.Set("Content-Type", "application/json")
	markReadResp, err := app.Test(markReadReq, 5000)
	if err != nil {
		t.Fatalf("mark read request failed: %v", err)
	}
	markReadResp.Body.Close()
	if markReadResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected mark read 204, got %d", markReadResp.StatusCode)
	}

	afterRead := listGroups()
	if len(afterRead.Groups) == 0 || afterRead.Groups[0].HasUnread {
		t.Fatalf("expected direct group unread cleared after mark read")
	}
}

func signInTestUser(t *testing.T, app *fiber.App, username string, password string) string {
	t.Helper()
	signInBody, _ := json.Marshal(map[string]any{
		"passwordCredentials": map[string]any{
			"username": username,
			"password": password,
		},
	})
	signInReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/signin", bytes.NewReader(signInBody))
	signInReq.Header.Set("Content-Type", "application/json")
	signInResp, err := app.Test(signInReq, 5000)
	if err != nil {
		t.Fatalf("sign in request failed: %v", err)
	}
	defer signInResp.Body.Close()
	if signInResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(signInResp.Body)
		t.Fatalf("expected sign in 200, got %d body=%s", signInResp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload signInResponse
	if err := json.NewDecoder(signInResp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode sign in response failed: %v", err)
	}
	return payload.AccessToken
}
