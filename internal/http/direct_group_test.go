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

func TestDirectGroupRoute_CreateOrGetFriendDirect(t *testing.T) {
	app, _ := newTestAppWithUserService(t, true, true)

	createUser := func(username string) {
		body, _ := json.Marshal(map[string]any{
			"user": map[string]any{
				"username": username,
				"password": "register-password",
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatalf("create user %s failed: %v", username, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			payload, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected create user 200, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
		}
	}

	createUser("friend-direct")
	createUser("outsider-direct")

	addFriendBody, _ := json.Marshal(map[string]any{"user": "friend-direct"})
	addFriendReq := httptest.NewRequest(http.MethodPost, "/api/v1/friends", bytes.NewReader(addFriendBody))
	addFriendReq.Header.Set("Authorization", "Bearer demo-token")
	addFriendReq.Header.Set("Content-Type", "application/json")
	addFriendResp, err := app.Test(addFriendReq, 5000)
	if err != nil {
		t.Fatalf("add friend request failed: %v", err)
	}
	defer addFriendResp.Body.Close()
	if addFriendResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(addFriendResp.Body)
		t.Fatalf("expected add friend 200, got %d body=%s", addFriendResp.StatusCode, strings.TrimSpace(string(body)))
	}

	createDirect := func(user string) (*apiGroup, int) {
		body, _ := json.Marshal(map[string]any{"user": user})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/directs", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer demo-token")
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatalf("create direct request failed: %v", err)
		}
		defer resp.Body.Close()
		status := resp.StatusCode
		if status != http.StatusOK {
			payload, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected create direct 200, got %d body=%s", status, strings.TrimSpace(string(payload)))
		}
		var group apiGroup
		if err := json.NewDecoder(resp.Body).Decode(&group); err != nil {
			t.Fatalf("decode direct group response failed: %v", err)
		}
		return &group, status
	}

	first, _ := createDirect("friend-direct")
	second, _ := createDirect("friend-direct")
	if first.Name != second.Name {
		t.Fatalf("expected direct group to be idempotent, got %s and %s", first.Name, second.Name)
	}
	if first.Type != "DIRECT" {
		t.Fatalf("expected direct group type DIRECT, got %s", first.Type)
	}
	if len(first.Members) != 2 {
		t.Fatalf("expected 2 direct group members, got %d", len(first.Members))
	}

	nonFriendBody, _ := json.Marshal(map[string]any{"user": "outsider-direct"})
	nonFriendReq := httptest.NewRequest(http.MethodPost, "/api/v1/directs", bytes.NewReader(nonFriendBody))
	nonFriendReq.Header.Set("Authorization", "Bearer demo-token")
	nonFriendReq.Header.Set("Content-Type", "application/json")
	nonFriendResp, err := app.Test(nonFriendReq, 5000)
	if err != nil {
		t.Fatalf("create non-friend direct request failed: %v", err)
	}
	defer nonFriendResp.Body.Close()
	if nonFriendResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(nonFriendResp.Body)
		t.Fatalf("expected create non-friend direct 400, got %d body=%s", nonFriendResp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func TestDirectGroupRoute_RejectThirdMember(t *testing.T) {
	app, _ := newTestAppWithUserService(t, true, true)

	for _, username := range []string{"friend-a", "friend-b"} {
		body, _ := json.Marshal(map[string]any{
			"user": map[string]any{
				"username": username,
				"password": "register-password",
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatalf("create user %s failed: %v", username, err)
		}
		resp.Body.Close()
	}

	for _, username := range []string{"friend-a", "friend-b"} {
		body, _ := json.Marshal(map[string]any{"user": username})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/friends", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer demo-token")
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatalf("add friend %s failed: %v", username, err)
		}
		resp.Body.Close()
	}

	directBody, _ := json.Marshal(map[string]any{"user": "friend-a"})
	directReq := httptest.NewRequest(http.MethodPost, "/api/v1/directs", bytes.NewReader(directBody))
	directReq.Header.Set("Authorization", "Bearer demo-token")
	directReq.Header.Set("Content-Type", "application/json")
	directResp, err := app.Test(directReq, 5000)
	if err != nil {
		t.Fatalf("create direct request failed: %v", err)
	}
	defer directResp.Body.Close()
	if directResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(directResp.Body)
		t.Fatalf("expected create direct 200, got %d body=%s", directResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var directGroup apiGroup
	if err := json.NewDecoder(directResp.Body).Decode(&directGroup); err != nil {
		t.Fatalf("decode direct group failed: %v", err)
	}
	groupID := directGroup.Name[strings.LastIndex(directGroup.Name, "/")+1:]

	inviteBody, _ := json.Marshal(map[string]any{"user": "friend-b"})
	inviteReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+groupID+"/members", bytes.NewReader(inviteBody))
	inviteReq.Header.Set("Authorization", "Bearer demo-token")
	inviteReq.Header.Set("Content-Type", "application/json")
	inviteResp, err := app.Test(inviteReq, 5000)
	if err != nil {
		t.Fatalf("invite third member request failed: %v", err)
	}
	defer inviteResp.Body.Close()
	if inviteResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(inviteResp.Body)
		t.Fatalf("expected invite third member 400, got %d body=%s", inviteResp.StatusCode, strings.TrimSpace(string(body)))
	}
}
