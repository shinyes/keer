package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGroupMembersRoute_InviteFriendOnly(t *testing.T) {
	app, userService := newTestAppWithUserService(t, true, true)

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

	createUser("friend-invite")
	createUser("nonfriend-invite")

	addFriendBody, _ := json.Marshal(map[string]any{"user": "friend-invite"})
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

	createGroupBody, _ := json.Marshal(map[string]any{
		"name":        "friend invite group",
		"description": "invite only",
	})
	createGroupReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(createGroupBody))
	createGroupReq.Header.Set("Authorization", "Bearer demo-token")
	createGroupReq.Header.Set("Content-Type", "application/json")
	createGroupResp, err := app.Test(createGroupReq, 5000)
	if err != nil {
		t.Fatalf("create group request failed: %v", err)
	}
	defer createGroupResp.Body.Close()
	if createGroupResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createGroupResp.Body)
		t.Fatalf("expected create group 200, got %d body=%s", createGroupResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created apiGroup
	if err := json.NewDecoder(createGroupResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create group response failed: %v", err)
	}
	groupID := created.Name
	if idx := strings.LastIndex(groupID, "/"); idx >= 0 {
		groupID = groupID[idx+1:]
	}

	friendInviteBody, _ := json.Marshal(map[string]any{"user": "friend-invite"})
	friendInviteReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+groupID+"/members", bytes.NewReader(friendInviteBody))
	friendInviteReq.Header.Set("Authorization", "Bearer demo-token")
	friendInviteReq.Header.Set("Content-Type", "application/json")
	friendInviteResp, err := app.Test(friendInviteReq, 5000)
	if err != nil {
		t.Fatalf("invite friend request failed: %v", err)
	}
	defer friendInviteResp.Body.Close()
	if friendInviteResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(friendInviteResp.Body)
		t.Fatalf("expected invite friend 200, got %d body=%s", friendInviteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	nonFriendInviteBody, _ := json.Marshal(map[string]any{"user": "nonfriend-invite"})
	nonFriendInviteReq := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+groupID+"/members", bytes.NewReader(nonFriendInviteBody))
	nonFriendInviteReq.Header.Set("Authorization", "Bearer demo-token")
	nonFriendInviteReq.Header.Set("Content-Type", "application/json")
	nonFriendInviteResp, err := app.Test(nonFriendInviteReq, 5000)
	if err != nil {
		t.Fatalf("invite non-friend request failed: %v", err)
	}
	defer nonFriendInviteResp.Body.Close()
	if nonFriendInviteResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(nonFriendInviteResp.Body)
		t.Fatalf("expected invite non-friend 400, got %d body=%s", nonFriendInviteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	demoUser, err := userService.GetUserByIdentifier(context.Background(), "demo")
	if err != nil {
		t.Fatalf("GetUserByIdentifier(demo) error = %v", err)
	}
	friends, err := userService.ListFriends(context.Background(), demoUser.ID)
	if err != nil {
		t.Fatalf("ListFriends() error = %v", err)
	}
	if len(friends) != 1 || friends[0].Username != "friend-invite" {
		t.Fatalf("expected friend-invite in friend list, got %+v", friends)
	}

	var invitedGroup apiGroup
	if err := json.NewDecoder(friendInviteResp.Body).Decode(&invitedGroup); err != nil {
		t.Fatalf("decode invited group response failed: %v", err)
	}
	memberUsernames := make([]string, 0, len(invitedGroup.Members))
	for _, member := range invitedGroup.Members {
		memberUsernames = append(memberUsernames, member.Username)
	}
	if !contains(memberUsernames, "friend-invite") {
		t.Fatalf("expected invited group members to contain friend-invite, got %v", memberUsernames)
	}
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
