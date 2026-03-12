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

func TestFriendRoutes_AddListRemove(t *testing.T) {
	app, userService := newTestAppWithUserService(t, true, true)

	createBody, _ := json.Marshal(map[string]any{
		"user": map[string]any{
			"username": "friend01",
			"password": "register-password",
		},
	})
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create friend request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected create user 200, got %d body=%s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	addBody, _ := json.Marshal(map[string]any{"user": "friend01"})
	addReq := httptest.NewRequest(http.MethodPost, "/api/v1/friends", bytes.NewReader(addBody))
	addReq.Header.Set("Authorization", "Bearer demo-token")
	addReq.Header.Set("Content-Type", "application/json")
	addResp, err := app.Test(addReq, 5000)
	if err != nil {
		t.Fatalf("add friend request failed: %v", err)
	}
	defer addResp.Body.Close()
	if addResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(addResp.Body)
		t.Fatalf("expected add friend 200, got %d body=%s", addResp.StatusCode, strings.TrimSpace(string(body)))
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/friends", nil)
	listReq.Header.Set("Authorization", "Bearer demo-token")
	listResp, err := app.Test(listReq, 5000)
	if err != nil {
		t.Fatalf("list friends request failed: %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		t.Fatalf("expected list friends 200, got %d body=%s", listResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var listed listUsersResponse
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode friends failed: %v", err)
	}
	if len(listed.Users) != 1 || listed.Users[0].Username != "friend01" {
		t.Fatalf("expected friend01 in friends list, got %+v", listed.Users)
	}

	demoUser, err := userService.GetUserByIdentifier(context.Background(), "demo")
	if err != nil {
		t.Fatalf("GetUserByIdentifier(demo) error = %v", err)
	}
	friends, err := userService.ListFriends(context.Background(), demoUser.ID)
	if err != nil {
		t.Fatalf("ListFriends() error = %v", err)
	}
	if len(friends) != 1 || friends[0].Username != "friend01" {
		t.Fatalf("expected friend01 after add, got %+v", friends)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/friends/friend01", nil)
	deleteReq.Header.Set("Authorization", "Bearer demo-token")
	deleteResp, err := app.Test(deleteReq, 5000)
	if err != nil {
		t.Fatalf("remove friend request failed: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected remove friend 204, got %d body=%s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	friends, err = userService.ListFriends(context.Background(), demoUser.ID)
	if err != nil {
		t.Fatalf("ListFriends() after delete error = %v", err)
	}
	if len(friends) != 0 {
		t.Fatalf("expected empty friends after delete, got %+v", friends)
	}
}
