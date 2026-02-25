package service

import (
	"context"
	"testing"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func TestGetUserTagCount(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()

	owner := mustCreateUser(t, services.store, "owner")
	viewer := mustCreateUser(t, services.store, "viewer")

	if _, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "#alpha #alpha #beta",
		Tags:       []string{"alpha", "alpha", "beta"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo private error = %v", err)
	}
	publicMemo, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "#beta #book/fiction",
		Tags:       []string{"beta", "book/fiction"},
		Visibility: models.VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("CreateMemo public error = %v", err)
	}
	if _, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "#alpha",
		Tags:       []string{"alpha"},
		Visibility: models.VisibilityProtected,
	}); err != nil {
		t.Fatalf("CreateMemo protected error = %v", err)
	}
	archivedMemo, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "#archived",
		Tags:       []string{"archived"},
		Visibility: models.VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("CreateMemo archived error = %v", err)
	}
	archivedState := models.MemoStateArchived
	if _, err := services.store.UpdateMemo(ctx, archivedMemo.Memo.ID, store.MemoUpdate{State: &archivedState}); err != nil {
		t.Fatalf("UpdateMemo archived state error = %v", err)
	}

	ownerCounts, err := services.memoService.GetUserTagCount(ctx, owner.ID, owner.ID)
	if err != nil {
		t.Fatalf("GetUserTagCount owner error = %v", err)
	}
	assertTagCount(t, ownerCounts, "alpha", 2)
	assertTagCount(t, ownerCounts, "beta", 2)
	assertTagCount(t, ownerCounts, "book/fiction", 1)
	assertTagCount(t, ownerCounts, "archived", 0)

	viewerCounts, err := services.memoService.GetUserTagCount(ctx, owner.ID, viewer.ID)
	if err != nil {
		t.Fatalf("GetUserTagCount viewer error = %v", err)
	}
	assertTagCount(t, viewerCounts, "alpha", 1)
	assertTagCount(t, viewerCounts, "beta", 1)
	assertTagCount(t, viewerCounts, "book/fiction", 1)
	assertTagCount(t, viewerCounts, "archived", 0)

	// Ensure memo payload tags are used and duplicated tags in same memo count once.
	if ownerCounts["alpha"] == viewerCounts["alpha"] {
		t.Fatalf("expected visibility-aware tag counts to differ for alpha")
	}

	_ = publicMemo
}

func assertTagCount(t *testing.T, actual map[string]int, tag string, want int) {
	t.Helper()
	got := actual[tag]
	if got != want {
		t.Fatalf("tag %q count mismatch got=%d want=%d", tag, got, want)
	}
}
