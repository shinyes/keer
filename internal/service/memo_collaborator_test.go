package service

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/shinyes/keer/internal/models"
)

func TestCollaboratorCanManageMemo(t *testing.T) {
	t.Parallel()

	services := setupTestServices(t)
	ctx := context.Background()
	owner := mustCreateUser(t, services.store, "memo-collab-owner")
	collaborator := mustCreateUser(t, services.store, "memo-collab-editor")

	collaboratorTag := fmt.Sprintf("collab/%d", collaborator.ID)
	created, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "owner memo",
		Visibility: models.VisibilityPrivate,
		Tags:       []string{collaboratorTag},
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}

	updatedContent := "edited by collaborator"
	updated, err := services.memoService.UpdateMemo(ctx, collaborator.ID, created.Memo.ID, UpdateMemoInput{
		Content: &updatedContent,
	})
	if err != nil {
		t.Fatalf("UpdateMemo() as collaborator error = %v", err)
	}
	if updated.Memo.Content != updatedContent {
		t.Fatalf("expected content %q, got %q", updatedContent, updated.Memo.Content)
	}

	if err := services.memoService.DeleteMemo(ctx, collaborator.ID, created.Memo.ID); err != nil {
		t.Fatalf("DeleteMemo() as collaborator error = %v", err)
	}
	if _, err := services.store.GetMemoByID(ctx, created.Memo.ID); err == nil {
		t.Fatalf("expected memo deleted")
	} else if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
	}
}

func TestCollaboratorCanSeePrivateMemo(t *testing.T) {
	t.Parallel()

	services := setupTestServices(t)
	ctx := context.Background()
	owner := mustCreateUser(t, services.store, "memo-collab-visible-owner")
	collaborator := mustCreateUser(t, services.store, "memo-collab-visible-member")
	outsider := mustCreateUser(t, services.store, "memo-collab-visible-outsider")

	collaboratorTag := fmt.Sprintf("collab/%d", collaborator.ID)
	created, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "private collab memo",
		Visibility: models.VisibilityPrivate,
		Tags:       []string{collaboratorTag},
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}

	filter := fmt.Sprintf("creator_id == %d", owner.ID)
	collaboratorView, _, err := services.memoService.ListMemos(ctx, collaborator.ID, nil, filter, 50, "")
	if err != nil {
		t.Fatalf("ListMemos() collaborator error = %v", err)
	}
	if len(collaboratorView) != 1 || collaboratorView[0].Memo.ID != created.Memo.ID {
		t.Fatalf("expected collaborator to see memo id=%d, got %+v", created.Memo.ID, collaboratorView)
	}

	outsiderView, _, err := services.memoService.ListMemos(ctx, outsider.ID, nil, filter, 50, "")
	if err != nil {
		t.Fatalf("ListMemos() outsider error = %v", err)
	}
	if len(outsiderView) != 0 {
		t.Fatalf("expected outsider cannot see private collab memo, got %d", len(outsiderView))
	}
}
