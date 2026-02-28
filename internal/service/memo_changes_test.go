package service

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestListMemoChanges_IncludesCreateAndDeleteEvents(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()

	owner := mustCreateUser(t, services.store, "owner-sync")
	collaborator := mustCreateUser(t, services.store, "collab-sync")
	outsider := mustCreateUser(t, services.store, "outsider-sync")

	collaboratorTag := fmt.Sprintf("collab/%d", collaborator.ID)

	beforeCreate := time.Now().UTC().Add(-time.Second)
	created, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "sync change memo",
		Visibility: "PRIVATE",
		Tags:       []string{collaboratorTag},
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}

	createChanges, err := services.memoService.ListMemoChanges(
		ctx,
		owner.ID,
		nil,
		"",
		beforeCreate,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() for create error = %v", err)
	}
	if len(createChanges.Memos) != 1 {
		t.Fatalf("expected 1 changed memo after create, got %d", len(createChanges.Memos))
	}
	if createChanges.Memos[0].Memo.Name() != created.Memo.Name() {
		t.Fatalf("expected changed memo %q, got %q", created.Memo.Name(), createChanges.Memos[0].Memo.Name())
	}

	beforeDelete := time.Now().UTC().Add(-time.Second)
	if err := services.memoService.DeleteMemo(ctx, collaborator.ID, created.Memo.ID); err != nil {
		t.Fatalf("DeleteMemo() by collaborator error = %v", err)
	}

	ownerDeleteChanges, err := services.memoService.ListMemoChanges(
		ctx,
		owner.ID,
		nil,
		"",
		beforeDelete,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() owner delete window error = %v", err)
	}
	if !containsString(ownerDeleteChanges.DeletedMemoNames, created.Memo.Name()) {
		t.Fatalf("expected owner to receive deleted memo %q, got %v", created.Memo.Name(), ownerDeleteChanges.DeletedMemoNames)
	}

	collaboratorDeleteChanges, err := services.memoService.ListMemoChanges(
		ctx,
		collaborator.ID,
		nil,
		"",
		beforeDelete,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() collaborator delete window error = %v", err)
	}
	if !containsString(collaboratorDeleteChanges.DeletedMemoNames, created.Memo.Name()) {
		t.Fatalf("expected collaborator to receive deleted memo %q, got %v", created.Memo.Name(), collaboratorDeleteChanges.DeletedMemoNames)
	}

	outsiderDeleteChanges, err := services.memoService.ListMemoChanges(
		ctx,
		outsider.ID,
		nil,
		"",
		beforeDelete,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() outsider delete window error = %v", err)
	}
	if len(outsiderDeleteChanges.DeletedMemoNames) != 0 {
		t.Fatalf("expected outsider to receive no deleted memo names, got %v", outsiderDeleteChanges.DeletedMemoNames)
	}
}

func TestListMemoChanges_IncludesVisibilityRevocationEvents(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()

	owner := mustCreateUser(t, services.store, "owner-revoke")
	collaborator := mustCreateUser(t, services.store, "collab-revoke")

	collaboratorTag := fmt.Sprintf("collab/%d", collaborator.ID)
	created, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "memo before collaborator revoke",
		Visibility: "PRIVATE",
		Tags:       []string{collaboratorTag, "topic/test"},
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}

	beforeRevoke := time.Now().UTC().Add(-time.Second)
	newTags := []string{"topic/test"}
	if _, err := services.memoService.UpdateMemo(ctx, owner.ID, created.Memo.ID, UpdateMemoInput{
		Tags: &newTags,
	}); err != nil {
		t.Fatalf("UpdateMemo() remove collaborator tag error = %v", err)
	}

	collaboratorChanges, err := services.memoService.ListMemoChanges(
		ctx,
		collaborator.ID,
		nil,
		"",
		beforeRevoke,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() collaborator revoke window error = %v", err)
	}
	if !containsString(collaboratorChanges.DeletedMemoNames, created.Memo.Name()) {
		t.Fatalf("expected collaborator to receive revoke event for %q, got %v", created.Memo.Name(), collaboratorChanges.DeletedMemoNames)
	}

	ownerChanges, err := services.memoService.ListMemoChanges(
		ctx,
		owner.ID,
		nil,
		"",
		beforeRevoke,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ListMemoChanges() owner revoke window error = %v", err)
	}
	if containsString(ownerChanges.DeletedMemoNames, created.Memo.Name()) {
		t.Fatalf("expected owner not to receive revoke event for own memo %q", created.Memo.Name())
	}
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
