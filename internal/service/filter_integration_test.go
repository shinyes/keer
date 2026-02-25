package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func TestListMemos_TagInHierarchical(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "u1")

	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book",
		Tags:       []string{"book"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #book error = %v", err)
	}
	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book/fiction",
		Tags:       []string{"book/fiction"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #book/fiction error = %v", err)
	}
	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#work",
		Tags:       []string{"work"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #work error = %v", err)
	}

	list, _, err := services.memoService.ListMemos(ctx, user.ID, nil, `tag in ["book"]`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos tag in [book] error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 memos for tag in [book], got %d", len(list))
	}

	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, `tag in ["book","work"]`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos tag in [book,work] error = %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 memos for tag in [book,work], got %d", len(list))
	}
}

func TestListMemos_CELTagsExistsAndMembership(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "u2")

	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book [link](https://example.com)",
		Tags:       []string{"book"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #book error = %v", err)
	}
	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book/fiction",
		Tags:       []string{"book/fiction"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #book/fiction error = %v", err)
	}
	if _, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#work",
		Tags:       []string{"work"},
		Visibility: models.VisibilityPrivate,
	}); err != nil {
		t.Fatalf("CreateMemo #work error = %v", err)
	}

	list, _, err := services.memoService.ListMemos(ctx, user.ID, nil, `tags.exists(t, t.startsWith("book"))`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos tags.exists() error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 memos for tags.exists startsWith(book), got %d", len(list))
	}

	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, `"work" in tags`, 200, "")
	if err != nil {
		t.Fatalf(`ListMemos "work" in tags error = %v`, err)
	}
	if len(list) != 1 {
		t.Fatalf(`expected 1 memo for "work" in tags, got %d`, len(list))
	}

	filter := fmt.Sprintf(`creator_id == %d && visibility in ["PRIVATE"] && tags.exists(t, t.startsWith("book")) && !("work" in tags)`, user.ID)
	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, filter, 200, "")
	if err != nil {
		t.Fatalf("ListMemos full CEL expression error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 memos for full CEL expression, got %d", len(list))
	}

	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, `property.hasLink == true`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos property.hasLink error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 memo for property.hasLink, got %d", len(list))
	}
}

func TestListMemos_CELNotAndNotEqual(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "u3")

	m1, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#work",
		Tags:       []string{"work"},
		Visibility: models.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("CreateMemo m1 error = %v", err)
	}
	m2, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book",
		Tags:       []string{"book"},
		Visibility: models.VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("CreateMemo m2 error = %v", err)
	}
	m3, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "#book/fiction",
		Tags:       []string{"book/fiction"},
		Visibility: models.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("CreateMemo m3 error = %v", err)
	}

	// Mark one memo pinned=true to verify pinned != true.
	pinnedTrue := true
	if _, err := services.store.UpdateMemo(ctx, m2.Memo.ID, store.MemoUpdate{Pinned: &pinnedTrue}); err != nil {
		t.Fatalf("UpdateMemo pinned=true error = %v", err)
	}

	list, _, err := services.memoService.ListMemos(ctx, user.ID, nil, `!("work" in tags)`, 200, "")
	if err != nil {
		t.Fatalf(`ListMemos !("work" in tags) error = %v`, err)
	}
	if len(list) != 2 {
		t.Fatalf(`expected 2 memos for !("work" in tags), got %d`, len(list))
	}

	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, `pinned != true`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos pinned != true error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 memos for pinned != true, got %d", len(list))
	}

	list, _, err = services.memoService.ListMemos(ctx, user.ID, nil, `visibility != "PUBLIC"`, 200, "")
	if err != nil {
		t.Fatalf("ListMemos visibility != PUBLIC error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 memos for visibility != PUBLIC, got %d", len(list))
	}

	_ = m1
	_ = m3
}
