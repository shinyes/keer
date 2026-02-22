package service

import (
	"context"
	"testing"

	"github.com/shinyes/keer/internal/models"
)

func TestCreateMemo_AllowsEmptyContentWithAttachments(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "memo-empty-content")

	attachment, err := services.store.CreateAttachment(
		ctx,
		user.ID,
		"image.png",
		"",
		"image/png",
		1024,
		"memo-compat-test-hash",
		"LOCAL",
		"attachments/test/image.png",
	)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	memo, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:         "",
		Visibility:      models.VisibilityPrivate,
		AttachmentNames: []string{"attachments/" + models.Int64ToString(attachment.ID)},
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}
	if memo.Memo.Content != "" {
		t.Fatalf("expected empty content, got %q", memo.Memo.Content)
	}
	if len(memo.Attachments) != 1 || memo.Attachments[0].ID != attachment.ID {
		t.Fatalf("expected attached resource id=%d, got %+v", attachment.ID, memo.Attachments)
	}
}

func TestUpdateMemo_AllowsEmptyContent(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "memo-update-empty-content")

	created, err := services.memoService.CreateMemo(ctx, user.ID, CreateMemoInput{
		Content:    "before",
		Visibility: models.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("CreateMemo() error = %v", err)
	}

	empty := ""
	updated, err := services.memoService.UpdateMemo(ctx, user.ID, created.Memo.ID, UpdateMemoInput{
		Content: &empty,
	})
	if err != nil {
		t.Fatalf("UpdateMemo() error = %v", err)
	}
	if updated.Memo.Content != "" {
		t.Fatalf("expected empty content after update, got %q", updated.Memo.Content)
	}
}

func TestParseResourceID_CompatibilityFormats(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "simple", input: "attachments/12", want: 12},
		{name: "plain id", input: "12", want: 12},
		{name: "with suffix", input: "attachments/12|local-uuid", want: 12},
		{name: "full path", input: "/api/v1/attachments/12", want: 12},
		{name: "with trailing slash", input: "attachments/12/", want: 12},
		{name: "invalid", input: "attachments/", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseResourceID(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: parseResourceID() error = %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: parseResourceID() got %d, want %d", tc.name, got, tc.want)
		}
	}
}
