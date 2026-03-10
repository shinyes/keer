package service

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/shinyes/keer/internal/models"
)

func TestListMemos_ResolvesQuotedMemo(t *testing.T) {
	t.Parallel()

	services := setupTestServices(t)
	ctx := context.Background()
	owner := mustCreateUser(t, services.store, "memo-quote-owner")

	source, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "source memo",
		Visibility: models.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("CreateMemo(source) error = %v", err)
	}
	quoted, err := services.memoService.CreateMemo(ctx, owner.ID, CreateMemoInput{
		Content:    "quoted memo",
		Visibility: models.VisibilityPrivate,
		Tags:       []string{buildQuoteTagForTest(memoQuoteSourceKindRemote, source.Memo.Name())},
	})
	if err != nil {
		t.Fatalf("CreateMemo(quoted) error = %v", err)
	}

	list, _, err := services.memoService.ListMemos(
		ctx,
		owner.ID,
		nil,
		fmt.Sprintf("creator_id == %d", owner.ID),
		50,
		"",
	)
	if err != nil {
		t.Fatalf("ListMemos() error = %v", err)
	}

	item := findMemoByID(list, quoted.Memo.ID)
	if item == nil {
		t.Fatalf("expected quoted memo id=%d in list result", quoted.Memo.ID)
	}
	if item.Quote == nil {
		t.Fatalf("expected quoted memo to include quote metadata")
	}
	if item.Quote.SourceKind != memoQuoteSourceKindRemote {
		t.Fatalf("expected source kind %q, got %q", memoQuoteSourceKindRemote, item.Quote.SourceKind)
	}
	if item.Quote.Source != source.Memo.Name() {
		t.Fatalf("expected source %q, got %q", source.Memo.Name(), item.Quote.Source)
	}
	if item.Quote.Memo == nil {
		t.Fatalf("expected referenced memo to be resolved")
	}
	if item.Quote.Memo.Memo.ID != source.Memo.ID {
		t.Fatalf("expected referenced memo id=%d, got %d", source.Memo.ID, item.Quote.Memo.Memo.ID)
	}
}

func TestListMemos_QuotedMemoUnavailableWhenSourceInvisible(t *testing.T) {
	t.Parallel()

	services := setupTestServices(t)
	ctx := context.Background()
	sourceOwner := mustCreateUser(t, services.store, "memo-quote-source-owner")
	quoteOwner := mustCreateUser(t, services.store, "memo-quote-quote-owner")

	source, err := services.memoService.CreateMemo(ctx, sourceOwner.ID, CreateMemoInput{
		Content:    "private source",
		Visibility: models.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("CreateMemo(source) error = %v", err)
	}
	quoted, err := services.memoService.CreateMemo(ctx, quoteOwner.ID, CreateMemoInput{
		Content:    "quote private source",
		Visibility: models.VisibilityPrivate,
		Tags:       []string{buildQuoteTagForTest(memoQuoteSourceKindRemote, source.Memo.Name())},
	})
	if err != nil {
		t.Fatalf("CreateMemo(quoted) error = %v", err)
	}

	list, _, err := services.memoService.ListMemos(
		ctx,
		quoteOwner.ID,
		nil,
		fmt.Sprintf("creator_id == %d", quoteOwner.ID),
		50,
		"",
	)
	if err != nil {
		t.Fatalf("ListMemos() error = %v", err)
	}

	item := findMemoByID(list, quoted.Memo.ID)
	if item == nil {
		t.Fatalf("expected quoted memo id=%d in list result", quoted.Memo.ID)
	}
	if item.Quote == nil {
		t.Fatalf("expected quote metadata")
	}
	if item.Quote.Memo != nil {
		t.Fatalf("expected quoted source memo to be unavailable for viewer")
	}
}

func TestParseMemoIDFromQuoteSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		wantID int64
		wantOK bool
	}{
		{name: "id only", source: "42", wantID: 42, wantOK: true},
		{name: "memo name", source: "memos/42", wantID: 42, wantOK: true},
		{name: "api path", source: "/api/v1/memos/42", wantID: 42, wantOK: true},
		{name: "with suffix", source: "memos/42|local-1", wantID: 42, wantOK: true},
		{name: "other resource", source: "groups/1/messages/42", wantOK: false},
		{name: "uuid", source: "local:abc-def", wantOK: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotOK := parseMemoIDFromQuoteSource(tt.source)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotID != tt.wantID {
				t.Fatalf("id = %d, want %d", gotID, tt.wantID)
			}
		})
	}
}

func buildQuoteTagForTest(sourceKind MemoQuoteSourceKind, source string) string {
	return "quote/src/" + string(sourceKind) + "/" + hex.EncodeToString([]byte(source))
}

func findMemoByID(memos []MemoWithAttachments, memoID int64) *MemoWithAttachments {
	for i := range memos {
		if memos[i].Memo.ID == memoID {
			return &memos[i]
		}
	}
	return nil
}
