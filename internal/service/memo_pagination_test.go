package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinyes/keer/internal/models"
)

func TestListMemos_PaginatesBeyondTenThousandRows(t *testing.T) {
	services := setupTestServices(t)
	ctx := context.Background()
	user := mustCreateUser(t, services.store, "memo-page")

	const totalMemos = 10050
	if err := bulkInsertVisibleMemos(t, services, user.ID, totalMemos); err != nil {
		t.Fatalf("bulkInsertVisibleMemos() error = %v", err)
	}

	seen := 0
	pageToken := ""
	for {
		page, nextToken, err := services.memoService.ListMemos(ctx, user.ID, nil, "", 200, pageToken)
		if err != nil {
			t.Fatalf("ListMemos(pageToken=%q) error = %v", pageToken, err)
		}
		seen += len(page)
		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}

	if seen != totalMemos {
		t.Fatalf("expected to paginate through %d memos, got %d", totalMemos, seen)
	}
}

func bulkInsertVisibleMemos(t *testing.T, services testServices, creatorID int64, count int) error {
	t.Helper()
	tx, err := services.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(
		context.Background(),
		`INSERT INTO memos (
			creator_id, content, payload_envelope, visibility, state, pinned, create_time, update_time, display_time,
			latitude, longitude, has_link, has_task_list, has_code, has_incomplete_tasks
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		if _, err := stmt.ExecContext(
			context.Background(),
			creatorID,
			fmt.Sprintf("payload-%d", i),
			"",
			models.VisibilityPrivate,
			models.MemoStateNormal,
			0,
			ts,
			ts,
			ts,
			nil,
			nil,
			0,
			0,
			0,
			0,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}
