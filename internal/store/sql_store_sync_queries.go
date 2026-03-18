package store

import (
	"context"
	"database/sql"
	"strings"

	"github.com/shinyes/keer/internal/models"
)

func (s *SQLStore) ListSyncEvents(
	ctx context.Context,
	afterID int64,
	domains []models.SyncDomain,
	limit int,
) ([]models.SyncEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(`
		SELECT
			id,
			domain,
			action,
			actor_user_id,
			target_user_id,
			group_id,
			memo_id,
			group_message_id,
			event_time
		FROM sync_events
		WHERE id > ?
	`)
	args := []any{afterID}
	domainFilter, domainArgs := buildSyncDomainFilter(domains)
	if domainFilter != "" {
		queryBuilder.WriteString(" AND ")
		queryBuilder.WriteString(domainFilter)
		args = append(args, domainArgs...)
	}
	queryBuilder.WriteString(" ORDER BY id ASC LIMIT ?")
	query := queryBuilder.String()
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.SyncEvent, 0, limit)
	for rows.Next() {
		var event models.SyncEvent
		var domain string
		var action string
		var eventTimeRaw string
		if err := rows.Scan(
			&event.ID,
			&domain,
			&action,
			&event.ActorUserID,
			&event.TargetUserID,
			&event.GroupID,
			&event.MemoID,
			&event.GroupMessageID,
			&eventTimeRaw,
		); err != nil {
			return nil, err
		}
		event.Domain = models.SyncDomain(strings.TrimSpace(domain))
		event.Action = models.SyncAction(strings.ToUpper(strings.TrimSpace(action)))
		if !event.Action.IsValid() {
			event.Action = models.SyncActionUpsert
		}
		eventTime, err := parseTime(eventTimeRaw)
		if err != nil {
			return nil, err
		}
		event.EventTime = eventTime
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *SQLStore) HasSyncEventsAfter(
	ctx context.Context,
	afterID int64,
	domains []models.SyncDomain,
) (bool, error) {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT 1 FROM sync_events WHERE id > ?")
	args := []any{afterID}
	domainFilter, domainArgs := buildSyncDomainFilter(domains)
	if domainFilter != "" {
		queryBuilder.WriteString(" AND ")
		queryBuilder.WriteString(domainFilter)
		args = append(args, domainArgs...)
	}
	queryBuilder.WriteString(" LIMIT 1")
	query := queryBuilder.String()

	var marker int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&marker)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, err
}

func buildSyncDomainFilter(domains []models.SyncDomain) (string, []any) {
	normalized := make([]string, 0, len(domains))
	seen := map[models.SyncDomain]struct{}{}
	for _, domain := range domains {
		if !domain.IsValid() {
			continue
		}
		if _, exists := seen[domain]; exists {
			continue
		}
		seen[domain] = struct{}{}
		normalized = append(normalized, string(domain))
	}
	if len(normalized) == 0 {
		return "", nil
	}
	placeholders := make([]string, len(normalized))
	args := make([]any, len(normalized))
	for i, domain := range normalized {
		placeholders[i] = "?"
		args[i] = domain
	}
	var filterBuilder strings.Builder
	filterBuilder.WriteString("domain IN (")
	filterBuilder.WriteString(strings.Join(placeholders, ","))
	filterBuilder.WriteString(")")
	return filterBuilder.String(), args
}
