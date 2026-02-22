package service

import (
	"testing"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

func TestCompileMemoFilter_SQLPrefilterBasic(t *testing.T) {
	filter, err := CompileMemoFilter(`creator_id == 7 && visibility in ["PRIVATE","PROTECTED"] && "book" in tags && property.hasLink == true`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()

	if pf.Unsatisfiable {
		t.Fatalf("expected satisfiable prefilter")
	}
	if len(pf.CreatorIDs) != 1 || pf.CreatorIDs[0] != 7 {
		t.Fatalf("unexpected creatorIDs: %+v", pf.CreatorIDs)
	}
	if len(pf.VisibilityIn) != 2 {
		t.Fatalf("unexpected visibilityIn length: %+v", pf.VisibilityIn)
	}
	if !containsVisibility(pf.VisibilityIn, models.VisibilityPrivate) || !containsVisibility(pf.VisibilityIn, models.VisibilityProtected) {
		t.Fatalf("unexpected visibilityIn values: %+v", pf.VisibilityIn)
	}
	if pf.HasLink == nil || !*pf.HasLink {
		t.Fatalf("expected HasLink=true in prefilter")
	}
	if len(pf.TagGroups) != 1 {
		t.Fatalf("expected one tag group, got %d", len(pf.TagGroups))
	}
	if len(pf.TagGroups[0].Options) != 1 || pf.TagGroups[0].Options[0].Kind != store.TagMatchExact || pf.TagGroups[0].Options[0].Value != "book" {
		t.Fatalf("unexpected tag group: %+v", pf.TagGroups[0])
	}
}

func TestCompileMemoFilter_SQLPrefilterUnsatisfiable(t *testing.T) {
	filter, err := CompileMemoFilter(`creator_id == 1 && creator_id == 2`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	if !filter.SQLPrefilter().Unsatisfiable {
		t.Fatalf("expected unsatisfiable prefilter")
	}
}

func TestCompileMemoFilter_SQLPrefilterORUnion(t *testing.T) {
	filter, err := CompileMemoFilter(`creator_id == 1 || creator_id == 2`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if pf.Unsatisfiable {
		t.Fatalf("expected satisfiable prefilter")
	}
	if len(pf.CreatorIDs) != 2 {
		t.Fatalf("expected creatorIDs union pushed down on OR expression, got %+v", pf.CreatorIDs)
	}
}

func TestCompileMemoFilter_SQLPrefilterORWithUnconstrainedBranch(t *testing.T) {
	filter, err := CompileMemoFilter(`creator_id == 1 || pinned == true || content.contains("x")`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if pf.Unsatisfiable {
		t.Fatalf("expected satisfiable prefilter")
	}
	if len(pf.CreatorIDs) != 0 {
		t.Fatalf("expected creatorIDs dropped when OR branch unconstrained, got %+v", pf.CreatorIDs)
	}
	if pf.Pinned != nil {
		t.Fatalf("expected pinned dropped when OR branch unconstrained, got %v", *pf.Pinned)
	}
}

func TestCompileMemoFilter_SQLPrefilterTagsExists(t *testing.T) {
	filter, err := CompileMemoFilter(`tags.exists(t, t.startsWith("book"))`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if len(pf.TagGroups) != 1 {
		t.Fatalf("expected one tag group, got %d", len(pf.TagGroups))
	}
	if len(pf.TagGroups[0].Options) != 1 {
		t.Fatalf("expected one tag option, got %+v", pf.TagGroups[0].Options)
	}
	opt := pf.TagGroups[0].Options[0]
	if opt.Kind != store.TagMatchPrefix || opt.Value != "book" {
		t.Fatalf("unexpected tag prefix option: %+v", opt)
	}
}

func TestCompileMemoFilter_SQLPrefilterLegacyTagIn(t *testing.T) {
	filter, err := CompileMemoFilter(`tag in ["book"] && visibility == "PRIVATE"`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if len(pf.TagGroups) != 1 {
		t.Fatalf("expected one tag group, got %d", len(pf.TagGroups))
	}
	if len(pf.VisibilityIn) != 1 || pf.VisibilityIn[0] != models.VisibilityPrivate {
		t.Fatalf("unexpected visibilityIn: %+v", pf.VisibilityIn)
	}
	if len(pf.TagGroups[0].Options) != 2 {
		t.Fatalf("expected two tag options (exact/prefix), got %+v", pf.TagGroups[0].Options)
	}
}

func TestCompileMemoFilter_SQLPrefilterNotEqual(t *testing.T) {
	filter, err := CompileMemoFilter(`pinned != true && visibility != "PUBLIC" && state != "ARCHIVED" && property.hasCode != false`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if pf.Pinned == nil || *pf.Pinned != false {
		t.Fatalf("expected pinned=false, got %+v", pf.Pinned)
	}
	if pf.HasCode == nil || *pf.HasCode != true {
		t.Fatalf("expected hasCode=true, got %+v", pf.HasCode)
	}
	if len(pf.VisibilityIn) != 2 || !containsVisibility(pf.VisibilityIn, models.VisibilityPrivate) || !containsVisibility(pf.VisibilityIn, models.VisibilityProtected) {
		t.Fatalf("unexpected visibilityIn for != PUBLIC: %+v", pf.VisibilityIn)
	}
	if len(pf.StateIn) != 1 || pf.StateIn[0] != models.MemoStateNormal {
		t.Fatalf("unexpected stateIn for != ARCHIVED: %+v", pf.StateIn)
	}
}

func TestCompileMemoFilter_SQLPrefilterNegatedTagMembership(t *testing.T) {
	filter, err := CompileMemoFilter(`!("work" in tags)`)
	if err != nil {
		t.Fatalf("CompileMemoFilter() error = %v", err)
	}
	pf := filter.SQLPrefilter()
	if len(pf.ExcludeTagGroups) != 1 {
		t.Fatalf("expected one exclude tag group, got %d", len(pf.ExcludeTagGroups))
	}
	opt := pf.ExcludeTagGroups[0].Options
	if len(opt) != 1 || opt[0].Kind != store.TagMatchExact || opt[0].Value != "work" {
		t.Fatalf("unexpected exclude tag options: %+v", opt)
	}
}

func containsVisibility(values []models.Visibility, target models.Visibility) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
