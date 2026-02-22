package service

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types/ref"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/store"
)

type CELMemoFilter struct {
	program      cel.Program
	sqlPrefilter store.MemoSQLPrefilter
}

var legacyTagInExpr = regexp.MustCompile(`(?i)\btag\s+in\s+\[((?:\s*"[^"\\]*(?:\\.[^"\\]*)*"\s*,?)*)\]`)

var (
	allVisibilityValues = []models.Visibility{
		models.VisibilityPrivate,
		models.VisibilityProtected,
		models.VisibilityPublic,
	}
	allStateValues = []models.MemoState{
		models.MemoStateNormal,
		models.MemoStateArchived,
	}
)

func CompileMemoFilter(raw string) (*CELMemoFilter, error) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return nil, nil
	}

	rewritten, err := rewriteLegacyTagIn(normalized)
	if err != nil {
		return nil, err
	}
	rewritten = rewritePropertySelectors(rewritten)

	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar("creator_id", decls.Int),
			decls.NewVar("visibility", decls.String),
			decls.NewVar("state", decls.String),
			decls.NewVar("pinned", decls.Bool),
			decls.NewVar("content", decls.String),
			decls.NewVar("tags", decls.NewListType(decls.String)),
			decls.NewVar("property", decls.NewMapType(decls.String, decls.Bool)),
			decls.NewVar("has_link", decls.Bool),
			decls.NewVar("has_task_list", decls.Bool),
			decls.NewVar("has_code", decls.Bool),
			decls.NewVar("has_incomplete_tasks", decls.Bool),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build CEL env: %w", err)
	}

	ast, issues := env.Compile(rewritten)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("invalid CEL filter: %w", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("build CEL program: %w", err)
	}

	return &CELMemoFilter{
		program:      program,
		sqlPrefilter: buildSQLPrefilter(ast.Expr()),
	}, nil
}

func (f *CELMemoFilter) Matches(memo models.Memo) (bool, error) {
	if f == nil {
		return true, nil
	}
	property := map[string]bool{
		"hasLink":            memo.Payload.Property.HasLink,
		"hasTaskList":        memo.Payload.Property.HasTaskList,
		"hasCode":            memo.Payload.Property.HasCode,
		"hasIncompleteTasks": memo.Payload.Property.HasIncompleteTasks,
	}

	out, _, err := f.program.Eval(map[string]any{
		"creator_id":           memo.CreatorID,
		"visibility":           string(memo.Visibility),
		"state":                string(memo.State),
		"pinned":               memo.Pinned,
		"content":              memo.Content,
		"tags":                 memo.Payload.Tags,
		"property":             property,
		"has_link":             memo.Payload.Property.HasLink,
		"has_task_list":        memo.Payload.Property.HasTaskList,
		"has_code":             memo.Payload.Property.HasCode,
		"has_incomplete_tasks": memo.Payload.Property.HasIncompleteTasks,
	})
	if err != nil {
		return false, fmt.Errorf("evaluate CEL filter: %w", err)
	}
	b, err := asBool(out)
	if err != nil {
		return false, err
	}
	return b, nil
}

func (f *CELMemoFilter) SQLPrefilter() store.MemoSQLPrefilter {
	if f == nil {
		return store.EmptyMemoPrefilter()
	}
	return f.sqlPrefilter
}

func asBool(v ref.Val) (bool, error) {
	switch val := v.Value().(type) {
	case bool:
		return val, nil
	default:
		return false, fmt.Errorf("filter expression must return bool, got %T", val)
	}
}

func rewriteLegacyTagIn(input string) (string, error) {
	matches := legacyTagInExpr.FindAllStringSubmatchIndex(input, -1)
	if len(matches) == 0 {
		return input, nil
	}

	var sb strings.Builder
	last := 0
	for _, m := range matches {
		start, end := m[0], m[1]
		listStart, listEnd := m[2], m[3]
		sb.WriteString(input[last:start])
		rawList := input[listStart:listEnd]
		tags, err := parseCELQuotedStringList(rawList)
		if err != nil {
			return "", err
		}
		if len(tags) == 0 {
			sb.WriteString("false")
		} else {
			conds := make([]string, 0, len(tags))
			for _, tag := range tags {
				escaped := celQuote(tag)
				conds = append(conds, fmt.Sprintf(`tags.exists(t, t == "%s" || t.startsWith("%s/"))`, escaped, escaped))
			}
			sb.WriteString("(" + strings.Join(conds, " || ") + ")")
		}
		last = end
	}
	sb.WriteString(input[last:])
	return sb.String(), nil
}

func parseCELQuotedStringList(raw string) ([]string, error) {
	data := "[" + raw + "]"
	list := []string{}
	if err := json.Unmarshal([]byte(data), &list); err != nil {
		return nil, fmt.Errorf("invalid tag in-list: %w", err)
	}
	return list, nil
}

func celQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

func rewritePropertySelectors(input string) string {
	replacer := strings.NewReplacer(
		"property.hasLink", "has_link",
		"property.hasTaskList", "has_task_list",
		"property.hasCode", "has_code",
		"property.hasIncompleteTasks", "has_incomplete_tasks",
	)
	return replacer.Replace(input)
}

func buildSQLPrefilter(expr *exprpb.Expr) store.MemoSQLPrefilter {
	return normalizePrefilter(derivePrefilter(expr))
}

func derivePrefilter(expr *exprpb.Expr) store.MemoSQLPrefilter {
	if expr == nil {
		return store.EmptyMemoPrefilter()
	}
	if c := expr.GetConstExpr(); c != nil {
		if v, ok := constBool(c); ok && !v {
			return store.MemoSQLPrefilter{Unsatisfiable: true}
		}
		return store.EmptyMemoPrefilter()
	}

	if call := expr.GetCallExpr(); call != nil {
		switch call.Function {
		case "_&&_":
			if len(call.Args) != 2 {
				return store.EmptyMemoPrefilter()
			}
			return mergePrefilterAnd(derivePrefilter(call.Args[0]), derivePrefilter(call.Args[1]))
		case "_||_":
			if len(call.Args) != 2 {
				return store.EmptyMemoPrefilter()
			}
			return mergePrefilterOr(derivePrefilter(call.Args[0]), derivePrefilter(call.Args[1]))
		case "_==_":
			return deriveAtomicEq(call)
		case "_!=_":
			return deriveAtomicNeq(call)
		case "@in":
			return deriveAtomicIn(call)
		case "!_":
			if len(call.Args) != 1 {
				return store.EmptyMemoPrefilter()
			}
			return deriveNegatedPrefilter(call.Args[0])
		default:
			return store.EmptyMemoPrefilter()
		}
	}

	if comp := expr.GetComprehensionExpr(); comp != nil {
		if group, ok := extractTagExistsGroup(comp); ok && len(group.Options) > 0 {
			return store.MemoSQLPrefilter{
				TagGroups: []store.TagMatchGroup{group},
			}
		}
	}

	return store.EmptyMemoPrefilter()
}

func deriveAtomicEq(call *exprpb.Expr_Call) store.MemoSQLPrefilter {
	if len(call.Args) != 2 {
		return store.EmptyMemoPrefilter()
	}
	name, c, ok := identAndConst(call.Args[0], call.Args[1])
	if !ok {
		name, c, ok = identAndConst(call.Args[1], call.Args[0])
	}
	if !ok {
		return store.EmptyMemoPrefilter()
	}

	pf := store.EmptyMemoPrefilter()
	switch name {
	case "creator_id":
		id, ok := constInt64(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.CreatorIDs = []int64{id}
	case "visibility":
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		v := models.Visibility(s)
		if !v.IsValid() {
			pf.Unsatisfiable = true
			return pf
		}
		pf.VisibilityIn = []models.Visibility{v}
	case "state":
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		st := models.MemoState(s)
		if !st.IsValid() {
			pf.Unsatisfiable = true
			return pf
		}
		pf.StateIn = []models.MemoState{st}
	case "pinned":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.Pinned = ptrBool(v)
	case "has_link":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasLink = ptrBool(v)
	case "has_task_list":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasTaskList = ptrBool(v)
	case "has_code":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasCode = ptrBool(v)
	case "has_incomplete_tasks":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasIncompleteTasks = ptrBool(v)
	}
	return pf
}

func deriveAtomicIn(call *exprpb.Expr_Call) store.MemoSQLPrefilter {
	if len(call.Args) != 2 {
		return store.EmptyMemoPrefilter()
	}
	lhs := call.Args[0]
	rhs := call.Args[1]

	if id := lhs.GetIdentExpr(); id != nil {
		list := rhs.GetListExpr()
		if list == nil {
			return store.EmptyMemoPrefilter()
		}
		pf := store.EmptyMemoPrefilter()
		switch id.Name {
		case "creator_id":
			ids := make([]int64, 0, len(list.Elements))
			for _, e := range list.Elements {
				v, ok := constInt64(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				ids = append(ids, v)
			}
			if len(ids) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			pf.CreatorIDs = ids
			return pf
		case "visibility":
			values := make([]models.Visibility, 0, len(list.Elements))
			for _, e := range list.Elements {
				s, ok := constString(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				v := models.Visibility(s)
				if !v.IsValid() {
					pf.Unsatisfiable = true
					return pf
				}
				values = append(values, v)
			}
			if len(values) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			pf.VisibilityIn = values
			return pf
		case "state":
			values := make([]models.MemoState, 0, len(list.Elements))
			for _, e := range list.Elements {
				s, ok := constString(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				st := models.MemoState(s)
				if !st.IsValid() {
					pf.Unsatisfiable = true
					return pf
				}
				values = append(values, st)
			}
			if len(values) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			pf.StateIn = values
			return pf
		case "pinned":
			values := make([]bool, 0, len(list.Elements))
			for _, e := range list.Elements {
				v, ok := constBool(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				values = append(values, v)
			}
			if len(values) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			hasTrue := slices.Contains(values, true)
			hasFalse := slices.Contains(values, false)
			if hasTrue && hasFalse {
				return store.EmptyMemoPrefilter()
			}
			if hasTrue {
				pf.Pinned = ptrBool(true)
			} else {
				pf.Pinned = ptrBool(false)
			}
			return pf
		}
	}

	// "x" in tags
	if id := rhs.GetIdentExpr(); id != nil && id.Name == "tags" {
		c := lhs.GetConstExpr()
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		return store.MemoSQLPrefilter{
			TagGroups: []store.TagMatchGroup{{
				Options: []store.TagMatchOption{{
					Kind:  store.TagMatchExact,
					Value: s,
				}},
			}},
		}
	}

	return store.EmptyMemoPrefilter()
}

func deriveAtomicNeq(call *exprpb.Expr_Call) store.MemoSQLPrefilter {
	if len(call.Args) != 2 {
		return store.EmptyMemoPrefilter()
	}
	name, c, ok := identAndConst(call.Args[0], call.Args[1])
	if !ok {
		name, c, ok = identAndConst(call.Args[1], call.Args[0])
	}
	if !ok {
		return store.EmptyMemoPrefilter()
	}

	pf := store.EmptyMemoPrefilter()
	switch name {
	case "visibility":
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		v := models.Visibility(s)
		if !v.IsValid() {
			return store.EmptyMemoPrefilter()
		}
		values := make([]models.Visibility, 0, len(allVisibilityValues))
		for _, candidate := range allVisibilityValues {
			if candidate != v {
				values = append(values, candidate)
			}
		}
		if len(values) == 0 {
			pf.Unsatisfiable = true
			return pf
		}
		pf.VisibilityIn = values
		return pf
	case "state":
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		st := models.MemoState(s)
		if !st.IsValid() {
			return store.EmptyMemoPrefilter()
		}
		values := make([]models.MemoState, 0, len(allStateValues))
		for _, candidate := range allStateValues {
			if candidate != st {
				values = append(values, candidate)
			}
		}
		if len(values) == 0 {
			pf.Unsatisfiable = true
			return pf
		}
		pf.StateIn = values
		return pf
	case "pinned":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.Pinned = ptrBool(!v)
		return pf
	case "has_link":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasLink = ptrBool(!v)
		return pf
	case "has_task_list":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasTaskList = ptrBool(!v)
		return pf
	case "has_code":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasCode = ptrBool(!v)
		return pf
	case "has_incomplete_tasks":
		v, ok := constBool(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		pf.HasIncompleteTasks = ptrBool(!v)
		return pf
	default:
		return store.EmptyMemoPrefilter()
	}
}

func deriveNegatedPrefilter(expr *exprpb.Expr) store.MemoSQLPrefilter {
	if expr == nil {
		return store.EmptyMemoPrefilter()
	}

	if c := expr.GetConstExpr(); c != nil {
		if v, ok := constBool(c); ok {
			if v {
				return store.MemoSQLPrefilter{Unsatisfiable: true}
			}
			return store.EmptyMemoPrefilter()
		}
		return store.EmptyMemoPrefilter()
	}

	if call := expr.GetCallExpr(); call != nil {
		switch call.Function {
		case "_==_":
			return deriveAtomicNeq(call)
		case "_!=_":
			return deriveAtomicEq(call)
		case "@in":
			return deriveAtomicNotIn(call)
		case "!_":
			if len(call.Args) != 1 {
				return store.EmptyMemoPrefilter()
			}
			return derivePrefilter(call.Args[0])
		case "_&&_":
			if len(call.Args) != 2 {
				return store.EmptyMemoPrefilter()
			}
			return mergePrefilterOr(deriveNegatedPrefilter(call.Args[0]), deriveNegatedPrefilter(call.Args[1]))
		case "_||_":
			if len(call.Args) != 2 {
				return store.EmptyMemoPrefilter()
			}
			return mergePrefilterAnd(deriveNegatedPrefilter(call.Args[0]), deriveNegatedPrefilter(call.Args[1]))
		default:
			return store.EmptyMemoPrefilter()
		}
	}

	if comp := expr.GetComprehensionExpr(); comp != nil {
		if group, ok := extractTagExistsGroup(comp); ok && len(group.Options) > 0 {
			return store.MemoSQLPrefilter{
				ExcludeTagGroups: []store.TagMatchGroup{group},
			}
		}
	}

	return store.EmptyMemoPrefilter()
}

func deriveAtomicNotIn(call *exprpb.Expr_Call) store.MemoSQLPrefilter {
	if len(call.Args) != 2 {
		return store.EmptyMemoPrefilter()
	}
	lhs := call.Args[0]
	rhs := call.Args[1]

	if id := lhs.GetIdentExpr(); id != nil {
		list := rhs.GetListExpr()
		if list == nil {
			return store.EmptyMemoPrefilter()
		}
		pf := store.EmptyMemoPrefilter()
		switch id.Name {
		case "visibility":
			excluded := map[models.Visibility]struct{}{}
			for _, e := range list.Elements {
				s, ok := constString(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				v := models.Visibility(s)
				if !v.IsValid() {
					continue
				}
				excluded[v] = struct{}{}
			}
			values := make([]models.Visibility, 0, len(allVisibilityValues))
			for _, candidate := range allVisibilityValues {
				if _, ok := excluded[candidate]; ok {
					continue
				}
				values = append(values, candidate)
			}
			if len(values) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			if len(values) == len(allVisibilityValues) {
				return store.EmptyMemoPrefilter()
			}
			pf.VisibilityIn = values
			return pf
		case "state":
			excluded := map[models.MemoState]struct{}{}
			for _, e := range list.Elements {
				s, ok := constString(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				st := models.MemoState(s)
				if !st.IsValid() {
					continue
				}
				excluded[st] = struct{}{}
			}
			values := make([]models.MemoState, 0, len(allStateValues))
			for _, candidate := range allStateValues {
				if _, ok := excluded[candidate]; ok {
					continue
				}
				values = append(values, candidate)
			}
			if len(values) == 0 {
				pf.Unsatisfiable = true
				return pf
			}
			if len(values) == len(allStateValues) {
				return store.EmptyMemoPrefilter()
			}
			pf.StateIn = values
			return pf
		case "pinned":
			excluded := map[bool]struct{}{}
			for _, e := range list.Elements {
				v, ok := constBool(e.GetConstExpr())
				if !ok {
					return store.EmptyMemoPrefilter()
				}
				excluded[v] = struct{}{}
			}
			_, hasTrue := excluded[true]
			_, hasFalse := excluded[false]
			switch {
			case hasTrue && hasFalse:
				pf.Unsatisfiable = true
				return pf
			case hasTrue:
				pf.Pinned = ptrBool(false)
				return pf
			case hasFalse:
				pf.Pinned = ptrBool(true)
				return pf
			default:
				return store.EmptyMemoPrefilter()
			}
		default:
			return store.EmptyMemoPrefilter()
		}
	}

	// !("x" in tags)
	if id := rhs.GetIdentExpr(); id != nil && id.Name == "tags" {
		c := lhs.GetConstExpr()
		s, ok := constString(c)
		if !ok {
			return store.EmptyMemoPrefilter()
		}
		return store.MemoSQLPrefilter{
			ExcludeTagGroups: []store.TagMatchGroup{{
				Options: []store.TagMatchOption{{
					Kind:  store.TagMatchExact,
					Value: s,
				}},
			}},
		}
	}

	return store.EmptyMemoPrefilter()
}

func mergePrefilterAnd(a store.MemoSQLPrefilter, b store.MemoSQLPrefilter) store.MemoSQLPrefilter {
	if a.Unsatisfiable || b.Unsatisfiable {
		return store.MemoSQLPrefilter{Unsatisfiable: true}
	}

	out := store.EmptyMemoPrefilter()

	out.CreatorIDs, out.Unsatisfiable = mergeSetAnd(a.CreatorIDs, b.CreatorIDs)
	if out.Unsatisfiable {
		return out
	}
	out.VisibilityIn, out.Unsatisfiable = mergeVisibilityAnd(a.VisibilityIn, b.VisibilityIn)
	if out.Unsatisfiable {
		return out
	}
	out.StateIn, out.Unsatisfiable = mergeStateAnd(a.StateIn, b.StateIn)
	if out.Unsatisfiable {
		return out
	}
	out.Pinned, out.Unsatisfiable = mergeBoolPtrAnd(a.Pinned, b.Pinned)
	if out.Unsatisfiable {
		return out
	}
	out.HasLink, out.Unsatisfiable = mergeBoolPtrAnd(a.HasLink, b.HasLink)
	if out.Unsatisfiable {
		return out
	}
	out.HasTaskList, out.Unsatisfiable = mergeBoolPtrAnd(a.HasTaskList, b.HasTaskList)
	if out.Unsatisfiable {
		return out
	}
	out.HasCode, out.Unsatisfiable = mergeBoolPtrAnd(a.HasCode, b.HasCode)
	if out.Unsatisfiable {
		return out
	}
	out.HasIncompleteTasks, out.Unsatisfiable = mergeBoolPtrAnd(a.HasIncompleteTasks, b.HasIncompleteTasks)
	if out.Unsatisfiable {
		return out
	}

	out.TagGroups = append(copyTagGroups(a.TagGroups), b.TagGroups...)
	out.ExcludeTagGroups = append(copyTagGroups(a.ExcludeTagGroups), b.ExcludeTagGroups...)
	return out
}

func mergePrefilterOr(a store.MemoSQLPrefilter, b store.MemoSQLPrefilter) store.MemoSQLPrefilter {
	if a.Unsatisfiable && b.Unsatisfiable {
		return store.MemoSQLPrefilter{Unsatisfiable: true}
	}
	if a.Unsatisfiable {
		return b
	}
	if b.Unsatisfiable {
		return a
	}

	out := store.EmptyMemoPrefilter()

	out.CreatorIDs = mergeSetOr(a.CreatorIDs, b.CreatorIDs)
	out.VisibilityIn = mergeVisibilityOr(a.VisibilityIn, b.VisibilityIn)
	out.StateIn = mergeStateOr(a.StateIn, b.StateIn)
	out.Pinned = mergeBoolPtrOr(a.Pinned, b.Pinned)
	out.HasLink = mergeBoolPtrOr(a.HasLink, b.HasLink)
	out.HasTaskList = mergeBoolPtrOr(a.HasTaskList, b.HasTaskList)
	out.HasCode = mergeBoolPtrOr(a.HasCode, b.HasCode)
	out.HasIncompleteTasks = mergeBoolPtrOr(a.HasIncompleteTasks, b.HasIncompleteTasks)
	out.TagGroups = mergeTagGroupsOr(a.TagGroups, b.TagGroups)
	out.ExcludeTagGroups = intersectTagGroups(a.ExcludeTagGroups, b.ExcludeTagGroups)

	return out
}

func normalizePrefilter(pf store.MemoSQLPrefilter) store.MemoSQLPrefilter {
	if pf.Unsatisfiable {
		return pf
	}
	pf.CreatorIDs = uniqueInt64(pf.CreatorIDs)
	pf.VisibilityIn = uniqueVisibility(pf.VisibilityIn)
	pf.StateIn = uniqueState(pf.StateIn)
	pf.TagGroups = normalizeTagGroups(pf.TagGroups)
	pf.ExcludeTagGroups = normalizeTagGroups(pf.ExcludeTagGroups)
	for _, group := range pf.TagGroups {
		if len(group.Options) == 0 {
			pf.Unsatisfiable = true
			return pf
		}
	}
	for _, group := range pf.ExcludeTagGroups {
		if len(group.Options) == 0 {
			pf.Unsatisfiable = true
			return pf
		}
	}
	return pf
}

func mergeSetAnd(a []int64, b []int64) ([]int64, bool) {
	switch {
	case len(a) == 0:
		return copyInt64Slice(b), false
	case len(b) == 0:
		return copyInt64Slice(a), false
	default:
		v := intersectInt64(a, b)
		if len(v) == 0 {
			return nil, true
		}
		return v, false
	}
}

func mergeVisibilityAnd(a []models.Visibility, b []models.Visibility) ([]models.Visibility, bool) {
	switch {
	case len(a) == 0:
		return append([]models.Visibility{}, b...), false
	case len(b) == 0:
		return append([]models.Visibility{}, a...), false
	default:
		v := intersectVisibility(a, b)
		if len(v) == 0 {
			return nil, true
		}
		return v, false
	}
}

func mergeStateAnd(a []models.MemoState, b []models.MemoState) ([]models.MemoState, bool) {
	switch {
	case len(a) == 0:
		return append([]models.MemoState{}, b...), false
	case len(b) == 0:
		return append([]models.MemoState{}, a...), false
	default:
		v := intersectState(a, b)
		if len(v) == 0 {
			return nil, true
		}
		return v, false
	}
}

func mergeBoolPtrAnd(a *bool, b *bool) (*bool, bool) {
	switch {
	case a == nil:
		return copyBoolPtr(b), false
	case b == nil:
		return copyBoolPtr(a), false
	default:
		if *a != *b {
			return nil, true
		}
		return copyBoolPtr(a), false
	}
}

func mergeSetOr(a []int64, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	return uniqueInt64(append(copyInt64Slice(a), b...))
}

func mergeVisibilityOr(a []models.Visibility, b []models.Visibility) []models.Visibility {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	return uniqueVisibility(append(append([]models.Visibility{}, a...), b...))
}

func mergeStateOr(a []models.MemoState, b []models.MemoState) []models.MemoState {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	return uniqueState(append(append([]models.MemoState{}, a...), b...))
}

func mergeBoolPtrOr(a *bool, b *bool) *bool {
	if a == nil || b == nil {
		return nil
	}
	if *a != *b {
		return nil
	}
	return copyBoolPtr(a)
}

func mergeTagGroupsOr(a []store.TagMatchGroup, b []store.TagMatchGroup) []store.TagMatchGroup {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	common := intersectTagGroups(a, b)
	if len(common) > 0 {
		return common
	}
	if len(a) == 1 && len(b) == 1 {
		merged := store.TagMatchGroup{
			Options: append(copyTagOptions(a[0].Options), b[0].Options...),
		}
		merged.Options = normalizeTagOptions(merged.Options)
		return []store.TagMatchGroup{merged}
	}
	return nil
}

func normalizeTagGroups(groups []store.TagMatchGroup) []store.TagMatchGroup {
	out := make([]store.TagMatchGroup, 0, len(groups))
	seen := map[string]struct{}{}
	for _, group := range groups {
		group.Options = normalizeTagOptions(group.Options)
		key := tagGroupKey(group)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, group)
	}
	return out
}

func normalizeTagOptions(options []store.TagMatchOption) []store.TagMatchOption {
	out := make([]store.TagMatchOption, 0, len(options))
	seen := map[string]struct{}{}
	for _, option := range options {
		key := fmt.Sprintf("%d:%s", option.Kind, option.Value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, option)
	}
	return out
}

func intersectTagGroups(a []store.TagMatchGroup, b []store.TagMatchGroup) []store.TagMatchGroup {
	setB := map[string]store.TagMatchGroup{}
	for _, group := range b {
		g := store.TagMatchGroup{Options: normalizeTagOptions(group.Options)}
		setB[tagGroupKey(g)] = g
	}
	out := make([]store.TagMatchGroup, 0)
	seen := map[string]struct{}{}
	for _, group := range a {
		g := store.TagMatchGroup{Options: normalizeTagOptions(group.Options)}
		key := tagGroupKey(g)
		if _, ok := setB[key]; !ok {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, g)
	}
	return out
}

func tagGroupKey(group store.TagMatchGroup) string {
	keys := make([]string, 0, len(group.Options))
	for _, option := range group.Options {
		keys = append(keys, fmt.Sprintf("%d:%s", option.Kind, option.Value))
	}
	slices.Sort(keys)
	return strings.Join(keys, "|")
}

func copyInt64Slice(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	out := make([]int64, len(values))
	copy(out, values)
	return out
}

func copyBoolPtr(v *bool) *bool {
	if v == nil {
		return nil
	}
	b := *v
	return &b
}

func copyTagGroups(groups []store.TagMatchGroup) []store.TagMatchGroup {
	if len(groups) == 0 {
		return nil
	}
	out := make([]store.TagMatchGroup, len(groups))
	for i, group := range groups {
		out[i] = store.TagMatchGroup{
			Options: copyTagOptions(group.Options),
		}
	}
	return out
}

func copyTagOptions(options []store.TagMatchOption) []store.TagMatchOption {
	if len(options) == 0 {
		return nil
	}
	out := make([]store.TagMatchOption, len(options))
	copy(out, options)
	return out
}

func ptrBool(v bool) *bool {
	return &v
}

func identAndConst(left *exprpb.Expr, right *exprpb.Expr) (string, *exprpb.Constant, bool) {
	id := left.GetIdentExpr()
	if id == nil {
		return "", nil, false
	}
	c := right.GetConstExpr()
	if c == nil {
		return "", nil, false
	}
	return id.Name, c, true
}

func constString(c *exprpb.Constant) (string, bool) {
	if c == nil {
		return "", false
	}
	switch v := c.ConstantKind.(type) {
	case *exprpb.Constant_StringValue:
		return v.StringValue, true
	default:
		return "", false
	}
}

func constBool(c *exprpb.Constant) (bool, bool) {
	if c == nil {
		return false, false
	}
	switch v := c.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		return v.BoolValue, true
	default:
		return false, false
	}
}

func constInt64(c *exprpb.Constant) (int64, bool) {
	if c == nil {
		return 0, false
	}
	switch v := c.ConstantKind.(type) {
	case *exprpb.Constant_Int64Value:
		return v.Int64Value, true
	case *exprpb.Constant_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return 0, false
		}
		return int64(v.Uint64Value), true
	default:
		return 0, false
	}
}

func extractTagExistsGroup(comp *exprpb.Expr_Comprehension) (store.TagMatchGroup, bool) {
	iterRange := comp.IterRange.GetIdentExpr()
	if iterRange == nil || iterRange.Name != "tags" {
		return store.TagMatchGroup{}, false
	}
	loop := comp.LoopStep.GetCallExpr()
	if loop == nil || loop.Function != "_||_" || len(loop.Args) != 2 {
		return store.TagMatchGroup{}, false
	}

	var predicate *exprpb.Expr
	if isIdent(loop.Args[0], comp.AccuVar) {
		predicate = loop.Args[1]
	} else if isIdent(loop.Args[1], comp.AccuVar) {
		predicate = loop.Args[0]
	} else {
		return store.TagMatchGroup{}, false
	}

	options, ok := extractTagPredicateOptions(predicate, comp.IterVar)
	if !ok || len(options) == 0 {
		return store.TagMatchGroup{}, false
	}
	return store.TagMatchGroup{Options: options}, true
}

func extractTagPredicateOptions(expr *exprpb.Expr, iterVar string) ([]store.TagMatchOption, bool) {
	call := expr.GetCallExpr()
	if call == nil {
		return nil, false
	}
	switch call.Function {
	case "_||_":
		if len(call.Args) != 2 {
			return nil, false
		}
		left, okLeft := extractTagPredicateOptions(call.Args[0], iterVar)
		right, okRight := extractTagPredicateOptions(call.Args[1], iterVar)
		if !okLeft || !okRight {
			return nil, false
		}
		return append(left, right...), true
	case "_==_":
		if len(call.Args) != 2 {
			return nil, false
		}
		if isIdent(call.Args[0], iterVar) {
			if s, ok := constString(call.Args[1].GetConstExpr()); ok {
				return []store.TagMatchOption{{Kind: store.TagMatchExact, Value: s}}, true
			}
		}
		if isIdent(call.Args[1], iterVar) {
			if s, ok := constString(call.Args[0].GetConstExpr()); ok {
				return []store.TagMatchOption{{Kind: store.TagMatchExact, Value: s}}, true
			}
		}
		return nil, false
	case "startsWith":
		if call.Target != nil && isIdent(call.Target, iterVar) && len(call.Args) == 1 {
			if s, ok := constString(call.Args[0].GetConstExpr()); ok {
				return []store.TagMatchOption{{Kind: store.TagMatchPrefix, Value: s}}, true
			}
		}
		// Fallback for non-target style startsWith(iterVar, "x")
		if len(call.Args) == 2 && isIdent(call.Args[0], iterVar) {
			if s, ok := constString(call.Args[1].GetConstExpr()); ok {
				return []store.TagMatchOption{{Kind: store.TagMatchPrefix, Value: s}}, true
			}
		}
		return nil, false
	default:
		return nil, false
	}
}

func isIdent(expr *exprpb.Expr, name string) bool {
	id := expr.GetIdentExpr()
	return id != nil && id.Name == name
}

func uniqueInt64(values []int64) []int64 {
	seen := map[int64]struct{}{}
	out := make([]int64, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func uniqueVisibility(values []models.Visibility) []models.Visibility {
	seen := map[models.Visibility]struct{}{}
	out := make([]models.Visibility, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func uniqueState(values []models.MemoState) []models.MemoState {
	seen := map[models.MemoState]struct{}{}
	out := make([]models.MemoState, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func intersectInt64(a []int64, b []int64) []int64 {
	set := map[int64]struct{}{}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := make([]int64, 0, len(a))
	for _, v := range a {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	return uniqueInt64(out)
}

func intersectVisibility(a []models.Visibility, b []models.Visibility) []models.Visibility {
	set := map[models.Visibility]struct{}{}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := make([]models.Visibility, 0, len(a))
	for _, v := range a {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	return uniqueVisibility(out)
}

func intersectState(a []models.MemoState, b []models.MemoState) []models.MemoState {
	set := map[models.MemoState]struct{}{}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := make([]models.MemoState, 0, len(a))
	for _, v := range a {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	return uniqueState(out)
}
