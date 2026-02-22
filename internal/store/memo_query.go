package store

import "github.com/shinyes/keer/internal/models"

type TagMatchKind int

const (
	TagMatchExact TagMatchKind = iota + 1
	TagMatchPrefix
)

type TagMatchOption struct {
	Kind  TagMatchKind
	Value string
}

type TagMatchGroup struct {
	Options []TagMatchOption
}

type MemoSQLPrefilter struct {
	Unsatisfiable bool

	CreatorIDs   []int64
	VisibilityIn []models.Visibility
	StateIn      []models.MemoState
	Pinned       *bool

	HasLink            *bool
	HasTaskList        *bool
	HasCode            *bool
	HasIncompleteTasks *bool

	TagGroups        []TagMatchGroup
	ExcludeTagGroups []TagMatchGroup
}

func EmptyMemoPrefilter() MemoSQLPrefilter {
	return MemoSQLPrefilter{}
}
