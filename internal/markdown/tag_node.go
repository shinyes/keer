package markdown

import (
	"github.com/yuin/goldmark/ast"
)

var KindTag = ast.NewNodeKind("Tag")

type TagNode struct {
	ast.BaseInline
	Tag []byte
}

func (*TagNode) Kind() ast.NodeKind {
	return KindTag
}

func (n *TagNode) Dump(source []byte, level int) {
	ast.DumpHelper(n, source, level, map[string]string{
		"Tag": string(n.Tag),
	}, nil)
}
