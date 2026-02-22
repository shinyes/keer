package markdown

import (
	"unicode"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

const maxTagRunes = 100

type tagParser struct{}

func newTagParser() parser.InlineParser {
	return &tagParser{}
}

func (*tagParser) Trigger() []byte {
	return []byte{'#'}
}

func isValidTagRune(r rune) bool {
	if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsSymbol(r) {
		return true
	}
	return r == '_' || r == '-' || r == '/' || r == '&'
}

func (*tagParser) Parse(_ ast.Node, block text.Reader, _ parser.Context) ast.Node {
	line, _ := block.PeekLine()
	if len(line) == 0 || line[0] != '#' {
		return nil
	}
	if len(line) == 1 {
		return nil
	}
	if line[1] == '#' || line[1] == ' ' {
		return nil
	}

	pos := 1
	count := 0
	for pos < len(line) {
		r, size := utf8.DecodeRune(line[pos:])
		if r == utf8.RuneError && size == 1 {
			break
		}
		if !isValidTagRune(r) {
			break
		}
		count++
		if count > maxTagRunes {
			break
		}
		pos += size
	}
	if pos <= 1 {
		return nil
	}

	tag := make([]byte, pos-1)
	copy(tag, line[1:pos])
	block.Advance(pos)
	return &TagNode{Tag: tag}
}

type tagExtension struct{}

var TagExtension = &tagExtension{}

func (*tagExtension) Extend(m goldmark.Markdown) {
	m.Parser().AddOptions(
		parser.WithInlineParsers(
			util.Prioritized(newTagParser(), 200),
		),
	)
}
