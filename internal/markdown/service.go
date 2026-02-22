package markdown

import (
	"github.com/shinyes/keer/internal/models"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

type Service struct {
	md goldmark.Markdown
}

func NewService() *Service {
	md := goldmark.New(
		goldmark.WithExtensions(
			extension.GFM,
			TagExtension,
		),
		goldmark.WithParserOptions(
			parser.WithAutoHeadingID(),
		),
	)
	return &Service{md: md}
}

func (s *Service) ExtractPayload(content string) (models.MemoPayload, error) {
	root, err := s.parse([]byte(content))
	if err != nil {
		return models.MemoPayload{}, err
	}

	tags := make([]string, 0)
	prop := models.MemoPayloadProperty{}

	err = ast.Walk(root, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}

		if tagNode, ok := n.(*TagNode); ok {
			tags = append(tags, string(tagNode.Tag))
		}

		switch n.Kind() {
		case ast.KindLink:
			prop.HasLink = true
		case ast.KindCodeSpan, ast.KindCodeBlock, ast.KindFencedCodeBlock:
			prop.HasCode = true
		case east.KindTaskCheckBox:
			prop.HasTaskList = true
			if checkBox, ok := n.(*east.TaskCheckBox); ok && !checkBox.IsChecked {
				prop.HasIncompleteTasks = true
			}
		}

		return ast.WalkContinue, nil
	})
	if err != nil {
		return models.MemoPayload{}, err
	}

	return models.MemoPayload{
		Tags:     uniquePreserveCase(tags),
		Property: prop,
	}, nil
}

func (s *Service) ExtractTags(content string) ([]string, error) {
	payload, err := s.ExtractPayload(content)
	if err != nil {
		return nil, err
	}
	return payload.Tags, nil
}

func (s *Service) parse(content []byte) (ast.Node, error) {
	reader := text.NewReader(content)
	root := s.md.Parser().Parse(reader)
	return root, nil
}

func uniquePreserveCase(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, tag)
	}
	return result
}
