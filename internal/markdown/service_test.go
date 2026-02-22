package markdown

import (
	"strings"
	"testing"
)

func TestExtractTags_BasicAndUnicode(t *testing.T) {
	svc := NewService()
	content := strings.Join([]string{
		"#work #项目 #テスト #테스트 #book/fiction #science&tech #2024 #go🚀",
	}, "\n")
	tags, err := svc.ExtractTags(content)
	if err != nil {
		t.Fatalf("ExtractTags() error = %v", err)
	}

	expect := []string{"work", "项目", "テスト", "테스트", "book/fiction", "science&tech", "2024", "go🚀"}
	assertStringSlicesEqual(t, expect, tags)
}

func TestExtractTags_HeadingAndPunctuation(t *testing.T) {
	svc := NewService()
	content := strings.Join([]string{
		"## heading should not be tag",
		"# heading should not be tag",
		"real tag: #real, and #done.",
	}, "\n")
	tags, err := svc.ExtractTags(content)
	if err != nil {
		t.Fatalf("ExtractTags() error = %v", err)
	}

	expect := []string{"real", "done"}
	assertStringSlicesEqual(t, expect, tags)
}

func TestExtractTags_MaxLength100Runes(t *testing.T) {
	svc := NewService()
	long := strings.Repeat("测", 101)
	content := "#" + long
	tags, err := svc.ExtractTags(content)
	if err != nil {
		t.Fatalf("ExtractTags() error = %v", err)
	}

	if len(tags) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(tags))
	}
	if got := len([]rune(tags[0])); got != 100 {
		t.Fatalf("expected tag rune length 100, got %d", got)
	}
}

func TestExtractTags_DedupCaseSensitive(t *testing.T) {
	svc := NewService()
	content := "#work #Work #work #WORK"
	tags, err := svc.ExtractTags(content)
	if err != nil {
		t.Fatalf("ExtractTags() error = %v", err)
	}

	expect := []string{"work", "Work", "WORK"}
	assertStringSlicesEqual(t, expect, tags)
}

func assertStringSlicesEqual(t *testing.T, expected []string, actual []string) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("length mismatch expected=%d actual=%d, actual=%v", len(expected), len(actual), actual)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("index %d mismatch expected=%q actual=%q", i, expected[i], actual[i])
		}
	}
}
