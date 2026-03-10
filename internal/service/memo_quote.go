package service

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/shinyes/keer/internal/models"
)

const (
	memoQuoteTagPrefix         = "quote/src/"
	memoQuoteSourceKindLocal   = MemoQuoteSourceKind("local")
	memoQuoteSourceKindRemote  = MemoQuoteSourceKind("remote")
	memoQuoteSegmentCount      = 4
	memoQuoteSourceKindSegment = 2
	memoQuoteSourceHexSegment  = 3
)

type MemoQuoteSourceKind string

type MemoQuoteDescriptor struct {
	SourceKind MemoQuoteSourceKind
	Source     string
}

type MemoQuoteMemo struct {
	Memo        models.Memo
	Attachments []models.Attachment
}

type MemoQuote struct {
	SourceKind MemoQuoteSourceKind
	Source     string
	Memo       *MemoQuoteMemo
}

func parseMemoQuoteDescriptor(tags []string) (MemoQuoteDescriptor, bool) {
	for _, rawTag := range tags {
		tag := strings.TrimSpace(rawTag)
		if !strings.HasPrefix(tag, memoQuoteTagPrefix) {
			continue
		}
		parts := strings.Split(tag, "/")
		if len(parts) != memoQuoteSegmentCount {
			continue
		}

		sourceKindRaw := strings.TrimSpace(parts[memoQuoteSourceKindSegment])
		sourceKind := MemoQuoteSourceKind(sourceKindRaw)
		switch sourceKind {
		case memoQuoteSourceKindLocal, memoQuoteSourceKindRemote:
		default:
			continue
		}

		source, ok := decodeMemoQuoteSource(parts[memoQuoteSourceHexSegment])
		if !ok {
			continue
		}

		return MemoQuoteDescriptor{
			SourceKind: sourceKind,
			Source:     source,
		}, true
	}

	return MemoQuoteDescriptor{}, false
}

func decodeMemoQuoteSource(encoded string) (string, bool) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return "", false
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return "", false
	}
	if !utf8.Valid(decoded) {
		return "", false
	}
	source := strings.TrimSpace(string(decoded))
	if source == "" {
		return "", false
	}
	return source, true
}

func parseMemoIDFromQuoteSource(source string) (int64, bool) {
	source = strings.TrimSpace(source)
	if source == "" {
		return 0, false
	}

	source = strings.SplitN(source, "|", 2)[0]
	source = strings.Trim(source, "/")
	if source == "" {
		return 0, false
	}

	segments := strings.Split(source, "/")
	lastSegment := strings.TrimSpace(segments[len(segments)-1])
	if lastSegment == "" {
		return 0, false
	}
	memoID, err := strconv.ParseInt(lastSegment, 10, 64)
	if err != nil || memoID <= 0 {
		return 0, false
	}

	if len(segments) == 1 {
		return memoID, true
	}
	prevSegment := strings.ToLower(strings.TrimSpace(segments[len(segments)-2]))
	if prevSegment != "memos" {
		return 0, false
	}
	return memoID, true
}
