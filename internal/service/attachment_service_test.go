package service

import (
	"strings"
	"testing"
)

func TestParseMemoID_CompatibilityFormats(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "simple", input: "memos/9", want: 9},
		{name: "plain id", input: "9", want: 9},
		{name: "with suffix", input: "memos/9|local-uuid", want: 9},
		{name: "full path", input: "/api/v1/memos/9", want: 9},
		{name: "trailing slash", input: "memos/9/", want: 9},
		{name: "invalid", input: "memos/", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseMemoID(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: parseMemoID() error = %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: parseMemoID() got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestBuildAttachmentStorageKey_Format(t *testing.T) {
	key, err := buildAttachmentStorageKey(1, "16848.jpg")
	if err != nil {
		t.Fatalf("buildAttachmentStorageKey() error = %v", err)
	}

	if !strings.HasPrefix(key, "attachments/1/") {
		t.Fatalf("unexpected key prefix: %s", key)
	}
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		t.Fatalf("unexpected key format: %s", key)
	}

	filePart := parts[2]
	if !strings.HasSuffix(filePart, "_16848.jpg") {
		t.Fatalf("unexpected file part: %s", filePart)
	}
	nanoid := strings.TrimSuffix(filePart, "_16848.jpg")
	if len(nanoid) != 5 {
		t.Fatalf("unexpected nanoid length: got %d, key=%s", len(nanoid), key)
	}
	for _, ch := range nanoid {
		if !(ch >= '0' && ch <= '9') &&
			!(ch >= 'a' && ch <= 'z') &&
			!(ch >= 'A' && ch <= 'Z') &&
			ch != '-' &&
			ch != '_' {
			t.Fatalf("unexpected nanoid char %q in key=%s", ch, key)
		}
	}
}

func TestGenerateShortNanoID_Length(t *testing.T) {
	id, err := generateShortNanoID(5)
	if err != nil {
		t.Fatalf("generateShortNanoID() error = %v", err)
	}
	if len(id) != 5 {
		t.Fatalf("nanoid length got %d, want 5", len(id))
	}
}
