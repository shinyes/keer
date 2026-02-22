package main

import (
	"testing"
	"time"
)

func TestParseTTL(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{input: "24h", want: 24 * time.Hour},
		{input: "7d", want: 7 * 24 * time.Hour},
		{input: "2day", want: 2 * 24 * time.Hour},
		{input: "3days", want: 3 * 24 * time.Hour},
		{input: "1.5d", want: 36 * time.Hour},
		{input: "0d", wantErr: true},
		{input: "-1d", wantErr: true},
		{input: "abc", wantErr: true},
	}

	for _, tc := range tests {
		got, err := parseTTL(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseTTL(%q) expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseTTL(%q) unexpected error: %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("parseTTL(%q) got %s, want %s", tc.input, got, tc.want)
		}
	}
}

func TestParseCommandLine(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:  "simple",
			input: "user create demo pass",
			want:  []string{"user", "create", "demo", "pass"},
		},
		{
			name:  "quoted",
			input: "token create demo \"mobile token\" --ttl 7d",
			want:  []string{"token", "create", "demo", "mobile token", "--ttl", "7d"},
		},
		{
			name:  "single quote",
			input: "token create demo 'token with space'",
			want:  []string{"token", "create", "demo", "token with space"},
		},
		{
			name:  "apostrophe in token",
			input: "user create cyk cyk'slife cyk admin",
			want:  []string{"user", "create", "cyk", "cyk'slife", "cyk", "admin"},
		},
		{
			name:    "unterminated quote",
			input:   "token create demo \"bad",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got, err := parseCommandLine(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s: args len got %d want %d", tc.name, len(got), len(tc.want))
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: arg[%d] got %q want %q", tc.name, i, got[i], tc.want[i])
			}
		}
	}
}
