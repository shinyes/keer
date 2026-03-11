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
			input: "user create foo secret foo admin",
			want:  []string{"user", "create", "foo", "secret", "foo", "admin"},
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

func TestParseTokenListArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantID     string
		wantAll    bool
		shouldFail bool
	}{
		{
			name:    "identifier only",
			args:    []string{"alice"},
			wantID:  "alice",
			wantAll: false,
		},
		{
			name:    "identifier with all",
			args:    []string{"alice", "--all"},
			wantID:  "alice",
			wantAll: true,
		},
		{
			name:    "all before identifier",
			args:    []string{"--all", "1"},
			wantID:  "1",
			wantAll: true,
		},
		{
			name:       "missing identifier",
			args:       []string{"--all"},
			shouldFail: true,
		},
		{
			name:       "unknown option",
			args:       []string{"alice", "--unknown"},
			shouldFail: true,
		},
		{
			name:       "extra positional",
			args:       []string{"alice", "bob"},
			shouldFail: true,
		},
	}

	for _, tc := range tests {
		gotID, gotAll, err := parseTokenListArgs(tc.args)
		if tc.shouldFail {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if gotID != tc.wantID {
			t.Fatalf("%s: id got %q want %q", tc.name, gotID, tc.wantID)
		}
		if gotAll != tc.wantAll {
			t.Fatalf("%s: all got %v want %v", tc.name, gotAll, tc.wantAll)
		}
	}
}

func TestResolveTokenExpiresAt(t *testing.T) {
	now := time.Date(2026, 2, 22, 16, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		ttlRaw  string
		want    *time.Time
		wantErr bool
	}{
		{
			name:    "default ttl 7d",
			want:    ptrTime(now.Add(7 * 24 * time.Hour)),
			wantErr: false,
		},
		{
			name:    "explicit ttl",
			ttlRaw:  "24h",
			want:    ptrTime(now.Add(24 * time.Hour)),
			wantErr: false,
		},
		{
			name:    "invalid ttl",
			ttlRaw:  "bad",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got, err := resolveTokenExpiresAt(tc.ttlRaw, now)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if got == nil {
			t.Fatalf("%s: expected non-nil expiresAt", tc.name)
		}
		if !got.Equal(*tc.want) {
			t.Fatalf("%s: expiresAt got %s want %s", tc.name, got.UTC().Format(time.RFC3339), tc.want.UTC().Format(time.RFC3339))
		}
	}
}

func ptrTime(v time.Time) *time.Time {
	return &v
}
