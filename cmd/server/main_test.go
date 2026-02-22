package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/shinyes/keer/internal/config"
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

func TestCollectInteractiveS3Config(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		"https://s3.example.com",
		"auto",
		"memos",
		"key-id",
		"key-secret",
		"false",
	}, "\n") + "\n")
	var output bytes.Buffer

	cfg, err := collectInteractiveS3Config(input, &output, config.S3Config{
		Region:       "auto",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("collectInteractiveS3Config() error = %v", err)
	}
	if cfg.Endpoint != "https://s3.example.com" {
		t.Fatalf("endpoint mismatch: %q", cfg.Endpoint)
	}
	if cfg.Region != "auto" {
		t.Fatalf("region mismatch: %q", cfg.Region)
	}
	if cfg.Bucket != "memos" {
		t.Fatalf("bucket mismatch: %q", cfg.Bucket)
	}
	if cfg.AccessKeyID != "key-id" {
		t.Fatalf("access key id mismatch: %q", cfg.AccessKeyID)
	}
	if cfg.AccessSecret != "key-secret" {
		t.Fatalf("access secret mismatch: %q", cfg.AccessSecret)
	}
	if cfg.UsePathStyle {
		t.Fatalf("expected use path style false")
	}
}

func TestCollectInteractiveS3Config_KeepSecretAndDefaultBool(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		"", // keep endpoint default
		"", // keep region default
		"", // keep bucket default
		"", // keep access key id default
		"", // keep existing secret
		"", // keep bool default
	}, "\n") + "\n")
	var output bytes.Buffer

	cfg, err := collectInteractiveS3Config(input, &output, config.S3Config{
		Endpoint:     "https://old-s3.example.com",
		Region:       "auto",
		Bucket:       "memos-old",
		AccessKeyID:  "old-id",
		AccessSecret: "old-secret",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("collectInteractiveS3Config() error = %v", err)
	}
	if cfg.Endpoint != "https://old-s3.example.com" {
		t.Fatalf("endpoint mismatch: %q", cfg.Endpoint)
	}
	if cfg.Region != "auto" {
		t.Fatalf("region mismatch: %q", cfg.Region)
	}
	if cfg.Bucket != "memos-old" {
		t.Fatalf("bucket mismatch: %q", cfg.Bucket)
	}
	if cfg.AccessKeyID != "old-id" {
		t.Fatalf("access key id mismatch: %q", cfg.AccessKeyID)
	}
	if cfg.AccessSecret != "old-secret" {
		t.Fatalf("expected existing secret to be kept, got %q", cfg.AccessSecret)
	}
	if !cfg.UsePathStyle {
		t.Fatalf("expected use path style true")
	}
}

func TestCollectInteractiveS3Config_InvalidBool(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		"https://s3.example.com",
		"auto",
		"memos",
		"key-id",
		"key-secret",
		"not-bool",
	}, "\n"))
	var output bytes.Buffer

	_, err := collectInteractiveS3Config(input, &output, config.S3Config{
		Region:       "auto",
		UsePathStyle: true,
	})
	if err == nil {
		t.Fatalf("expected error for invalid bool input")
	}
}

func TestParseBoolInput(t *testing.T) {
	tests := []struct {
		input  string
		want   bool
		wantOK bool
	}{
		{input: "true", want: true, wantOK: true},
		{input: "yes", want: true, wantOK: true},
		{input: "1", want: true, wantOK: true},
		{input: "false", want: false, wantOK: true},
		{input: "no", want: false, wantOK: true},
		{input: "0", want: false, wantOK: true},
		{input: "abc", wantOK: false},
	}

	for _, tc := range tests {
		got, ok := parseBoolInput(tc.input)
		if ok != tc.wantOK {
			t.Fatalf("parseBoolInput(%q) ok=%v want %v", tc.input, ok, tc.wantOK)
		}
		if ok && got != tc.want {
			t.Fatalf("parseBoolInput(%q) got=%v want %v", tc.input, got, tc.want)
		}
	}
}
