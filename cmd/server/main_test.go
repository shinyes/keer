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
