package service

import "testing"

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
