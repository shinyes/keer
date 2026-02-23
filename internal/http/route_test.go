package http

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestFiberUserStatsRoutePattern(t *testing.T) {
	app := fiber.New()
	app.Get("/api/v1/users/:id\\:getStats", func(c *fiber.Ctx) error {
		return c.SendString(c.Params("id"))
	})

	req := httptest.NewRequest("GET", "/api/v1/users/123:getStats", nil)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("app.Test() error = %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "123" {
		t.Fatalf("expected id=123, got %q", string(body))
	}
}

func TestParseSingleByteRange(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		size      int64
		wantStart int64
		wantEnd   int64
		wantRange bool
		wantErr   bool
	}{
		{
			name:      "empty",
			raw:       "",
			size:      1000,
			wantRange: false,
		},
		{
			name:      "open ended",
			raw:       "bytes=100-",
			size:      1000,
			wantStart: 100,
			wantEnd:   999,
			wantRange: true,
		},
		{
			name:      "bounded",
			raw:       "bytes=100-199",
			size:      1000,
			wantStart: 100,
			wantEnd:   199,
			wantRange: true,
		},
		{
			name:      "suffix",
			raw:       "bytes=-200",
			size:      1000,
			wantStart: 800,
			wantEnd:   999,
			wantRange: true,
		},
		{
			name:      "suffix larger than size",
			raw:       "bytes=-2000",
			size:      1000,
			wantStart: 0,
			wantEnd:   999,
			wantRange: true,
		},
		{
			name:      "end overflow clipped",
			raw:       "bytes=900-9999",
			size:      1000,
			wantStart: 900,
			wantEnd:   999,
			wantRange: true,
		},
		{
			name:      "invalid unit",
			raw:       "items=0-1",
			size:      1000,
			wantRange: true,
			wantErr:   true,
		},
		{
			name:      "multi range unsupported",
			raw:       "bytes=0-1,2-3",
			size:      1000,
			wantRange: true,
			wantErr:   true,
		},
		{
			name:      "start out of bounds",
			raw:       "bytes=1000-",
			size:      1000,
			wantRange: true,
			wantErr:   true,
		},
		{
			name:      "end before start",
			raw:       "bytes=200-100",
			size:      1000,
			wantRange: true,
			wantErr:   true,
		},
		{
			name:      "invalid resource size",
			raw:       "bytes=0-1",
			size:      0,
			wantRange: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			start, end, hasRange, err := parseSingleByteRange(tt.raw, tt.size)
			if hasRange != tt.wantRange {
				t.Fatalf("hasRange = %v, want %v", hasRange, tt.wantRange)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if start != tt.wantStart || end != tt.wantEnd {
				t.Fatalf("range = %d-%d, want %d-%d", start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}
