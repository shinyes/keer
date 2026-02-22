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
	resp, err := app.Test(req)
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
