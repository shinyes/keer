package http

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestListMemos_ReturnsQuotePayload(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	source := createMemoWithTagsForQuoteTest(t, app, token, "source memo", nil)
	quoteTag := "quote/src/remote/" + hex.EncodeToString([]byte(source.Name))
	quoted := createMemoWithTagsForQuoteTest(t, app, token, "quoted memo", []string{quoteTag})

	endpoint := "/api/v1/memos?filter=" + url.QueryEscape("creator_id == 1")
	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("list memos request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}

	var payload listMemosResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode list memos response failed: %v", err)
	}

	var quotedMemo *apiMemo
	for i := range payload.Memos {
		if payload.Memos[i].Name == quoted.Name {
			quotedMemo = &payload.Memos[i]
			break
		}
	}
	if quotedMemo == nil {
		t.Fatalf("expected quoted memo %q in response", quoted.Name)
	}
	if quotedMemo.Quote == nil {
		t.Fatalf("expected quote payload in memo response")
	}
	if quotedMemo.Quote.SourceKind != "remote" {
		t.Fatalf("expected quote sourceKind remote, got %q", quotedMemo.Quote.SourceKind)
	}
	if quotedMemo.Quote.Source != source.Name {
		t.Fatalf("expected quote source %q, got %q", source.Name, quotedMemo.Quote.Source)
	}
	if quotedMemo.Quote.Memo == nil {
		t.Fatalf("expected referenced memo preview in quote payload")
	}
	if quotedMemo.Quote.Memo.Name != source.Name {
		t.Fatalf("expected referenced memo name %q, got %q", source.Name, quotedMemo.Quote.Memo.Name)
	}
}

func createMemoWithTagsForQuoteTest(
	t *testing.T,
	app *fiber.App,
	token string,
	content string,
	tags []string,
) apiMemo {
	t.Helper()

	createPayload := map[string]any{
		"encryptedPayload": content,
		"payloadEnvelope": map[string]any{
			"wrappedKeys": []any{
				map[string]any{
					"slotType":      "account_master",
					"slotRef":       "users/1",
					"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
					"wrappedKey":    "wrapped-key",
				},
			},
		},
		"visibility":  "PRIVATE",
		"attachments": []any{},
	}
	if tags != nil {
		createPayload["tags"] = tags
	}
	createBody, _ := json.Marshal(createPayload)
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/memos", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := app.Test(createReq, 5000)
	if err != nil {
		t.Fatalf("create memo request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected create memo 200, got %d body=%s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created apiMemo
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create memo response failed: %v", err)
	}
	return created
}
