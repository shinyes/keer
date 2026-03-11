package http

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateMemo_UsesAssociationAttachmentMetadata(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createAttachmentPayload := map[string]any{
		"filename":             "note.bin",
		"type":                 "application/octet-stream",
		"content":              base64.StdEncoding.EncodeToString([]byte("attachment-bytes")),
		"descriptorCiphertext": "base-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "base-wrapped-key",
			},
		}},
		"blobEncryption": "base-blob",
	}
	createAttachmentBody, _ := json.Marshal(createAttachmentPayload)
	createAttachmentReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments", bytes.NewReader(createAttachmentBody))
	createAttachmentReq.Header.Set("Authorization", "Bearer "+token)
	createAttachmentReq.Header.Set("Content-Type", "application/json")

	createAttachmentResp, err := app.Test(createAttachmentReq, 5000)
	if err != nil {
		t.Fatalf("create attachment request failed: %v", err)
	}
	defer createAttachmentResp.Body.Close()
	if createAttachmentResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createAttachmentResp.Body)
		t.Fatalf("expected attachment create 200, got %d body=%s", createAttachmentResp.StatusCode, string(body))
	}

	var createdAttachment apiAttachment
	if err := json.NewDecoder(createAttachmentResp.Body).Decode(&createdAttachment); err != nil {
		t.Fatalf("decode created attachment failed: %v", err)
	}

	createMemoPayload := map[string]any{
		"encryptedPayload": "memo-ciphertext",
		"payloadEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "account_master",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "memo-wrapped-key",
			},
		}},
		"visibility": "PRIVATE",
		"attachments": []map[string]any{
			{
				"name":                 createdAttachment.Name,
				"descriptorCiphertext": "assoc-descriptor",
				"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
					map[string]any{
						"slotType":      "account_master",
						"slotRef":       "users/1",
						"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
						"wrappedKey":    "assoc-wrapped-key",
					},
				}},
				"blobEncryption": "assoc-blob",
			},
		},
	}
	createMemoBody, _ := json.Marshal(createMemoPayload)
	createMemoReq := httptest.NewRequest(http.MethodPost, "/api/v1/memos", bytes.NewReader(createMemoBody))
	createMemoReq.Header.Set("Authorization", "Bearer "+token)
	createMemoReq.Header.Set("Content-Type", "application/json")

	createMemoResp, err := app.Test(createMemoReq, 5000)
	if err != nil {
		t.Fatalf("create memo request failed: %v", err)
	}
	defer createMemoResp.Body.Close()
	if createMemoResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createMemoResp.Body)
		t.Fatalf("expected memo create 200, got %d body=%s", createMemoResp.StatusCode, string(body))
	}

	var createdMemo apiMemo
	if err := json.NewDecoder(createMemoResp.Body).Decode(&createdMemo); err != nil {
		t.Fatalf("decode created memo failed: %v", err)
	}
	if len(createdMemo.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(createdMemo.Attachments))
	}
	if createdMemo.Attachments[0].DescriptorCiphertext != "assoc-descriptor" {
		t.Fatalf("expected assoc descriptor, got %q", createdMemo.Attachments[0].DescriptorCiphertext)
	}
	if createdMemo.Attachments[0].BlobEncryption != "assoc-blob" {
		t.Fatalf("expected assoc blob metadata, got %q", createdMemo.Attachments[0].BlobEncryption)
	}
}

func TestCreateAttachment_RejectsUnsupportedDescriptorEnvelopeSlotType(t *testing.T) {
	app := newTestApp(t, true, true)
	token := "demo-token"

	createAttachmentPayload := map[string]any{
		"filename":             "note.bin",
		"type":                 "application/octet-stream",
		"content":              base64.StdEncoding.EncodeToString([]byte("attachment-bytes")),
		"descriptorCiphertext": "base-descriptor",
		"descriptorEnvelope": map[string]any{"wrappedKeys": []any{
			map[string]any{
				"slotType":      "invalid_slot",
				"slotRef":       "users/1",
				"wrapAlgorithm": "AES_GCM_ACCOUNT_MASTER_KEY_V1",
				"wrappedKey":    "base-wrapped-key",
			},
		}},
		"blobEncryption": "base-blob",
	}
	createAttachmentBody, _ := json.Marshal(createAttachmentPayload)
	createAttachmentReq := httptest.NewRequest(http.MethodPost, "/api/v1/attachments", bytes.NewReader(createAttachmentBody))
	createAttachmentReq.Header.Set("Authorization", "Bearer "+token)
	createAttachmentReq.Header.Set("Content-Type", "application/json")

	createAttachmentResp, err := app.Test(createAttachmentReq, 5000)
	if err != nil {
		t.Fatalf("create attachment request failed: %v", err)
	}
	defer createAttachmentResp.Body.Close()
	if createAttachmentResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(createAttachmentResp.Body)
		t.Fatalf("expected attachment create 400, got %d body=%s", createAttachmentResp.StatusCode, string(body))
	}
}
