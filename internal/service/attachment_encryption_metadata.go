package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

func patchThumbnailBlobEncryptionMetadata(rawMetadata string, thumbnailBlobEncryption string) (string, error) {
	trimmedMetadata := strings.TrimSpace(rawMetadata)
	trimmedThumbnail := strings.TrimSpace(thumbnailBlobEncryption)
	if trimmedMetadata == "" {
		if trimmedThumbnail == "" {
			return "", nil
		}
		return "", fmt.Errorf("attachment encryption metadata is empty")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmedMetadata), &payload); err != nil {
		return "", fmt.Errorf("invalid attachment encryption metadata")
	}
	payload["thumbnailBlobEncryption"] = trimmedThumbnail

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
