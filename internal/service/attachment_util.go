package service

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shinyes/keer/internal/storage"
)

func parseMemoID(name string) (int64, error) {
	raw := strings.TrimSpace(name)
	if raw == "" {
		return 0, fmt.Errorf("invalid memo name")
	}
	raw = strings.SplitN(raw, "|", 2)[0]
	raw = strings.Trim(raw, "/")
	if idx := strings.LastIndex(raw, "/"); idx >= 0 {
		raw = raw[idx+1:]
	}
	if raw == "" {
		return 0, fmt.Errorf("invalid memo name")
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memo id")
	}
	return id, nil
}

func sanitizeFilename(filename string) string {
	filename = strings.TrimSpace(filename)
	filename = filepath.Base(filename)
	if filename == "." || filename == ".." {
		return ""
	}
	filename = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, filename)
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return ""
	}
	return filename
}

func hashAttachmentContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func buildAttachmentStorageKey(userID int64, nanoID string) string {
	return fmt.Sprintf("attachments/%d/%s.bin", userID, nanoID)
}

func generateNanoID(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("invalid nano id length")
	}
	alphabet := attachmentNanoIDAlphabet
	buf := make([]byte, length)
	randBytes := make([]byte, length)
	if _, err := rand.Read(randBytes); err != nil {
		return "", fmt.Errorf("generate nano id: %w", err)
	}
	for i := 0; i < length; i++ {
		buf[i] = alphabet[int(randBytes[i])%len(alphabet)]
	}
	return string(buf), nil
}

func storageTypeName(s storage.Store) string {
	if s == nil {
		return storage.TypeLocal
	}
	return storage.NormalizeType(s.Type())
}

func encodeDirectSessionPath(storageKey string) string {
	return directSessionPathPrefix + strings.TrimSpace(storageKey)
}

func encodeMultipartSessionPath(storageKey string, multipartUploadID string, partSize int64) string {
	encodedStorageKey := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(storageKey)))
	encodedMultipartUploadID := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(multipartUploadID)))
	return fmt.Sprintf(
		"%s%s.%s.%d",
		multipartSessionPathPrefix,
		encodedStorageKey,
		encodedMultipartUploadID,
		partSize,
	)
}

func decodeDirectSessionPath(tempPath string) (string, bool) {
	raw := strings.TrimSpace(tempPath)
	if !strings.HasPrefix(raw, directSessionPathPrefix) {
		return "", false
	}
	key := strings.TrimSpace(strings.TrimPrefix(raw, directSessionPathPrefix))
	if key == "" {
		return "", false
	}
	return key, true
}

func decodeMultipartSessionPath(tempPath string) (multipartSessionInfo, bool) {
	raw := strings.TrimSpace(tempPath)
	if !strings.HasPrefix(raw, multipartSessionPathPrefix) {
		return multipartSessionInfo{}, false
	}
	payload := strings.TrimSpace(strings.TrimPrefix(raw, multipartSessionPathPrefix))
	if payload == "" {
		return multipartSessionInfo{}, false
	}

	if decoded, ok := decodeMultipartSessionPathEncoded(payload); ok {
		return decoded, true
	}
	return multipartSessionInfo{}, false
}

func decodeMultipartSessionPathEncoded(payload string) (multipartSessionInfo, bool) {
	parts := strings.Split(payload, ".")
	if len(parts) != 3 {
		return multipartSessionInfo{}, false
	}
	storageKeyBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(parts[0]))
	if err != nil {
		return multipartSessionInfo{}, false
	}
	multipartUploadIDBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(parts[1]))
	if err != nil {
		return multipartSessionInfo{}, false
	}
	storageKey := strings.TrimSpace(string(storageKeyBytes))
	multipartUploadID := strings.TrimSpace(string(multipartUploadIDBytes))
	partSize, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil || partSize <= 0 {
		return multipartSessionInfo{}, false
	}
	if storageKey == "" || multipartUploadID == "" {
		return multipartSessionInfo{}, false
	}
	return multipartSessionInfo{
		StorageKey:        storageKey,
		MultipartUploadID: multipartUploadID,
		PartSize:          partSize,
	}, true
}

func hashDirectUploadReference(userID int64, uploadID string, storageKey string, size int64) string {
	raw := fmt.Sprintf("s3-direct|%d|%s|%s|%d", userID, uploadID, storageKey, size)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func hashMultipartUploadReference(
	userID int64,
	uploadID string,
	storageKey string,
	size int64,
	parts []storage.S3UploadedPart,
) string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("s3-multipart|%d|%s|%s|%d", userID, uploadID, storageKey, size))
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("|%d:%d:%s", part.PartNumber, part.Size, part.ETag))
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

func isManagedStorageKey(key string) bool {
	return strings.HasPrefix(key, "attachments/") || strings.HasPrefix(key, "avatars/")
}

func sumUploadedPartSizes(parts []storage.S3UploadedPart) int64 {
	var total int64
	for _, part := range parts {
		total += part.Size
	}
	return total
}
