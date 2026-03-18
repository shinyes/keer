package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type LocalStore struct {
	baseDir string
}

func NewLocalStore(baseDir string) (*LocalStore, error) {
	cleanBaseDir := filepath.Clean(baseDir)
	if err := os.MkdirAll(cleanBaseDir, 0o750); err != nil {
		return nil, fmt.Errorf("create uploads dir: %w", err)
	}
	return &LocalStore{baseDir: cleanBaseDir}, nil
}

func (s *LocalStore) Put(_ context.Context, key string, _ string, data []byte) (int64, error) {
	return s.PutStream(context.Background(), key, "", bytes.NewReader(data), int64(len(data)))
}

func (s *LocalStore) PutStream(_ context.Context, key string, _ string, reader io.Reader, size int64) (int64, error) {
	cleanKey, err := sanitizeStorageKey(key)
	if err != nil {
		return 0, err
	}
	root, err := s.openBaseRoot()
	if err != nil {
		return 0, err
	}
	defer root.Close()
	parentDir := filepath.Dir(cleanKey)
	if parentDir != "." {
		if err := root.MkdirAll(parentDir, 0o750); err != nil {
			return 0, fmt.Errorf("create upload parent: %w", err)
		}
	}
	f, err := root.OpenFile(cleanKey, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("create upload file: %w", err)
	}
	defer f.Close()

	written, err := io.Copy(f, reader)
	if err != nil {
		return 0, fmt.Errorf("write upload file: %w", err)
	}
	if size >= 0 && written != size {
		return 0, fmt.Errorf("write upload file: size mismatch expected=%d actual=%d", size, written)
	}
	return written, nil
}

func (s *LocalStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	cleanKey, err := sanitizeStorageKey(key)
	if err != nil {
		return nil, err
	}
	root, err := s.openBaseRoot()
	if err != nil {
		return nil, err
	}
	f, err := root.Open(cleanKey)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	_ = root.Close()
	return f, nil
}

func (s *LocalStore) OpenRange(_ context.Context, key string, start int64, end int64) (io.ReadCloser, error) {
	if start < 0 {
		return nil, fmt.Errorf("invalid range start")
	}
	if end >= 0 && end < start {
		return nil, fmt.Errorf("invalid range end")
	}

	cleanKey, err := sanitizeStorageKey(key)
	if err != nil {
		return nil, err
	}
	root, err := s.openBaseRoot()
	if err != nil {
		return nil, err
	}
	f, err := root.Open(cleanKey)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	_ = root.Close()
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek upload file: %w", err)
	}
	if end < 0 {
		return f, nil
	}

	length := end - start + 1
	return &readerWithCloser{
		Reader: io.LimitReader(f, length),
		Closer: f,
	}, nil
}

func (s *LocalStore) Delete(_ context.Context, key string) error {
	cleanKey, err := sanitizeStorageKey(key)
	if err != nil {
		return err
	}
	root, err := s.openBaseRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Remove(cleanKey); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *LocalStore) ListKeys(_ context.Context, prefix string) ([]string, error) {
	keys := make([]string, 0)
	baseDir := filepath.Clean(s.baseDir)
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *LocalStore) Type() string {
	return TypeLocal
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

func sanitizeStorageKey(key string) (string, error) {
	cleanKey := filepath.ToSlash(filepath.Clean(strings.TrimSpace(key)))
	cleanKey = strings.TrimPrefix(cleanKey, "/")
	if cleanKey == "" || cleanKey == "." || cleanKey == ".." {
		return "", fmt.Errorf("invalid storage key")
	}
	if strings.HasPrefix(cleanKey, "../") || strings.Contains(cleanKey, ":") {
		return "", fmt.Errorf("invalid storage key traversal")
	}
	keyPath := filepath.FromSlash(cleanKey)
	if filepath.IsAbs(keyPath) {
		return "", fmt.Errorf("invalid storage key traversal")
	}
	return keyPath, nil
}

func (s *LocalStore) openBaseRoot() (*os.Root, error) {
	root, err := os.OpenRoot(s.baseDir)
	if err != nil {
		return nil, fmt.Errorf("open uploads dir root: %w", err)
	}
	return root, nil
}
