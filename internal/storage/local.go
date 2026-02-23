package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LocalStore struct {
	baseDir string
}

func NewLocalStore(baseDir string) (*LocalStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create uploads dir: %w", err)
	}
	return &LocalStore{baseDir: baseDir}, nil
}

func (s *LocalStore) Put(_ context.Context, key string, _ string, data []byte) (int64, error) {
	path, err := s.pathFor(key)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, fmt.Errorf("create upload parent: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return 0, fmt.Errorf("write upload file: %w", err)
	}
	return int64(len(data)), nil
}

func (s *LocalStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	path, err := s.pathFor(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *LocalStore) OpenRange(_ context.Context, key string, start int64, end int64) (io.ReadCloser, error) {
	if start < 0 {
		return nil, fmt.Errorf("invalid range start")
	}
	if end >= 0 && end < start {
		return nil, fmt.Errorf("invalid range end")
	}

	path, err := s.pathFor(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
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
	path, err := s.pathFor(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

func (s *LocalStore) pathFor(key string) (string, error) {
	cleanKey := filepath.ToSlash(filepath.Clean(strings.TrimSpace(key)))
	cleanKey = strings.TrimPrefix(cleanKey, "/")
	if cleanKey == "" || cleanKey == "." {
		return "", fmt.Errorf("invalid storage key")
	}
	path := filepath.Join(s.baseDir, filepath.FromSlash(cleanKey))
	rel, err := filepath.Rel(s.baseDir, path)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("invalid storage key traversal")
	}
	return path, nil
}
