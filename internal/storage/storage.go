package storage

import (
	"context"
	"io"
)

type Store interface {
	Put(ctx context.Context, key string, contentType string, data []byte) (int64, error)
	PutStream(ctx context.Context, key string, contentType string, reader io.Reader, size int64) (int64, error)
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// OpenRange opens [start, end] (inclusive). If end is negative, it reads to EOF.
	OpenRange(ctx context.Context, key string, start int64, end int64) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}
