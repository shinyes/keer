package storage

import (
	"context"
	"io"
)

type Store interface {
	Put(ctx context.Context, key string, contentType string, data []byte) (int64, error)
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}
