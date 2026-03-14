package storage

import (
	"context"
	"io"
	"sort"
	"strings"
)

const (
	TypeLocal = "LOCAL"
	TypeS3    = "S3"
)

type Store interface {
	Put(ctx context.Context, key string, contentType string, data []byte) (int64, error)
	PutStream(ctx context.Context, key string, contentType string, reader io.Reader, size int64) (int64, error)
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// OpenRange opens [start, end] (inclusive). If end is negative, it reads to EOF.
	OpenRange(ctx context.Context, key string, start int64, end int64) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	Type() string
}

type Router struct {
	defaultType string
	stores      map[string]Store
}

func NewRouter(defaultType string, stores ...Store) *Router {
	normalizedDefault := NormalizeType(defaultType)
	registry := make(map[string]Store, len(stores))
	for _, store := range stores {
		if store == nil {
			continue
		}
		registry[NormalizeType(store.Type())] = store
	}
	if normalizedDefault == "" {
		for storeType := range registry {
			normalizedDefault = storeType
			break
		}
	}
	return &Router{
		defaultType: normalizedDefault,
		stores:      registry,
	}
}

func (r *Router) DefaultType() string {
	return r.defaultType
}

func (r *Router) DefaultStore() Store {
	store, _ := r.StoreForType(r.defaultType)
	return store
}

func (r *Router) StoreForType(storeType string) (Store, bool) {
	store, ok := r.stores[NormalizeType(storeType)]
	return store, ok
}

func (r *Router) Stores() []Store {
	items := make([]Store, 0, len(r.stores))
	keys := make([]string, 0, len(r.stores))
	for storeType := range r.stores {
		keys = append(keys, storeType)
	}
	sort.Strings(keys)
	for _, storeType := range keys {
		items = append(items, r.stores[storeType])
	}
	return items
}

func NormalizeType(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case TypeS3:
		return TypeS3
	default:
		return TypeLocal
	}
}
