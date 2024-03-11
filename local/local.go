package local

import "context"

type LocalCache interface {
	Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key interface{}, args ...interface{}) error
	Load(ctx context.Context, key interface{}, args ...interface{}) error
	// Del(ctx context.Context, key interface{}, args ...interface{}) error
	// OnMessage(ctx context.Context, from interface{}, message interface{}) error
}
type Filter interface {
	// LocalCache
	Check(value []byte) (bool)
	Add(value []byte) bool
}
type LocalFilters interface {
	LocalCache
	Filter
	AddFilter(key string, filter Filter) error
}
type LocalCuckooFilter interface {
	LocalFilters
	Del(ctx context.Context, key string, value []byte) error
}
