package local

import "context"

type LocalCache interface {
	Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key interface{}, args ...interface{}) error
	Load(ctx context.Context, key interface{}, args ...interface{}) error
	// Del(ctx context.Context, key interface{}, args ...interface{}) error
	// OnMessage(ctx context.Context, from interface{}, message interface{}) error
}
type LocalFilter interface {
	LocalCache
	Check(ctx context.Context, key string, value []byte) (bool, error)
	Add(ctx context.Context, key string, value []byte) error
}
type LocalCuckooFilter interface {
	LocalFilter
	Del(ctx context.Context, key string, value []byte) error
}
