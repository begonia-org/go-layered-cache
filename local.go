package golayeredcache

import (
	"context"
)

type LocalCache interface {
	Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key interface{}, args ...interface{}) error
	Load(ctx context.Context, key interface{}, args ...interface{}) error
	// Del(ctx context.Context, key interface{}, args ...interface{}) error
	// OnMessage(ctx context.Context, from interface{}, message interface{}) error
}
