package cache

import (
	"context"

	"github.com/allegro/bigcache/v3"
)

func New(ctx context.Context, cfg *LayeredCacheConfig) *LayeredCacheImpl {
	big, err := bigcache.New(ctx, cfg.Config)
	if err != nil {
		cfg.Log.Error("bigcache.New", err)
	}
	local := NewLocalCache(big)
	source := NewSourceCache(cfg.RDB, cfg.Log,cfg.WatchKey,cfg.ReadStreamBlock,cfg.ReadMessageSize)
	return NewLayeredCache(local, source, cfg.Log, cfg.ReadStrategy)
}
