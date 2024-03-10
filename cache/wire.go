package cache

import (
	"context"

	"github.com/allegro/bigcache/v3"
	golayeredcache "github.com/begonia-org/go-layered-cache"
)

func New(ctx context.Context, options golayeredcache.LayeredBuildOptions, cacheConfig bigcache.Config) (*LayeredCacheFilter, error) {
	big, err := bigcache.New(ctx, cacheConfig)
	if err != nil {
		return nil, err
	}
	local := NewLocalCache(big)
	redSource := golayeredcache.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := NewCacheSourceImpl(redSource, options.Channel.(string), options.Log)
	layered := golayeredcache.NewLayeredCacheImpl(source, local, options.Log, options.Strategy)
	return NewLayeredCacheFilter(layered, options.KeyPrefix, options.Log), nil
}
