package golayeredcache

import (
	"context"
	"fmt"

	"github.com/allegro/bigcache/v3"
	"github.com/begonia-org/go-layered-cache/gobloom"
	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/local"
	"github.com/begonia-org/go-layered-cache/source"
)

func NewKeyValueCache(ctx context.Context, options LayeredBuildOptions, cacheConfig bigcache.Config) (LayeredKeyValueCache, error) {
	big, err := bigcache.New(ctx, cacheConfig)
	if err != nil {
		return nil, err
	}
	local := local.NewLocalCache(big)
	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewCacheSourceImpl(redSource, options.Channel.(string), options.Log)
	layered := NewLayeredCacheImpl(source, local, options.Log, options.Strategy)
	return NewLayeredKeyValueCacheImpl(layered, options.KeyPrefix, options.Log), nil
}

func NewLayeredCuckoo(options *LayeredBuildOptions, buildCuckooOptions *gocuckoo.CuckooBuildOptions) LayeredCuckooFilter {

	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewCuckooSourceImpl(redSource, options.Channel.(string), options.Log)
	local := local.NewLocalCuckooFilters(make(map[string]*gocuckoo.GoCuckooFilterImpl), buildCuckooOptions)
	layered := NewLayeredCuckooFilterImpl(NewLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*", options.KeyPrefix), options.Log)
	return layered
}

func NewLayeredBloom(options *LayeredBuildOptions, bloomOptions *gobloom.BloomBuildOptions) LayeredFilter {

	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewBloomSourceImpl(redSource, options.Channel.(string), options.Log)
	local := local.NewLocalBloomFilters(make(map[string]*gobloom.GoBloomChain), bloomOptions)
	layered := NewLayeredBloomFilter(NewLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*", options.KeyPrefix), options.Log)
	return layered
}
