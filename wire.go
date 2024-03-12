// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"context"
	"fmt"

	"github.com/begonia-org/go-layered-cache/gobloom"
	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/local"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// NewKeyValueCache creates a new LayeredKeyValueCache. It is only for key-value cache.
// The cache is a layered cache, which means it has a local cache and a source cache.
// The local cache is a bigcache, and the source cache is a redis cache.
//
// Parameters:
//   - ctx: context.Context
//   - options: LayeredBuildOptions is a struct that contains the options for building the cache.
//   - cacheConfig: bigcache.Config is the configuration for the bigcache.
//   - maxEntries: int is the max entries for the bigcache.
//
// Returns:
//   - LayeredKeyValueCache: the new LayeredKeyValueCache
//   - error: if any error occurs
func NewKeyValueCache(ctx context.Context, options LayeredBuildOptions, maxEntries int) (LayeredKeyValueCache, error) {

	local,err := local.NewLocalCache(ctx, maxEntries)
	if err != nil {
		return nil, err
	
	}
	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewCacheSourceImpl(redSource, options.Channel.(string), options.Log)
	layered := newBaseLayeredCacheImpl(source, local, options.Log, options.Strategy)
	return newLayeredKeyValueCacheImpl(layered, options.KeyPrefix, options.Log), nil
}

// NewLayeredCuckoo creates a new LayeredCuckooFilter. It is only for cuckoo filter.
// The cuckoo filter is a layered cache, which means it has a local cuckoo filter cache and a redis cuckoo filter cache.
//
// Parameters:
//   - options: LayeredBuildOptions is a struct that contains the options for building the layered cache.
//   - buildCuckooOptions: gocuckoo.CuckooBuildOptions is the configuration for the local cuckoo filter.
//
// Returns:
//   - LayeredCuckooFilter: the new LayeredCuckooFilter
func NewLayeredCuckoo(options *LayeredBuildOptions, buildCuckooOptions gocuckoo.CuckooBuildOptions, extOptions ...LayeredFilterOptions) LayeredCuckooFilter {

	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewCuckooSourceImpl(redSource, options.Channel.(string), options.Log)
	local := local.NewLocalCuckooFilters(make(map[string]*gocuckoo.GoCuckooFilterImpl), buildCuckooOptions)
	layered := newLayeredCuckooFilterImpl(newBaseLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*", options.KeyPrefix), options.Log)
	for _, extOption := range extOptions {
		err := extOption(layered)
		if err != nil {
			options.Log.Error(err.Error())
		}

	}
	return layered
}

// NewLayeredBloom creates a new LayeredBloomFilter. It is only for bloom filter.
// The bloom filter is a layered cache, which means it has a local bloom filter cache and a redis bloom filter cache.
//
// Parameters:
//   - options: LayeredBuildOptions is a struct that contains the options for building the layered cache.
//   - bloomOptions: gobloom.BloomBuildOptions is the configuration for the local bloom filter.
//
// Returns:
//   - LayeredBloomFilter: the new LayeredBloomFilter
func NewLayeredBloom(options *LayeredBuildOptions, bloomOptions gobloom.BloomBuildOptions, extOptions ...LayeredFilterOptions) LayeredFilter {

	redSource := source.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := source.NewBloomSourceImpl(redSource, options.Channel.(string), options.Log)
	local := local.NewLocalBloomFilters(make(map[string]*gobloom.GoBloomChain), bloomOptions)
	layered := newLayeredBloomFilter(newBaseLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*", options.KeyPrefix), options.Log)
	for _, extOption := range extOptions {
		err := extOption(layered)
		if err != nil {
			options.Log.Error(err.Error())
		}
	}
	return layered
}

type CacheOptions struct {
	RDB                *redis.Client
	KVChannel          string
	BigCacheMaxEntries int
	BloomChannel       string
	CuckooChannel      string
	Log                *logrus.Logger
	KvKeyPrefix        string
	BloomKeyPrefix     string
	CuckooKeyPrefix    string
	Strategy           CacheReadStrategy
}

// type CacheBuildOptions func (cache *Cache) error
type Cache struct {
	KV     LayeredKeyValueCache
	Bloom  LayeredFilter
	Cuckoo LayeredCuckooFilter
}

func checkOptions(options *CacheOptions) error {
	if options.RDB == nil {
		return fmt.Errorf("redis client is nil")
	}
	if options.KVChannel == "" {
		return fmt.Errorf("kv channel is empty")
	}
	if options.BloomChannel == "" {
		return fmt.Errorf("bloom channel is empty")
	}
	if options.CuckooChannel == "" {
		return fmt.Errorf("cuckoo channel is empty")

	}
	if options.Log == nil {
		return fmt.Errorf("log is nil")
	}
	if options.KvKeyPrefix == "" {
		return fmt.Errorf("kv key prefix is empty")
	}
	if options.BloomKeyPrefix == "" {
		return fmt.Errorf("bloom key prefix is empty")

	}
	if options.CuckooKeyPrefix == "" {
		return fmt.Errorf("cuckoo key prefix is empty")
	}

	if options.BigCacheMaxEntries <= 0 {
		return fmt.Errorf("bigcache max entries is invalid")

	}
	return nil
}

// func WithKvCache
func New(options *CacheOptions) (*Cache, error) {
	ctx := context.Background()
	err := checkOptions(options)
	if err != nil {
		return nil, err
	}
	kvWatcher := source.NewWatchOptions([]interface{}{options.KVChannel})
	KvOptions := LayeredBuildOptions{
		RDB:       options.RDB,
		Strategy:  options.Strategy,
		Watcher:   kvWatcher,
		Channel:   options.KVChannel,
		Log:       options.Log,
		KeyPrefix: options.KvKeyPrefix,
	}
	kv, err := NewKeyValueCache(ctx, KvOptions, options.BigCacheMaxEntries)
	if err != nil {
		panic(err)

	}
	bloomWatcher := source.NewWatchOptions([]interface{}{options.BloomChannel})
	bloomOptions := &LayeredBuildOptions{
		RDB:       options.RDB,
		Strategy:  options.Strategy,
		Watcher:   bloomWatcher,
		Channel:   options.BloomChannel,
		Log:       options.Log,
		KeyPrefix: options.BloomKeyPrefix,
	}

	cuckooWatcher := source.NewWatchOptions([]interface{}{options.CuckooChannel})
	cuckooOptions := &LayeredBuildOptions{
		RDB:       options.RDB,
		Strategy:  options.Strategy,
		Watcher:   cuckooWatcher,
		Channel:   options.CuckooChannel,
		Log:       options.Log,
		KeyPrefix: options.CuckooKeyPrefix,
	}
	return &Cache{
		KV:     kv,
		Bloom:  NewLayeredBloom(bloomOptions, gobloom.DefaultBuildBloomOptions),
		Cuckoo: NewLayeredCuckoo(cuckooOptions, gocuckoo.DefaultBuildCuckooOptions),
	}, nil
}
