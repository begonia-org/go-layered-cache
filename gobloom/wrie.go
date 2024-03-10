package gobloom

import (
	"fmt"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredBloomFilterOptions struct {
	RDB     *redis.Client
	Watcher *golayeredcache.WatchOptions
	Log     *logrus.Logger
	Entries uint64
	Errors  float64
	Channel interface{}
	KeyPrefix string
	Strategy golayeredcache.CacheReadStrategy
	DefaultBuildBloomOptions *BloomBuildOptions
}

func New(options *golayeredcache.LayeredBuildOptions,bloomOptions *BloomBuildOptions) *LayeredBloomFilter {

	redSource := golayeredcache.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := NewBloomSourceImpl(redSource, options.Channel.(string), options.Log)
	local := NewLocalBloomFilters(make(map[string]*GoBloomChain), bloomOptions)
	layered := NewLayeredBloomFilter(golayeredcache.NewLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*",options.KeyPrefix), options.Log)
	return layered
}
