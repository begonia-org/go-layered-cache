package gocuckoo

import (
	"fmt"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCuckooFilterOptions struct {
	RDB                      *redis.Client
	Watcher                  *golayeredcache.WatchOptions
	Log                      *logrus.Logger
	Entries                  uint64
	Errors                   float64
	Channel                  interface{}
	KeyPrefix                string
	Strategy                 golayeredcache.CacheReadStrategy
	DefaultBuildCuckooOptions *CuckooBuildOptions
}

func New(options *golayeredcache.LayeredBuildOptions,buildCuckooOptions *CuckooBuildOptions) *LayeredCuckooFilter {

	redSource := golayeredcache.NewDataSourceFromRedis(options.RDB, options.Watcher)

	source := NewCuckooSourceImpl(redSource, options.Channel.(string), options.Log)
	local := NewLocalCuckooFilters(make(map[string]*GoCuckooFilterImpl), buildCuckooOptions)
	layered := NewLayeredCuckooFilter(golayeredcache.NewLayeredCacheImpl(source, local, options.Log, options.Strategy), fmt.Sprintf("%s:*", options.KeyPrefix), options.Log)
	return layered
}
