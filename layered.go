package golayeredcache

import (
	"context"
	"fmt"

	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/local"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type CacheReadStrategy int

const (
	// Read from the local cache only
	LocalOnly CacheReadStrategy = iota
	// Read from the local cache first, then from the source cache
	LocalThenSource
)

type WithDelOperator interface {
	Del(ctx context.Context, key interface{}, args ...interface{}) error
}
type Watcher interface {
	Watch(ctx context.Context) <-chan error
	UnWatch() error
}
type Loader interface {
	DumpSourceToLocal(ctx context.Context) error
}
type LayeredCache interface {
	Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key interface{}, args ...interface{}) error
	GetFromLocal(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	SetToLocal(ctx context.Context, key interface{}, args ...interface{}) error

	Watch(ctx context.Context) <-chan error
	UnWatch() error
	OnMessage(ctx context.Context, from string, message interface{}) error
	DumpSourceToLocal(ctx context.Context) error
}
type LayeredKeyValueCache interface {
	// LayeredCache
	Watcher
	Loader
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Del(ctx context.Context, key string) error
}
type LayeredFilter interface {
	Watcher
	Loader
	Check(ctx context.Context, key string, value []byte) (bool, error)
	Add(ctx context.Context, key string, value []byte) error
}
type LayeredCuckooFilter interface {
	LayeredFilter
	Del(ctx context.Context, key string, value []byte) error
}


type LayeredBuildOptions struct {
	RDB     *redis.Client
	Watcher *source.WatchOptions
	Log     *logrus.Logger

	Channel   interface{}
	KeyPrefix string
	Strategy  CacheReadStrategy
}
type LayeredCuckooFilterOptions struct {
	RDB                       *redis.Client
	Watcher                   *source.WatchOptions
	Log                       *logrus.Logger
	Entries                   uint64
	Errors                    float64
	Channel                   interface{}
	KeyPrefix                 string
	Strategy                  CacheReadStrategy
	DefaultBuildCuckooOptions *gocuckoo.CuckooBuildOptions
}

type LayeredCacheImpl struct {
	// 一级缓存
	source source.DataSource
	// 二级缓存
	local local.LocalCache

	log *logrus.Logger

	strategy     CacheReadStrategy
	watchRunning bool
	watchCancel  context.CancelFunc
}

func (lc *LayeredCacheImpl) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	vals, err := lc.local.Get(ctx, key.(string), args...)
	if err != nil || len(vals) == 0 {
		if err != nil {
			lc.log.Errorf("local.Get:%v", err)
		}
		if lc.strategy == LocalOnly {
			return nil, err
		}
		vals, err = lc.source.Get(ctx, key.(string), args...)
		return vals, err

	}
	return vals, err
}
func (lc *LayeredCacheImpl) SetStrategy(strategy CacheReadStrategy) {
	lc.strategy = strategy
}
func (lc *LayeredCacheImpl) SetToLocal(ctx context.Context, key interface{}, args ...interface{}) error {
	return lc.local.Set(ctx, key, args...)
}
func (lc *LayeredCacheImpl) GetFromLocal(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	return lc.local.Get(ctx, key, args...)
}
func (lc *LayeredCacheImpl) DelOnLocal(ctx context.Context, key interface{}, args ...interface{}) error {
	if _, ok := lc.local.(WithDelOperator); !ok {
		return fmt.Errorf("local cache not implement WithDelOperator")

	}
	return lc.local.(WithDelOperator).Del(ctx, key, args...)
}
func (lc *LayeredCacheImpl) Set(ctx context.Context, key interface{}, args ...interface{}) error {
	err := lc.local.Set(ctx, key.(string), args...)
	if err != nil {
		lc.log.Errorf("local.Set:%v", err)
		return err
	}
	err = lc.source.Set(ctx, key.(string), args...)
	if err != nil {
		lc.log.Errorf("source.Set:%v", err)
		return err
	}
	return err
}

func (lc *LayeredCacheImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {
	if _, ok := lc.local.(WithDelOperator); !ok {
		return fmt.Errorf("local cache not implement WithDelOperator")
	}
	err := lc.local.(WithDelOperator).Del(ctx, key.(string), args...)
	if err != nil {
		lc.log.Error("local.Del", err)
	}
	err = lc.source.(WithDelOperator).Del(ctx, key.(string), args...)
	return err
}

func (lc *LayeredCacheImpl) Watch(ctx context.Context, onMessage source.OnMessage) <-chan error {
	if lc.watchRunning {
		lc.log.Warning("watch is running,if you want to watch again,please call UnWatch first")
		return nil
	}
	ch, cancel := lc.source.Watch(ctx, onMessage)
	lc.watchCancel = cancel
	lc.watchRunning = true
	return ch
}

func (lc *LayeredCacheImpl) UnWatch() error {
	if lc.watchRunning {
		lc.watchCancel()
		lc.watchRunning = false
	}
	return nil
}

func (lc *LayeredCacheImpl) Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{} {
	return lc.source.Dump(ctx, key.(string), args...)
}

func (lc *LayeredCacheImpl) Load(ctx context.Context, key interface{}, args ...interface{}) error {
	return lc.local.Load(ctx, key.(string), args...)
}

func (lc *LayeredCacheImpl) Scan(ctx context.Context, pattern string, onScan source.OnScan) <-chan error {
	return lc.source.Scan(ctx, pattern, onScan)
}

func NewLayeredCacheImpl(source source.DataSource, local local.LocalCache, log *logrus.Logger, strategy CacheReadStrategy) *LayeredCacheImpl {
	return &LayeredCacheImpl{
		source:   source,
		local:    local,
		log:      log,
		strategy: strategy,
	}
}
