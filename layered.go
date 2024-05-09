// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"context"
	"fmt"
	"time"

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
	LoadDump(ctx context.Context) error
}

// LayeredLocalCache is the interface for the local cache
type LayeredLocalCache interface{
	GetFromLocal(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	SetToLocal(ctx context.Context, key interface{}, args ...interface{}) error
}
// LayeredCache is the interface for the layered cache
type LayeredCache interface {
	LayeredLocalCache

	// Get key value
	Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key interface{}, args ...interface{}) error


	Watch(ctx context.Context) <-chan error
	UnWatch() error
	// OnMessage is a callback function for the message from the source cache
	OnMessage(ctx context.Context, from string, message interface{}) error
	LoadDump(ctx context.Context) error
}
type LayeredKeyValueCache interface {
	// LayeredCache
	LayeredLocalCache
	Watcher
	Loader
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, exp time.Duration) error
	Del(ctx context.Context, key string) error
}
type LayeredFilter interface {
	Watcher
	Loader
	Check(ctx context.Context, key string, value []byte) (bool, error)
	Add(ctx context.Context, key string, value []byte) error
	AddLocalFilter(key string, filter local.Filter) error
}
type LayeredCuckooFilter interface {
	LayeredFilter
	Del(ctx context.Context, key string, value []byte) error
}

// LayeredBuildOptions is the options for building the layered cache
type LayeredBuildOptions struct {
	// RDB is the redis client as the source cache
	RDB *redis.Client
	// Watcher is the watch options for the source cache
	Watcher *source.WatchOptions
	// Log is the logger
	Log *logrus.Logger
	// Channel is the channel for the source cache
	// It is used to receive the message from the source cache
	// It is also used to publish the message to the source cache
	// In the case of the source cache is redis, it is the redis xstream channel
	Channel interface{}
	// KeyPrefix is the key prefix for cache,like "cache:test:bloom"
	KeyPrefix string

	// Strategy is the read strategy
	// It is used to determine the read strategy when the cache is read.
	// For Read from the local cache only, it is LocalOnly.
	// For Read from the local cache first, then from the source cache when the local cache is none, it is LocalThenSource
	Strategy CacheReadStrategy
}

// LayeredBloomFilterOptions is the options for building the layered bloom filter
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

type BaseLayeredCacheImpl struct {
	// 一级缓存
	source source.DataSource
	// 二级缓存
	local local.LocalCache

	log *logrus.Logger

	strategy     CacheReadStrategy
	watchRunning bool
	watchCancel  context.CancelFunc
}

func (lc *BaseLayeredCacheImpl) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	vals, err := lc.local.Get(ctx, key.(string), args...)
	if err != nil || len(vals) == 0 {
		if err != nil {
			lc.log.Errorf("local.Get %v error:%v",key, err)
		}
		if lc.strategy == LocalOnly {
			return nil, err
		}
		vals, err = lc.source.Get(ctx, key.(string), args...)
		if err != nil {
			return nil, err
		}
		if len(vals) > 0 {
			err = lc.local.Set(ctx, key.(string), vals...)
			if err != nil {
				lc.log.Errorf("local.Set:%v", err)
			}
		}
		return vals, err

	}
	return vals, err
}
func (lc *BaseLayeredCacheImpl) SetStrategy(strategy CacheReadStrategy) {
	lc.strategy = strategy
}
func (lc *BaseLayeredCacheImpl) SetToLocal(ctx context.Context, key interface{}, args ...interface{}) error {
	return lc.local.Set(ctx, key, args...)
}
func (lc *BaseLayeredCacheImpl) GetFromLocal(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	return lc.local.Get(ctx, key, args...)
}
func (lc *BaseLayeredCacheImpl) DelOnLocal(ctx context.Context, key interface{}, args ...interface{}) error {
	if _, ok := lc.local.(WithDelOperator); !ok {
		return fmt.Errorf("local cache not implement WithDelOperator")

	}
	return lc.local.(WithDelOperator).Del(ctx, key, args...)
}
func (lc *BaseLayeredCacheImpl) Set(ctx context.Context, key interface{}, args ...interface{}) error {
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

func (lc *BaseLayeredCacheImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {
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

func (lc *BaseLayeredCacheImpl) Watch(ctx context.Context, onMessage source.OnMessage) <-chan error {
	if lc.watchRunning {
		lc.log.Warning("watch is running,if you want to watch again,please call UnWatch first")
		return nil
	}
	ch, cancel := lc.source.Watch(ctx, onMessage)
	lc.watchCancel = cancel
	lc.watchRunning = true
	return ch
}

func (lc *BaseLayeredCacheImpl) UnWatch() error {
	if lc.watchRunning {
		lc.watchCancel()
		lc.watchRunning = false
	}
	return nil
}

func (lc *BaseLayeredCacheImpl) Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{} {
	return lc.source.Dump(ctx, key.(string), args...)
}

func (lc *BaseLayeredCacheImpl) Load(ctx context.Context, key interface{}, args ...interface{}) error {
	return lc.local.Load(ctx, key.(string), args...)
}

func (lc *BaseLayeredCacheImpl) Scan(ctx context.Context, pattern string, onScan source.OnScan) <-chan error {
	return lc.source.Scan(ctx, pattern, onScan)
}

func newBaseLayeredCacheImpl(source source.DataSource, local local.LocalCache, log *logrus.Logger, strategy CacheReadStrategy) *BaseLayeredCacheImpl {
	return &BaseLayeredCacheImpl{
		source:   source,
		local:    local,
		log:      log,
		strategy: strategy,
	}
}
