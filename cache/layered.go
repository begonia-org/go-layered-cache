package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Del(ctx context.Context, key string) error
	WatchSource(ctx context.Context) <-chan error
	LoadSourceCache(ctx context.Context, prefixKeys ...interface{})
	UnWatchSource(ctx context.Context) error
}
type SourceCache interface {
	Get(ctx context.Context, keys ...interface{}) (map[string]interface{}, error)
	Set(ctx context.Context, key string, values interface{}, expiration time.Duration) error
	Del(ctx context.Context, keys ...interface{}) error
	Scan(ctx context.Context, result chan<- interface{}, keys ...interface{})

	Watch(ctx context.Context, to LocalCache) <-chan error
	UnWatch(ctx context.Context) error
}
// SourceCacheImpl is a source cache that uses redis as the backend
// It implements the SourceCache interface
// It uses a redis stream to watch for changes and updates the local cache
// It only supports the Get, Set, Del, and Scan operations for keys
type SourceCacheImpl struct {
	rdb       *redis.Client
	log       *logrus.Logger
	onceWatch sync.Once
	// stream is the name of the stream to watch for changes
	stream    string
	mux       sync.Mutex
	// errChan is the channel to send errors to
	// The messages are sent by the watch worker goroutine
	errChan   chan error
	// watchCancel is the function to cancel the watch worker goroutine
	watchCancel context.CancelFunc
	// readStreamBlock is the time to block when reading from the source cache
	readStreamBlock time.Duration
	// readMessageSize is the number of messages to read from the source cache
	readMessageSize int
}
type LayeredCacheImpl struct {
	local     LocalCache
	source    SourceCache
	strategy  CacheReadStrategy
	log       *logrus.Logger
	onceWatch sync.Once
	mux       sync.RWMutex
	errChan   chan error
}
type CacheReadStrategy int

const (
	// Read from the local cache only
	LocalOnly CacheReadStrategy = iota
	// Read from the local cache first, then from the source cache
	LocalThenSource
)

type LayeredCacheConfig struct {
	RDB *redis.Client
	Log *logrus.Logger
	// WatchKey string is the key to watch for changes in the source cache
	WatchKey string
	// ReadStrategy CacheReadStrategy is the strategy to use when reading from the cache
	// The default is LocalOnly
	ReadStrategy CacheReadStrategy
	// The time to block when reading from the source cache
	// It is only used when the source cache is a redis stream
	// See https://redis.io/commands/xread
	ReadStreamBlock time.Duration
	ReadMessageSize int
	bigcache.Config
}

func NewSourceCache(rdb *redis.Client, log *logrus.Logger, stream string,readStreamBlock time.Duration,readMessageSize int) SourceCache {
	return &SourceCacheImpl{
		rdb: rdb,

		log:       log,
		onceWatch: sync.Once{},
		stream:    stream,
		mux:       sync.Mutex{},
		errChan:   make(chan error, 100),
		readStreamBlock: readStreamBlock,
		readMessageSize: readMessageSize,
	}
}

// Get returns the values for the given keys
//
// If a key is not found, it is not returned in the result map
// An error is returned if the underlying redis command fails
// The keys are expected to be strings
func (sc *SourceCacheImpl) Get(ctx context.Context, keys ...interface{}) (map[string]interface{}, error) {
	// ...
	pipe := sc.rdb.Pipeline()
	for _, key := range keys {
		pipe.Get(ctx, key.(string))
	}
	cmders, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	values := make(map[string]interface{})
	for i, key := range keys {
		values[key.(string)], _ = cmders[i].(*redis.StringCmd).Result()

	}
	return values, nil

}

// Del deletes the values for the given keys and publishes the delete operation to the channel
//
// An error is returned if the underlying redis command fails
func (sc *SourceCacheImpl) Del(ctx context.Context, keys ...interface{}) error {
	// ...
	pipe := sc.rdb.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key.(string))
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: sc.getChannelKey(key.(string)),
			Values: map[string]interface{}{"key": key.(string), "op": "delete"},
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Set sets the value for the given key and publishes the set operation to the channel
//
// An error is returned if the underlying redis command fails
func (sc *SourceCacheImpl) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	pipe := sc.rdb.Pipeline()
	pipe.Set(ctx, key, value, expiration)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: sc.getChannelKey(sc.stream),
		Values: map[string]interface{}{"key": key, "value": value},
	})
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("Error setting value: %w", err)
	}
	return nil
}

func (sc *SourceCacheImpl) getChannelKey(key string) string {
	return fmt.Sprintf("%s:channel", key)
}

// Scan scans the keys with the given prefix and sends the results to the result channel
//
// An error is sent to the result channel if the underlying redis command fails
// The keys are expected to be strings and they are used as the prefix key for the scan
// The result is a *redis.StringCmd of key's value or an error
func (sc *SourceCacheImpl) Scan(ctx context.Context, result chan<- interface{}, keys ...interface{}) {
	wg := &sync.WaitGroup{}

	for _, key := range keys {
		wg.Add(1)
		go func(ctx context.Context, prefix string, wg *sync.WaitGroup) {
			var cur uint64
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					fullKeys, cur, err := sc.rdb.Scan(ctx, cur, prefix, 100).Result()
					if len(fullKeys) == 0 {
						return
					}
					if err != nil {
						sc.log.Errorf("Error scanning keys: %v", err)
						result <- err
						return
					}
					pipe := sc.rdb.Pipeline()
					returns := make([]*redis.StringCmd, 0)
					for _, key := range fullKeys {
						returns = append(returns, pipe.Get(ctx, key))
					}
					_, err = pipe.Exec(ctx)

					if err != nil {
						sc.log.Errorf("Error scanning keys: %v", err)
						result <- err
						continue
					}
					for _, val := range returns {
						result <- val
					}
					if cur == 0 {
						return
					}

				}
			}

		}(ctx, key.(string), wg)
	}
	wg.Wait()
}

// Watch watches the given streams and updates the local cache with the values
//
// An error is returned if the underlying redis command fails
// The worker goroutine listens for messages from the streams and
// updates the local cache and it is started only once.
func (sc *SourceCacheImpl) Watch(ctx context.Context, to LocalCache) <-chan error {
	sc.onceWatch.Do(func() {
		watchCtx, cancel := context.WithCancel(ctx)
		sc.watchCancel = cancel
		go func(ctx context.Context, to LocalCache) {
			defer close(sc.errChan)
			defer func() {
				sc.log.Info("watcher done")
			}()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// 更新频道
					if sc.stream == "" {
						continue
					}
					streams := []string{sc.getChannelKey(sc.stream), "0"}
					messages, err := sc.rdb.XRead(ctx, &redis.XReadArgs{
						Streams: streams,
						Block:   sc.readStreamBlock,
						Count:   int64(sc.readMessageSize),
					}).Result()
					if err != nil {
						sc.log.Errorf("Error reading from stream: %v", err)
						sc.errChan <- fmt.Errorf("Error reading from stream: %w", err)
						return
					}
					for _, message := range messages {
						for _, msg := range message.Messages {
							// 确认消息处理
							if key, ok := msg.Values["key"].(string); ok {

								if op, ok := msg.Values["op"].(string); ok && op == "delete" {
									err := to.Del(key)
									if err != nil {
										sc.errChan <- err
										sc.log.Errorf("Error deleting value: %v", err)
									}
									continue
								}
								// 处理消息
								if value, ok := msg.Values["value"].(string); ok && len(value) > 0 {
									err := to.Set(key, []byte(value))
									if err != nil {
										sc.errChan <- err
										sc.log.Errorf("Error setting value: %v", err)
									}
								} else {
									sc.errChan <- fmt.Errorf("Invalid value type, expected string, got %T", msg.Values["value"])
									sc.log.Errorf("Invalid value type, expected string, got %T", msg.Values["value"])
								}
							}
						}
					}

				}
			}
		}(watchCtx, to)
	})

	return sc.errChan
}

// UnWatch stops watching the given stream
func (sc *SourceCacheImpl) UnWatch(ctx context.Context) error {
	if sc.watchCancel != nil {
		sc.watchCancel()
	}
	return nil

}
func NewLayeredCache(local LocalCache, source SourceCache, log *logrus.Logger, strategy CacheReadStrategy) *LayeredCacheImpl {
	return &LayeredCacheImpl{
		local:     local,
		source:    source,
		onceWatch: sync.Once{},
		mux:       sync.RWMutex{},
		log:       log,
		strategy:  strategy,
		errChan:   make(chan error, 100),
	}
}

// Get returns the value for the given key
//
// If the key is not found in the local cache and the strategy is LocalThenSource, the source cache is queried
// If the key is found in the source cache, it is set in the local cache
// An error is returned if the underlying redis command fails
// Note: The value is expected to be a []byte
//
// Parameters:
//  - ctx: the context
//  - key: the key to get
// Returns:
//  - []byte: the value for the given key
//  - error: an error if the underlying handle fails
func (lc *LayeredCacheImpl) Get(ctx context.Context, key string) ([]byte, error) {

	val, err := lc.local.Get(key)
	if (err != nil || len(val) == 0) && lc.strategy == LocalThenSource {
		keys := []interface{}{key}
		vals, err := lc.source.Get(ctx, keys)
		if err != nil {
			return nil, err
		}
		if len(vals) > 0 {
			source := vals[key]
			switch source := source.(type) {
			case []byte:
				val = source
			default:
				return nil, fmt.Errorf("Get invalid value type, expected string or []byte, got %T", source)
			}
			err = lc.local.Set(key, val)
			if err != nil {
				return nil, err
			}
		}
	}
	return val, err
}

// Set sets the value for the given key
//
// The value is set in the local cache and the source cache
// An error is returned if the underlying redis command or local cache set fails
// NOTE: Expiration is not supported in the local cache which is implement by bigcache, 
// it is only set in the source cache
//
// Parameters:
//  - ctx: the context
//  - key: the key to set
//  - value: the value to set
//  - expiration: the expiration duration(ignored in the local cache)
// Returns:
//  - error: an error if the underlying handle fails
func (lc *LayeredCacheImpl) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	err := lc.local.Set(key, value)
	if err != nil {
		return err
	}
	err = lc.source.Set(ctx, key, value, expiration)
	return err
}

// Del deletes the value for the given key
//
// The value is deleted from the local cache and from the source cache
// An error is returned if the underlying redis command or local cache delete fails
//
// Parameters:
//  - ctx: the context
// - key: the key to delete
// Returns:
//  - error: an error if the underlying handle fails
func (lc *LayeredCacheImpl) Del(ctx context.Context, key string) error {
	err := lc.local.Del(key)
	if err != nil {
		return err
	}
	err = lc.source.Del(ctx, key)
	return err
}
// SetStrategy sets the read strategy for the cache
// The default is LocalOnly,it is meant to only read from the local cache even if the key is not found.
// The LocalThenSource strategy is meant to read from the local cache first, then from the source cache if the key is not found,
// and set the value in the local cache if it is found in the source cache.
//
// Parameters:
//  - strategy: the read strategy
func (lc *LayeredCacheImpl) SetStrategy(strategy CacheReadStrategy) {
	lc.strategy = strategy
}
func (lc *LayeredCacheImpl) GetStrategy() CacheReadStrategy {
	return lc.strategy
}

// UnWatch stops watching the given streams keys
func (lc *LayeredCacheImpl) UnWatchSource(ctx context.Context) error {
	lc.mux.Lock()
	defer lc.mux.Unlock()
	return lc.source.UnWatch(ctx)

}

// WatchSource starts watching the source cache for changes
//
// The worker goroutine listens for messages from the streams and
// updates the local cache and it is started only once.
func (lc *LayeredCacheImpl) WatchSource(ctx context.Context) <-chan error {
	lc.onceWatch.Do(func() {

		ch := lc.source.Watch(ctx, lc.local)
		go func() {
			for err := range ch {
				lc.errChan <- err
			}

		}()
	})
	return lc.errChan
}

// LoadSourceCache loads the source cache into the local cache
//
// The worker goroutine scans the source cache and sets the values in the local cache
// prefixKeys are the keys to use as the prefix for the scan
//
// Parameters:
//  - ctx: the context
//  - prefixKeys: the keys to use as the redis keys prefix for the scan
func (lc *LayeredCacheImpl) LoadSourceCache(ctx context.Context, prefixKeys ...interface{}) {
	result := make(chan interface{}, 100)

	go func() {
		defer close(result)
		lc.source.Scan(ctx, result, prefixKeys...)

	}()
	for val := range result {
		switch val := val.(type) {
		case error:
			lc.log.Errorf("Error scanning keys: %v", val)
		case *redis.StringCmd:
			value, err := val.Result()
			if err != nil {
				lc.log.Errorf("Error getting value: %v", err)
			}
			key := val.Args()[1].(string)
			err = lc.local.Set(key, []byte(value))
			if err != nil {
				lc.log.Errorf("Error setting value: %v", err)
			}
		default:
			lc.log.Errorf("Invalid value type, expected *redis.StringCmd, got %T", val)
		}
	}

}
