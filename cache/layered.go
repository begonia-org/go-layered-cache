package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Del(ctx context.Context, key string) error
	WatchSource(ctx context.Context) <-chan error
}
type SourceCache interface {
	Get(ctx context.Context, keys ...interface{}) (map[string]interface{}, error)
	Set(ctx context.Context, values map[string]interface{}) error
	Del(ctx context.Context, keys ...interface{}) error
	Scan(ctx context.Context, result chan<- interface{}, keys ...interface{})

	Watch(ctx context.Context, to LocalCache, from ...interface{}) <-chan error
}

type SourceCacheImpl struct {
	// ...
	rdb      *redis.Client
	group    string
	consumer string
	log      *logrus.Logger
	watcherCancel map[string]context.CancelFunc
}
type LayeredCacheImpl struct {
	local    LocalCache
	source   SourceCache
	strategy CacheReadStrategy
	log      *logrus.Logger
	streamsKeys  []interface{}
}
type CacheReadStrategy int

const (
	// Read from the local cache only
	LocalOnly CacheReadStrategy = iota
	// Read from the local cache first, then from the source cache
	LocalThenSource CacheReadStrategy = iota
)

func NewSourceCache(rdb *redis.Client) SourceCache {
	return &SourceCacheImpl{
		rdb: rdb,
		watcherCancel: make(map[string]context.CancelFunc),
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
func (sc *SourceCacheImpl) Set(ctx context.Context, values map[string]interface{}) error {
	// ...
	pipe := sc.rdb.Pipeline()
	for key, value := range values {
		pipe.Set(ctx, key, value, 0)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: sc.getChannelKey(key),
			Values: map[string]interface{}{"key": key, "value": value},
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (sc *SourceCacheImpl) getChannelKey(key string) string {
	return fmt.Sprintf("channel:%s", key)
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
					fullKeys, cur := sc.rdb.Scan(ctx, cur, prefix, 100).Val()
					if len(fullKeys) == 0 || cur == 0 {
						return
					}
					pipe := sc.rdb.Pipeline()
					returns := make([]*redis.StringCmd, 0)
					for _, key := range fullKeys {
						returns = append(returns, pipe.Get(ctx, key))
					}
					_, err := pipe.Exec(ctx)

					if err != nil {
						sc.log.Errorf("Error scanning keys: %v", err)
						result <- err
						continue
					}
					for _, val := range returns {
						result <- val
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
// The from parameter is expected to be a string slice
func (sc *SourceCacheImpl) Watch(ctx context.Context, to LocalCache, from ...interface{}) <-chan error {
	streams := make([]string, 0)
	ids := make([]string, 0)
	for _, key := range from {
		streams = append(streams, sc.getChannelKey(key.(string)))
		ids = append(ids, "0-0")
	}
	streams = append(streams, ids...)
	errChannel := make(chan error)
	defer close(errChannel)
	go func(ctx context.Context, to LocalCache, streams []string, errChannel chan error) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := sc.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    sc.group,
					Consumer: sc.consumer,
					Streams:  streams,
					Block:    0, // 0表示无限期阻塞直到消息到来
					Count:    1, // 每次读取一个消息
				}).Result()
				if err != nil {
					sc.log.Errorf("Error reading from stream: %v", err)
					errChannel <- err
					return
				}
				for _, message := range messages {
					for _, msg := range message.Messages {
						// 确认消息处理
						if key, ok := msg.Values["key"].(string); ok {
							_, err := sc.rdb.XAck(ctx, sc.getChannelKey(key), sc.group, msg.ID).Result()
							if err != nil {
								sc.log.Errorf("Error acknowledging message: %v", err)
								errChannel <- err
							}
							if op, ok := msg.Values["op"].(string); ok && op == "delete" {
								err := to.Del(key)
								if err != nil {
									errChannel <- err
									sc.log.Errorf("Error deleting value: %v", err)
								}
								continue
							}
							// 处理消息
							if value, ok := msg.Values["value"].([]byte); ok && len(value) > 0 {
								err := to.Set(key, value)
								if err != nil {
									errChannel <- err
									sc.log.Errorf("Error setting value: %v", err)
								}
							} else {
								errChannel <- fmt.Errorf("Invalid value type, expected string, got %T", msg.Values["value"])
								sc.log.Errorf("Invalid value type, expected string, got %T", msg.Values["value"])
							}
						}
					}
				}

			}
		}
	}(ctx, to, streams, errChannel)
	return errChannel
}
func NewLayeredCache(local LocalCache, source SourceCache) LayeredCache {
	return &LayeredCacheImpl{
		local:  local,
		source: source,
	}
}

// Get returns the value for the given key
//
// If the key is not found in the local cache and the strategy is LocalThenSource, the source cache is queried
// If the key is found in the source cache, it is set in the local cache
// An error is returned if the underlying redis command fails
// Note: The value is expected to be a []byte
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
// An error is returned if the underlying redis command fails
func (lc *LayeredCacheImpl) Set(ctx context.Context, key string, value []byte) error {
	err := lc.local.Set(key, value)
	if err != nil {
		return err
	}
	err = lc.source.Set(ctx, map[string]interface{}{key: value})
	return err
}

// Del deletes the value for the given key
//
// The value is deleted from the local cache
func (lc *LayeredCacheImpl) Del(ctx context.Context, key string) error {
	err := lc.local.Del(key)
	if err != nil {
		return err
	}
	err = lc.source.Del(ctx, key)
	return err
}
func (lc *LayeredCacheImpl) SetStrategy(strategy CacheReadStrategy) {
	lc.strategy = strategy
}
func (lc *LayeredCacheImpl) GetStrategy() CacheReadStrategy {
	return lc.strategy
}
func (lc *LayeredCacheImpl) WatchSource(ctx context.Context) <-chan error {
	return lc.source.Watch(ctx, lc.local, lc.streamsKeys...)
}

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
			err=lc.local.Set(val.Args()[1].(string), []byte(value))
			if err != nil {
				lc.log.Errorf("Error setting value: %v", err)
			}
			// fmt.Println(val.)
		}
	}

}
