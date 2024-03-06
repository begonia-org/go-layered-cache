package gocuckoo

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var ProvideSet = wire.NewSet(NewSourceCuckooFilter, NewLayeredCuckooFilter, NewGoCuckooFilter)

type LayeredCuckooFilterConfig struct {
	// Filters is a map of cuckoo filters to use in the layered cuckoo filter.
	// The key is the name of the filter and the value is the local cuckoo filter.
	Filters                map[string]GoCuckooFilter
	Rdb                    *redis.Client
	Log                    *logrus.Logger
	// ReadStreamMessageBlock is the time to block when reading from the redis stream.
	// See https://pkg.go.dev/github.com/redis/go-redis/v9#Client.XRead for more information.
	// See https://redis.io/commands/xread for more information.
	ReadStreamMessageBlock time.Duration
	// ReadStreamMessageSize is the number of messages to read from the redis stream.
	ReadStreamMessageSize  int
	// WatchKey is the key to use to watch the redis stream.
	WatchKey               string
}
type LayeredCuckooFilter interface {
	LoadFromSource(ctx context.Context) error
	Insert(ctx context.Context, kv ...interface{}) error
	Check(ctx context.Context, source interface{}, data []byte) bool
	Delete(context.Context, ...interface{}) error

	AddFilter(key string, filter GoCuckooFilter) error
	WatchSource(ctx context.Context, errChannel chan<- error)
}
type SourceCuckooFilter interface {
	Get(cxt context.Context, source ...interface{}) <-chan interface{}
	Set(cxt context.Context, kv ...interface{}) error
	Del(ctx context.Context, kv ...interface{}) error

	Watch(ctx context.Context, local map[string]GoCuckooFilter, errChan chan<- error)
}

type LayeredCuckooFilterImpl struct {
	localCuckooFilter map[string]GoCuckooFilter
	source            SourceCuckooFilter
	log               *logrus.Logger

	mux sync.RWMutex
}
type SourceCuckooFilterImpl struct {
	rdb                    *redis.Client
	log                    *logrus.Logger
	streamKey              string
	readStreamMessageBlock time.Duration
	readStreamMessageSize  int
}
type pubsubParams struct {
	// group    string
	// consumer string
	channel string
	message interface{}
}

func NewSourceCuckooFilter(rdb *redis.Client, log *logrus.Logger, streamKey string, readBlock time.Duration, readMessageSize int) SourceCuckooFilter {
	return &SourceCuckooFilterImpl{
		rdb:                    rdb,
		log:                    log,
		streamKey:              streamKey,
		readStreamMessageBlock: readBlock,
		readStreamMessageSize:  readMessageSize,
	}
}

// Get gets the data from the source stream.
//
// The sources parameter is a list of redis cuckoo filter keys to get the data from.
// It returns a channel that will receive the data which is a *RedisDump from the source cuckoo filter.
func (scf *SourceCuckooFilterImpl) Get(ctx context.Context, sources ...interface{}) <-chan interface{} {
	cur := 0
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		wg := &sync.WaitGroup{}
		for _, key := range sources {
			k := key.(string)
			wg.Add(1)
			go func(ctx context.Context, key string, wg *sync.WaitGroup) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						ch <- fmt.Errorf("recover:%v", r)
					}
				}()
				for {
					select {
					case <-ctx.Done():
						ch <- fmt.Errorf("context done,%w", ctx.Err())
						return
					default:
						val, err := scf.rdb.CFScanDump(ctx, key, int64(cur)).Result()
						if err != nil && err != redis.Nil {
							ch <- err
							return
						}
						if val.Iter == 0 {
							return
						}
						ch <- &RedisDump{Data: val.Data, Iter: uint64(val.Iter)}
						cur = int(val.Iter)
					}

				}
			}(ctx, k, wg)
			wg.Wait()
		}
	}()

	return ch
}

// Set adds key-value pairs to a Cuckoo filter and an associated stream within a transactional pipeline.
// This function accepts a variable number of key-value pairs as arguments, where keys and values are expected to be strings.
// It performs two main actions for each key-value pair:
//  1. Adds the pair to a Cuckoo filter using CFAdd.
//  2. Adds the pair to a stream with additional metadata using XAdd.
//
// The function enforces that keys must be strings and the number of arguments must be even (to form key-value pairs).
// It uses a Redis pipeline to ensure that all operations are executed atomically.
//
// Parameters:
//   - ctx: The context to control cancellation and deadlines.
//   - kv: A variadic list of key-value pairs, where each key is immediately followed by its value.
//
// Returns:
//   - An error if the arguments are invalid (odd number of arguments or non-string keys/values),
//     or if the Redis pipeline execution fails.
//
// Example:
//
//	err := scf.Set(context.Background(), "key1", "value1", "key2", "value2")
//	if err != nil {
//	    log.Fatalf("Failed to set key-value pairs: %v", err)
//	}
func (scf *SourceCuckooFilterImpl) Set(ctx context.Context, kv ...interface{}) error {
	if len(kv)%2 != 0 {
		return fmt.Errorf("invalid params,key-value pairs should be even")
	}

	pipeline := scf.rdb.TxPipeline()
	for i := 0; i < len(kv); i = i + 2 {
		key, ok := kv[i].(string)
		if !ok {
			return fmt.Errorf("invalid key type, it should be string")
		}
		data, ok := kv[i+1].(string)
		if !ok {
			return fmt.Errorf("invalid data type, it should be string")
		}
		pipeline.CFAdd(ctx, key, data)
		pipeline.XAdd(ctx, &redis.XAddArgs{
			Stream: scf.getChannelKey(scf.streamKey),
			Values: map[string]interface{}{"key": key, "data": data},
		})
	}
	_, err := pipeline.Exec(ctx)
	return err
}

// Del deletes the specified keys.
//
// The kv parameter is a list of key-value pairs to delete.
// The keys are expected to be strings and they are used as the key for the delete command.
// The values are expected to be the data to delete.
// It will delete the data from the cuckoo filter and also add a delete message to the stream .
// An error is returned if the underlying redis command fails.
func (scf *SourceCuckooFilterImpl) Del(ctx context.Context, kv ...interface{}) error {
	if len(kv)%2 != 0 {
		return fmt.Errorf("invalid params,key-value pairs should be even")
	}
	pipeline := scf.rdb.TxPipeline()
	for i := 0; i < len(kv); i = i + 2 {
		k, ok := kv[i].(string)
		if !ok {
			return fmt.Errorf("invalid key type, it should be string")
		}
		pipeline.CFDel(ctx, k, kv[i+1])
		pipeline.XAdd(ctx, &redis.XAddArgs{
			Stream: k,
			Values: map[string]interface{}{"op": "delete", "data": kv[i+1]},
		})
	}

	_, err := pipeline.Exec(ctx)
	return err
}

func (scf *SourceCuckooFilterImpl) getChannelKey(key string) string {
	return fmt.Sprintf("%s:channel", key)
}

// Watch watches the stream for new messages and processes them.
//
// The local parameter is the local memory cuckoo filter to update.
// The message 'operate' is used to determine if the message is a delete operation.
// It returns a channel that will receive any errors that occur while processing the stream.
// The function will block until the context is done.
func (scf *SourceCuckooFilterImpl) Watch(ctx context.Context, local map[string]GoCuckooFilter, errChan chan<- error) {
	chanKey := scf.getChannelKey(scf.streamKey)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			messages, err := scf.rdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{chanKey, "0"},
				Block:   scf.readStreamMessageBlock,
				Count:   int64(scf.readStreamMessageSize),
			}).Result()
			if err != nil {
				scf.log.Errorf("Error reading from stream: %v", err)
				errChan <- err
				continue
			}
			// 处理收到的消息
			for _, message := range messages {
				for _, msg := range message.Messages {

					if data, ok := msg.Values["data"].(string); ok {
						key, ok := msg.Values["key"].(string)
						if !ok {
							errChan <- fmt.Errorf("invalid key, it should be string")
							continue
						}
						if op, ok := msg.Values["op"]; ok && op == "delete" {
							ok = local[key].Delete([]byte(data))
							if !ok {
								errChan <- fmt.Errorf("delete failed")
								scf.log.Errorf("Error processing message: %v", err)
							}
							continue
						}
						// if need lock?
						err = local[key].Insert([]byte(data))
						if err != nil {
							errChan <- err
							scf.log.Errorf("Error processing message: %v", err)
						}
					} else {
						errChan <- fmt.Errorf("invalid data type,data expect string but got %T", msg.Values["data"])
					}

				}
			}

		}
		// 释放CPU
		runtime.Gosched()

	}
}
func NewLayeredCuckooFilter(local map[string]GoCuckooFilter, source SourceCuckooFilter, log *logrus.Logger) LayeredCuckooFilter {
	return &LayeredCuckooFilterImpl{
		localCuckooFilter: local,
		source:            source,
		log:               log,
		mux:               sync.RWMutex{},
	}
}

// WatchSource watches the source stream for new messages and processes them.
//
// The source stream which is a redis stream using the localCuckooFilter keys as the stream names.
// It will block until the context is done.
// The errChannel parameter is a channel that will receive any errors that occur while processing the stream.
func (lc *LayeredCuckooFilterImpl) WatchSource(ctx context.Context, errChannel chan<- error) {
	wg := &sync.WaitGroup{}
	defer close(errChannel)
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		lc.source.Watch(ctx, lc.localCuckooFilter, errChannel)

	}(ctx, wg)
	wg.Wait()

}

// AddFilter adds a new local cuckoo filter to the layered cuckoo filter.
//
// Parameters:
//   - key: The key to use to add the filter.If the key already exists, an error is returned.
//   - filter: The cuckoo filter to add.
//
// Returns:
//   - An error if the filter already exists.
func (lc *LayeredCuckooFilterImpl) AddFilter(key string, filter GoCuckooFilter) error {
	lc.mux.Lock()
	defer lc.mux.Unlock()
	if _, ok := lc.localCuckooFilter[key]; ok {
		return fmt.Errorf("filter already exists")
	}
	lc.localCuckooFilter[key] = filter
	return nil
}

// LoadFromSource loads the data from the redis Cuckoo filter into the local memory cuckoo filter.
//
// Parameters:
//   - ctx: The context to control cancellation and deadlines.
//
// Returns:
//   - An error if the data cannot be loaded from the source stream.
//
// NOTE:It will reinitialize the local memory cuckoo filter with the data from the source stream.
// The local memory cuckoo filter initial configuration which is from redis will be used to reinitialize the filter.
func (lc *LayeredCuckooFilterImpl) LoadFromSource(ctx context.Context) error {

	err := lc.initFromSource(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Insert inserts the data into the local memory cuckoo filter and also adds the data to the redis.
// This function accepts a variable number of key-value pairs as arguments, where keys are expected to be strings and
// values are expected to be []byte.
//
// It performs two main actions for each key-value pair:
//  1. Adds the pair to a Cuckoo filter using CFAdd.
//
// 2. Adds the pair to a local memory Cuckoo filter.
//
// The function enforces that keys must be strings and the number of arguments must be even (to form key-value pairs).
//
// Parameters:
//   - ctx: The context to control cancellation and deadlines.
//   - kv: A variadic list of key-value pairs, where each key is immediately followed by its value.
//     The Key is string and the value is []byte.
//
// Returns:
//   - An error if the arguments are invalid (odd number of arguments or non-string keys or not []byte values)
//
// Example:
//
//	err := lc.Insert(context.Background(), "key1", []byte("key1"), "key2", []byte("key2"))
func (lc *LayeredCuckooFilterImpl) Insert(ctx context.Context, kv ...interface{}) error {

	newKv := make([]interface{}, 0)
	err := lc.kvHandler(ctx, func(ctx context.Context, key string, data []byte) error {
		local := lc.localCuckooFilter[key]
		err := local.Insert(data)
		if err != nil {
			return err
		}
		newKv = append(newKv, key, string(data))
		return nil
	}, kv...)
	if err != nil {
		return err
	}
	err = lc.source.Set(ctx, newKv...)
	if err != nil {
		return err
	}
	return nil
}

// Check checks if the data is in the local memory cuckoo filter.
//
// Parameters:
//   - ctx: The context to control cancellation and deadlines.
//   - key: The key to use to check the data.
//   - data: The data to check.
//
// Returns:
//   - A boolean value indicating if the data is in the local memory cuckoo filter.
func (lc *LayeredCuckooFilterImpl) Check(ctx context.Context, key interface{}, data []byte) bool {
	lc.mux.RLock()
	defer lc.mux.RUnlock()
	relKey := key.(string)
	return lc.localCuckooFilter[relKey].Check(data)
}
func (lc *LayeredCuckooFilterImpl) kvHandler(ctx context.Context, handle func(context.Context, string, []byte) error, kv ...interface{}) error {
	if len(kv)%2 != 0 {
		return fmt.Errorf("invalid params,key-value pairs should be even")
	}
	lc.mux.Lock()
	defer lc.mux.Unlock()
	for i := 0; i < len(kv); i = i + 2 {
		key, ok := kv[i].(string)
		if !ok {
			return fmt.Errorf("invalid source,it should be string")
		}
		local, ok := lc.localCuckooFilter[key]
		if !ok || local == nil {
			return fmt.Errorf("invalid key,no such key")
		}
		data, ok := kv[i+1].([]byte)
		if !ok {
			return fmt.Errorf("invalid data,expecting []byte but got %T", kv[i+1])
		}

		err := handle(ctx, key, data)
		if err != nil {
			return err
		}

	}

	return nil
}

// Delete deletes the data from the local memory cuckoo filter and also adds the delete message to the redis.
//
// Parameters:
//   - ctx: The context to control cancellation and deadlines.
//   - kv: A variadic list of key-value pairs, where each key is immediately followed by its value.
//     The Key is string and the value is []byte.
//
// Returns:
//   - An error if the arguments are invalid (odd number of arguments or non-string keys or not []byte values)
func (lc *LayeredCuckooFilterImpl) Delete(ctx context.Context, kv ...interface{}) error {
	newKv := make([]interface{}, 0)
	err := lc.kvHandler(ctx, func(ctx context.Context, key string, data []byte) error {
		local := lc.localCuckooFilter[key]
		ok := local.Delete(data)
		if !ok {
			return fmt.Errorf("delete failed")
		}
		newKv = append(newKv, key, string(data))
		return nil
	}, kv...)
	if err != nil {
		return err
	}
	return lc.source.Del(ctx, newKv...)
}

// initFromSource initializes the local memory cuckoo filter with the data from the source stream.
func (lc *LayeredCuckooFilterImpl) initFromSource(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	for key, local := range lc.localCuckooFilter {
		wg.Add(1)
		go func(ctx context.Context, key string, localCuckooFilter GoCuckooFilter, wg *sync.WaitGroup) {
			sourceData := lc.source.Get(ctx, key)
			defer wg.Done()
			for data := range sourceData {
				if err, ok := data.(error); ok {
					lc.log.Errorf("Error getting source data: %v", err)
					return
				}
				if dump, ok := data.(*RedisDump); ok {
					err := localCuckooFilter.LoadFrom(dump)
					if err != nil {
						lc.log.Errorf("Error loading data from source: %v", err)
						return
					}
				}
			}
		}(ctx, key, local, wg)
	}
	wg.Wait()

	return nil

}
