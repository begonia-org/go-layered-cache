package gocuckoo

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCuckooFilter interface {
	LoadFromSource(ctx context.Context) error
	Insert(ctx context.Context, source interface{}, data []byte) error
	Check(ctx context.Context, source interface{}, data []byte) bool
	Delete(ctx context.Context, source interface{}, data []byte) error
}
type SourceCuckooFilter interface {
	Get(cxt context.Context, source ...interface{}) <-chan interface{}
	Set(cxt context.Context, source interface{}, data map[string]interface{}) error
	Del(ctx context.Context, source ...interface{}) error

	Watch(ctx context.Context, local GoCuckooFilter, consumer interface{}, errChan chan<- error)
}

type LayeredCuckooFilterImpl struct {
	localCuckooFilter map[string]GoCuckooFilter
	source            SourceCuckooFilter
	log               *logrus.Logger
	group             string
	consumer          string
	mux               sync.RWMutex
}
type SourceCuckooFilterImpl struct {
	rdb *redis.Client
	log *logrus.Logger
}
type pubsubParams struct {
	group    string
	consumer string
	channel  string
	message  interface{}
}

func NewSourceCuckooFilter(rdb *redis.Client, log *logrus.Logger) SourceCuckooFilter {
	return &SourceCuckooFilterImpl{
		rdb: rdb,
		log: log,
	}
}
func (scf *SourceCuckooFilterImpl) Get(ctx context.Context, sources ...interface{}) <-chan interface{} {
	cur := 0
	ch := make(chan interface{})
	defer close(ch)
	for _, key := range sources {
		k := key.(string)
		go func(ctx context.Context, key string) {
			for {
				select {
				case <-ctx.Done():
					ch <- fmt.Errorf("context done,%w", ctx.Err())
					return
				default:
					val, err := scf.rdb.CFScanDump(ctx, key, int64(cur)).Result()
					if err != nil {
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
		}(ctx, k)
	}

	return ch
}
func (scf *SourceCuckooFilterImpl) Set(ctx context.Context, source interface{}, data map[string]interface{}) error {
	key := source.(string)
	pipeline := scf.rdb.TxPipeline()
	if _, ok := data["data"]; !ok {
		return fmt.Errorf("data not found")
	}
	pipeline.CFAdd(ctx, key, data["data"])

	pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: data,
	})
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

// Watch watches the stream for new messages and processes them.
//
// The consumer parameter is a *pubsubParams object which containing the stream name and the consumer group to use.
// The local parameter is the local memory cuckoo filter to update.
// The message 'operate' is used to determine if the message is a delete operation.
// It returns a channel that will receive any errors that occur while processing the stream.
// The function will block until the context is done.
func (scf *SourceCuckooFilterImpl) Watch(ctx context.Context, local GoCuckooFilter, consumer interface{}, errChan chan<- error) {
	params, ok := consumer.(*pubsubParams)
	if !ok {
		errChan <- fmt.Errorf("invalid params")
		return

	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// XReadGroup命令参数
			args := redis.XReadGroupArgs{
				Group:    params.group,
				Consumer: params.consumer,
				Streams:  []string{params.channel, "0-0"},
				Block:    0, // 0表示无限期阻塞直到消息到来
				Count:    1, // 每次读取一个消息

			}

			// 阻塞等待消息
			messages, err := scf.rdb.XReadGroup(ctx, &args).Result()
			if err != nil {
				scf.log.Errorf("Error reading from stream: %v", err)
				errChan <- err
				continue
			}

			// 处理收到的消息
			for _, message := range messages {
				for _, msg := range message.Messages {
					// 确认消息处理
					_, err := scf.rdb.XAck(ctx, params.channel, params.group, msg.ID).Result()
					if err != nil {
						errChan <- err
						scf.log.Errorf("Error acknowledging message: %v", err)
					}
					// 处理消息

					if data, ok := msg.Values["data"].(string); ok {
						if op, ok := msg.Values["op"]; ok && op == "delete" {
							ok = local.Delete([]byte(data))
							if !ok {
								errChan <- fmt.Errorf("delete failed")
								scf.log.Errorf("Error processing message: %v", err)
							}
							continue
						}

						err = local.Insert([]byte(data))
						if err != nil {
							errChan <- err
							scf.log.Errorf("Error processing message: %v", err)
						}
					} else {
						errChan <- fmt.Errorf("invalid data type")
					}

				}
			}

		}
		// 释放CPU
		runtime.Gosched()

	}
}
func NewLayeredCuckooFilter(local map[string]GoCuckooFilter, source SourceCuckooFilter, keys []string, group string, consumer string, log *logrus.Logger) LayeredCuckooFilter {
	return &LayeredCuckooFilterImpl{
		localCuckooFilter: local,
		source:            source,
		log:               log,
		group:             group,
		consumer:          consumer,
	}
}

func (lc *LayeredCuckooFilterImpl) WatchSource(ctx context.Context, errChannel chan<- error) {
	wg := &sync.WaitGroup{}
	defer close(errChannel)
	for key, local := range lc.localCuckooFilter {
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, key string, local GoCuckooFilter) {
			defer wg.Done()
			lc.source.Watch(ctx, local, &pubsubParams{
				group:    lc.group,
				consumer: lc.consumer,
				channel:  key,
			}, errChannel)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					runtime.Gosched()

				}
			}

		}(ctx, wg, key, local)

	}
	wg.Wait()

}

func (lc *LayeredCuckooFilterImpl) LoadFromSource(ctx context.Context) error {

	err := lc.initFromSource(ctx)
	if err != nil {
		return err
	}

	return nil
}
func (lc *LayeredCuckooFilterImpl) Insert(ctx context.Context, key interface{}, data []byte) error {
	if _, ok := key.(string); !ok {
		return fmt.Errorf("invalid source,it should be string")
	}
	relKey := key.(string)
	lc.mux.Lock()
	defer lc.mux.Unlock()
	local := lc.localCuckooFilter[relKey]
	if local.Check(data) {
		return nil
	}
	err := local.Insert(data)
	if err != nil {
		return err
	}
	err = lc.source.Set(ctx, key, map[string]interface{}{"data": data})
	if err != nil {
		return err
	}
	return nil
}
func (lc *LayeredCuckooFilterImpl) Check(ctx context.Context, key interface{}, data []byte) bool {
	lc.mux.RLock()
	defer lc.mux.RUnlock()
	relKey := key.(string)
	return lc.localCuckooFilter[relKey].Check(data)
}
func (lc *LayeredCuckooFilterImpl) Delete(ctx context.Context, key interface{}, data []byte) error {
	lc.mux.Lock()
	defer lc.mux.Unlock()
	relKey := key.(string)
	ok := lc.localCuckooFilter[relKey].Delete(data)
	err := lc.source.Set(ctx, key, map[string]interface{}{"data": data, "op": "delete"})
	if err != nil {
		lc.log.Errorf("Error deleting data: %v", err)
		return fmt.Errorf("Error deleting data: %w", err)
	}
	if !ok {
		lc.log.Errorf("Error deleting local data")
		return fmt.Errorf("Error deleting local data")
	}
	return nil
}
func (lc *LayeredCuckooFilterImpl) initFromSource(ctx context.Context) error {
	for key, local := range lc.localCuckooFilter {
		go func(ctx context.Context, key string, localCuckooFilter GoCuckooFilter) {
			sourceData := lc.source.Get(ctx, key)
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
		}(ctx, key, local)

	}

	return nil

}
