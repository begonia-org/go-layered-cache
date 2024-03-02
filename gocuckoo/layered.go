package gocuckoo

import (
	"context"
	"fmt"
	"runtime"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCuckooFilter interface {
	Init(ctx context.Context) error
	Insert(ctx context.Context, data []byte) error
	Check(data []byte) bool
	Delete(ctx context.Context, data []byte) error
}
type SourceCuckooFilter interface {
	GetSourceData(cxt context.Context, source interface{}) <-chan interface{}
	Publish(cxt context.Context, source interface{}, data interface{}) error
	Watch(ctx context.Context, data interface{}, local GoCuckooFilter) <-chan error
}

type LayeredCuckooFilterImpl struct {
	localCuckooFilter GoCuckooFilter
	source            SourceCuckooFilter
	key               string
	log               *logrus.Logger
	group             string
	consumer          string
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

func (scf *SourceCuckooFilterImpl) GetSourceData(ctx context.Context, source interface{}) <-chan interface{} {
	key := source.(string)
	cur := 0
	ch := make(chan interface{})
	defer close(ch)
	go func(ctx context.Context) {
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
	}(ctx)
	return ch

}
func (scf *SourceCuckooFilterImpl) Publish(ctx context.Context, source interface{}, data map[string]interface{}) error {
	key := source.(string)
	pipeline := scf.rdb.TxPipeline()
	if _, ok := data["data"]; !ok {
		return fmt.Errorf("data not found")
	}
	if op, ok := data["op"]; ok && op == "delete" {
		pipeline.CFDel(ctx, key, data["data"])

	} else {
		pipeline.CFAdd(ctx, key, data["data"])

	}
	pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: data,
	})
	_, err := pipeline.Exec(ctx)
	return err
}
func (scf *SourceCuckooFilterImpl) Watch(ctx context.Context, params interface{}, local GoCuckooFilter) <-chan error {
	errChan := make(chan error)
	if pubsubParams, ok := params.(*pubsubParams); !ok || pubsubParams.message == nil {
		close(errChan)

	}
	b := params.(*pubsubParams)
	go func() {

		defer close(errChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// XReadGroup命令参数
				args := redis.XReadGroupArgs{
					Group:    b.group,
					Consumer: b.consumer,
					Streams:  []string{b.channel, "0-0"},
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
						_, err := scf.rdb.XAck(ctx, b.channel, b.group, msg.ID).Result()
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
	}()
	return errChan
}
func (lc *LayeredCuckooFilterImpl) Init(ctx context.Context) error {

	err := lc.initFromSource(ctx)
	if err != nil {
		return err
	}
	errChan := lc.source.Watch(ctx, &pubsubParams{
		group:    lc.group,
		consumer: lc.consumer,
		channel:  lc.key,
	}, lc.localCuckooFilter)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errChan:
				lc.log.Errorf("Error watching stream: %v", err)
			default:
				runtime.Gosched()

			}
		}

	}(ctx)
	return nil
}
func (lc *LayeredCuckooFilterImpl) Insert(ctx context.Context, data []byte) error {
	if lc.localCuckooFilter.Check(data) {
		return nil
	}
	err := lc.localCuckooFilter.Insert(data)
	if err != nil {
		return err
	}
	err = lc.source.Publish(ctx, lc.key, map[string]interface{}{"data": data})
	if err != nil {
		return err
	}
	return nil
}
func (lc *LayeredCuckooFilterImpl) Check(data []byte) bool {
	return lc.localCuckooFilter.Check(data)
}
func (lc *LayeredCuckooFilterImpl) Delete(ctx context.Context, data []byte) error {
	ok := lc.localCuckooFilter.Delete(data)
	err := lc.source.Publish(ctx, lc.key, map[string]interface{}{"data": data, "op": "delete"})
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
	// CuckooFilter_Insert(CuckooFilter *filter, CuckooHash hash)
	sourceData := lc.source.GetSourceData(ctx, lc.key)
	for data := range sourceData {
		if err, ok := data.(error); ok {
			return err
		}
		if dump, ok := data.(*RedisDump); ok {
			err := lc.localCuckooFilter.LoadFrom(dump)
			if err != nil {
				return err
			}
		}
	}
	return nil

}
