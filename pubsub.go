package golayeredbloom

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type BloomPubSub interface {
	Publish(ctx context.Context, channel string, message *BloomBroadcastMessage) error
	Subscribe(ctx context.Context, channel string, bg *LayeredBloomFilter) <-chan error
}

type BloomPubSubImpl struct {
	rdb          *redis.Client
	consumerName string
	groupName    string
	log          *logrus.Logger
}
type BloomBroadcastMessage struct {
	// The message to be broadcast.
	From      string `json:"from"`
	BloomKey  string `json:"bloom_key"`
	Locations []uint `json:"locations"`
}

func NewBloomPubSub(rdb *redis.Client, group, consumer string, log *logrus.Logger) BloomPubSub {
	return &BloomPubSubImpl{rdb: rdb, consumerName: consumer, groupName: group, log: log}

}

// Publish broadcasts a message to a channel.
func (b *BloomPubSubImpl) Publish(ctx context.Context, channel string, message *BloomBroadcastMessage) error {
	pipeline := b.rdb.TxPipeline()
	for _, location := range message.Locations {
		pipeline.SetBit(ctx, message.BloomKey, int64(location), 1)
	}
	pipeline.XAdd(ctx, &redis.XAddArgs{
		Stream: channel,
		Values: message,
	})
	_, err := pipeline.Exec(ctx)
	return err
}
func (b *BloomPubSubImpl) appendBitSet(ctx context.Context, message map[string]interface{}, bf *LayeredBloomFilter) error {
	// 将map转换为JSON字节序列
	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("Error marshalling map to json: %w", err)
	}

	// 将JSON解码到结构体中
	var p BloomBroadcastMessage
	err = json.Unmarshal(jsonData, &p)
	if err != nil {

		return fmt.Errorf("Error unmarshalling json to struct:%w", err)
	}
	return bf.appendBitSet(ctx, p.BloomKey, p.Locations)

}

func (b *BloomPubSubImpl) Subscribe(ctx context.Context, channel string, bf *LayeredBloomFilter) <-chan error {
	errChan := make(chan error)
	go func() {
		defer close(errChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// XReadGroup命令参数
				args := redis.XReadGroupArgs{
					Group:    b.groupName,
					Consumer: b.consumerName,
					Streams:  []string{channel, "0-0"},
					Block:    0, // 0表示无限期阻塞直到消息到来
					Count:    1, // 每次读取一个消息

				}

				// 阻塞等待消息
				messages, err := b.rdb.XReadGroup(ctx, &args).Result()
				if err != nil {
					b.log.Errorf("Error reading from stream: %v", err)
					errChan <- err
					continue
				}

				// 处理收到的消息
				for _, message := range messages {
					for _, msg := range message.Messages {
						// 确认消息处理
						_, err := b.rdb.XAck(ctx, channel, b.groupName, msg.ID).Result()
						if err != nil {
							errChan <- err
							b.log.Errorf("Error acknowledging message: %v", err)
						}
						// 处理消息
						err = b.appendBitSet(ctx, msg.Values, bf)
						if err != nil {
							errChan <- err
							b.log.Errorf("Error processing message: %v", err)
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
