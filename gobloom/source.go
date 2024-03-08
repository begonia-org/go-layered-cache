package gobloom

import (
	"context"
	"fmt"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/sirupsen/logrus"
)

type BloomSourceImpl struct {
	*golayeredcache.DataSourceFromRedis
	channel string
	cancel  context.CancelFunc
	log     *logrus.Logger
}
type LocalBloomFilter interface {
	Test(data []byte) bool
	Add(data []byte) bool
}

func (bs *BloomSourceImpl) Get(ctx context.Context, key string, values ...interface{}) ([]interface{}, error) {
	args := make([]interface{}, len(values))
	for i, v := range values {
		arg := []interface{}{key, v}
		args[i] = arg
	}
	vals, err := bs.DataSourceFromRedis.GetExec(ctx, "BF.EXISTS", args...)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(vals))
	for i, v := range vals {
		result[i] = v != nil
	}
	return result, nil
}

func (bs *BloomSourceImpl) Set(ctx context.Context, key string, values ...interface{}) error {
	args := make([]interface{}, len(values))
	messages := make([]interface{}, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		data, ok := v.([]byte)
		if !ok || len(data) == 0 {
			return fmt.Errorf("value is not []byte or empty")
		}

		arg := []interface{}{key, v}
		args[i] = arg
		messages[i] = map[string]interface{}{
			"key":   key,
			"value": data,
		}
	}
	return bs.DataSourceFromRedis.TxWriteHandle(ctx, &golayeredcache.TxHandleKeysOptions{
		Channel:      bs.channel,
		Cmd:          "BF.ADD",
		CmdArgs:      args,
		SendMessages: messages,
	})
}

func (bs *BloomSourceImpl) Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		var iter int64 = 0
		defer close(ch)
		keyStr, _ := key.(string)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				data, next, err := bs.DataSourceFromRedis.BFScanDump(ctx, keyStr, iter)
				iter = next
				if err != nil {
					ch <- err
					return
				}
				if data == nil || next==0 {
					return
				}
				ch <- golayeredcache.RedisDump{Data: data, Iter: uint64(iter)}
			}
		}
	}()
	return ch

}
func (bs *BloomSourceImpl) UnWatch(ctx context.Context) error {
	return bs.DataSourceFromRedis.UnWatch(ctx, bs.cancel)
}

func NewBloomSourceImpl(source *golayeredcache.DataSourceFromRedis, channel string, log *logrus.Logger) *BloomSourceImpl {
	return &BloomSourceImpl{
		DataSourceFromRedis: source,
		channel:             channel,
		log:                 log,
	}
}
