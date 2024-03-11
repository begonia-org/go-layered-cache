package source

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

type CacheSourceImpl struct {
	*DataSourceFromRedis
	channel string
	cancel  context.CancelFunc
	log     *logrus.Logger
}

func (bs *CacheSourceImpl) Get(ctx context.Context, key string, values ...interface{}) ([]interface{}, error) {
	args := make([]interface{}, len(values))
	for i, v := range values {
		arg := []interface{}{key, v}
		args[i] = arg
	}
	vals, err := bs.DataSourceFromRedis.Get(ctx, key, args...)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 1)
	result[0] = vals
	return result, nil
}

func (bs *CacheSourceImpl) Set(ctx context.Context, key string, values ...interface{}) error {
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

		arg := []interface{}{key, string(data)}
		args[i] = arg
		messages[i] = map[string]interface{}{
			"key":   key,
			"value": data,
		}
	}
	return bs.DataSourceFromRedis.TxWriteHandle(ctx, &TxHandleKeysOptions{
		Channel:      bs.channel,
		Cmd:          "SET",
		CmdArgs:      args,
		SendMessages: messages,
	})
}
func (bs *CacheSourceImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {
	delKey:=[]interface{}{key}
	args2 := []interface{}{delKey}
	messages := []interface{}{map[string]interface{}{
		"key":   key,
		"value": "",
		"op":    "delete",
	}}
	return bs.DataSourceFromRedis.TxWriteHandle(ctx, &TxHandleKeysOptions{
		Channel:      bs.channel,
		Cmd:          "DEL",
		CmdArgs:      args2,
		SendMessages: messages,
	})
}

func (bs *CacheSourceImpl) Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{} {
	ch := make(chan interface{}, 10)
	defer close(ch)
	values, err := bs.DataSourceFromRedis.Get(ctx, key.(string))
	if err != nil {
		ch <- fmt.Errorf("Get:%w", err)
	}
	if values == "" {
		return ch

	}
	ch <- []byte(values)
	// go func() {

	return ch

}
func (bs *CacheSourceImpl) UnWatch(ctx context.Context) error {
	return bs.DataSourceFromRedis.UnWatch(ctx, bs.cancel)
}

func NewCacheSourceImpl(source *DataSourceFromRedis, channel string, log *logrus.Logger) *CacheSourceImpl {
	return &CacheSourceImpl{
		DataSourceFromRedis: source,
		channel:             channel,
		log:                 log,
	}
}
