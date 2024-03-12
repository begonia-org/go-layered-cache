// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package source

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

type CuckooSourceImpl struct {
	*DataSourceFromRedis
	channel string
	cancel  context.CancelFunc
	log     *logrus.Logger
}


func (bs *CuckooSourceImpl) Get(ctx context.Context, key string, values ...interface{}) ([]interface{}, error) {
	args := make([]interface{}, len(values))
	for i, v := range values {
		arg := []interface{}{key, v}
		args[i] = arg
	}
	vals, err := bs.DataSourceFromRedis.GetExec(ctx, "CF.EXISTS", args...)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(vals))
	for i, v := range vals {
		result[i] = v != nil
	}
	return result, nil
}

func (bs *CuckooSourceImpl) Set(ctx context.Context, key string, values ...interface{}) error {
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
		Cmd:          "CF.ADD",
		CmdArgs:      args,
		SendMessages: messages,
	})
}
func (bs *CuckooSourceImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {
	args2 := make([]interface{}, len(args))
	messages := make([]interface{}, len(args))
	for i, v := range args {
		if v == nil {
			continue
		}
		data, ok := v.([]byte)
		if !ok || len(data) == 0 {
			return fmt.Errorf("value is not []byte or empty")
		}

		arg := []interface{}{key, data}
		args2[i] = arg
		messages[i] = map[string]interface{}{
			"key":   key,
			"value": data,
			"op":    "delete",
		}
	}
	return bs.DataSourceFromRedis.TxWriteHandle(ctx, &TxHandleKeysOptions{
		Channel:      bs.channel,
		Cmd:          "CF.DEL",
		CmdArgs:      args2,
		SendMessages: messages,
	})
}

func (bs *CuckooSourceImpl) Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{} {
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
				data, next, err := bs.DataSourceFromRedis.CFScanDump(ctx, keyStr, iter)
				iter = next
				if err != nil {
					ch <- err
					return
				}
				if data == nil || next == 0 {
					return
				}
				ch <- RedisDump{Data: data, Iter: uint64(iter)}
			}
		}
	}()
	return ch

}
func (bs *CuckooSourceImpl) UnWatch(ctx context.Context) error {
	return bs.DataSourceFromRedis.UnWatch(ctx, bs.cancel)
}

func NewCuckooSourceImpl(source *DataSourceFromRedis, channel string, log *logrus.Logger) *CuckooSourceImpl {
	return &CuckooSourceImpl{
		DataSourceFromRedis: source,
		channel:             channel,
		log:                 log,
	}
}
