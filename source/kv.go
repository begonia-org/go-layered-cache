// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package source

import (
	"context"
	"fmt"
	"time"

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

	messages := []interface{}{}
	msg := map[string]interface{}{
		"key":    key,
		"value":  values[0],
		"expire": 0,
	}
	args := []interface{}{key, values[0]}

	if len(values) > 1 {
		exp, ok := values[1].(time.Duration)
		if !ok {
			return fmt.Errorf("expire is not time.Duration")
		}
		args = append(args, "EX", int(exp.Seconds()))
		msg["expire"] = exp.Seconds()
	}
	messages = append(messages, msg)
	return bs.DataSourceFromRedis.TxWriteHandle(ctx, &TxHandleKeysOptions{
		Channel:      bs.channel,
		Cmd:          "SET",
		CmdArgs:      []interface{}{args},
		SendMessages: messages,
	})
}
func (bs *CacheSourceImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {
	delKey := []interface{}{key}
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
	valuesExp, err := bs.DataSourceFromRedis.Expiration(ctx, key.(string))
	if err != nil {
		ch <- fmt.Errorf("Expiration:%w", err)
	}
	val := []interface{}{[]byte(values), valuesExp}
	ch <- val
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
