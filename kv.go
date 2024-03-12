// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredKeyValueCacheImpl struct {
	*LayeredCacheImpl
	log       *logrus.Logger
	keyPrefix string
}

func (lb *LayeredKeyValueCacheImpl) OnMessage(ctx context.Context, from string, message interface{}) error {
	XMessage, ok := message.(redis.XMessage)
	if !ok {
		return fmt.Errorf("type except redis.XMessage,but got %T", message)
	}
	values := XMessage.Values
	if len(values) == 0 {
		return nil
	}
	key, ok := values["key"].(string)
	if !ok {
		return fmt.Errorf("key is not string, got %T", values["key"])
	}

	// lb.log.Infof("onMessage:%v", values)
	if op, ok := values["op"].(string); ok && op == "delete" {
		return lb.DelOnLocal(ctx, key)
	}
	value := values["value"].(string)
	if !ok {
		return fmt.Errorf("value is not string, got %T", values["value"])
	}

	return lb.SetToLocal(ctx, key, []byte(value))
}

func (lb *LayeredKeyValueCacheImpl) onScan(ctx context.Context, key interface{}) error {

	keyStr, ok := key.(string)
	if !ok {
		return fmt.Errorf("key is not string, got %T", key)

	}

	ch := lb.Dump(ctx, keyStr)
	for v := range ch {
		if err, ok := v.(error); ok {
			lb.log.Errorf("dump:%v", err)
			continue
		}
		if err := lb.Load(ctx, keyStr, v); err != nil {
			lb.log.Errorf("load:%v", err)
			continue
		}
	}

	return nil
}
func (lb *LayeredKeyValueCacheImpl) LoadDump(ctx context.Context) error {
	ch := lb.Scan(ctx, lb.keyPrefix, lb.onScan)
	for err := range ch {
		if err != nil {
			lb.log.Error("scan", err)
		}

	}
	return nil
}
func (lc *LayeredKeyValueCacheImpl) Watch(ctx context.Context) <-chan error {
	return lc.LayeredCacheImpl.Watch(ctx, lc.OnMessage)
}
func (lc *LayeredKeyValueCacheImpl) UnWatch() error {
	return lc.LayeredCacheImpl.UnWatch()
}

func NewLayeredKeyValueCacheImpl(layered *LayeredCacheImpl, keyPrefix string, log *logrus.Logger) LayeredKeyValueCache {
	return &LayeredKeyValueCacheImpl{
		LayeredCacheImpl: layered,
		keyPrefix:        keyPrefix,
		log:              log,
	}
}

func (lc *LayeredKeyValueCacheImpl) Get(ctx context.Context, key string) ([]byte, error) {
	values, err := lc.LayeredCacheImpl.Get(ctx, key)
	if err != nil || len(values) == 0 {
		return nil, err
	}
	return values[0].([]byte), nil
}
func (lc *LayeredKeyValueCacheImpl) Set(ctx context.Context, key string, value []byte) error {
	return lc.LayeredCacheImpl.Set(ctx, key, value)
}

func (lc *LayeredKeyValueCacheImpl) Del(ctx context.Context, key string) error {
	return lc.LayeredCacheImpl.Del(ctx, key)
}

// func (lc *LayeredKeyValueCacheImpl)Del()
