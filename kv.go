// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredKeyValueCacheImpl struct {
	*BaseLayeredCacheImpl
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
	exp, _ := values["expire"].(int64)
	expVal := time.Duration(exp) * time.Second
	return lb.SetToLocal(ctx, key, []byte(value), expVal)
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
		vals, ok := v.([]interface{})
		if !ok {
			lb.log.Errorf("dump: error type%v", v)
			continue
		}
		if err := lb.Load(ctx, keyStr, vals...); err != nil {
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
	return lc.BaseLayeredCacheImpl.Watch(ctx, lc.OnMessage)
}
func (lc *LayeredKeyValueCacheImpl) UnWatch() error {
	return lc.BaseLayeredCacheImpl.UnWatch()
}

func newLayeredKeyValueCacheImpl(layered *BaseLayeredCacheImpl, keyPrefix string, log *logrus.Logger) LayeredKeyValueCache {
	return &LayeredKeyValueCacheImpl{
		BaseLayeredCacheImpl: layered,
		keyPrefix:            keyPrefix,
		log:                  log,
	}
}

func (lc *LayeredKeyValueCacheImpl) Get(ctx context.Context, key string) ([]byte, error) {
	values, err := lc.BaseLayeredCacheImpl.Get(ctx, key)
	if err != nil || len(values) == 0 {
		return nil, err
	}
	if val, ok := values[0].([]byte); ok&&len(val)!=0 {
		return val, nil
	}
	if val, ok := values[0].(string); ok&&val!="" {
		return []byte(val), nil

	}
	return nil, fmt.Errorf("type except []byte or string, but got %T", values[0])
}
func (lc *LayeredKeyValueCacheImpl) Set(ctx context.Context, key string, value []byte, exp time.Duration) error {
	return lc.BaseLayeredCacheImpl.Set(ctx, key, value, exp)
}

func (lc *LayeredKeyValueCacheImpl) Del(ctx context.Context, key string) error {
	return lc.BaseLayeredCacheImpl.Del(ctx, key)
}

// func (lc *LayeredKeyValueCacheImpl)Del()
