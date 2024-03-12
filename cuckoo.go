// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"context"
	"fmt"

	"github.com/begonia-org/go-layered-cache/local"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCuckooFilterImpl struct {
	*BaseLayeredCacheImpl
	log       *logrus.Logger
	keyPrefix string
}

func newLayeredCuckooFilterImpl(layered *BaseLayeredCacheImpl, keyPrefix string, log *logrus.Logger) LayeredCuckooFilter {
	return &LayeredCuckooFilterImpl{
		BaseLayeredCacheImpl: layered,
		keyPrefix:        keyPrefix,
		log:              log,
	}
}
func (lb *LayeredCuckooFilterImpl) OnMessage(ctx context.Context, from string, message interface{}) error {
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
	value, ok := values["value"].(string)
	if !ok {
		return fmt.Errorf("value is not string, got %T", values["value"])

	}
	// lb.log.Infof("onMessage:%v", values)
	if op, ok := values["op"].(string); ok && op == "delete" {
		return lb.DelOnLocal(ctx, key, []byte(value))
	}
	return lb.SetToLocal(ctx, key, []byte(value))
}

// onScan is a callback function for scan BF.SCANDUMP key iterator
func (lb *LayeredCuckooFilterImpl) onScan(ctx context.Context, key interface{}) error {

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
		if val, ok := v.(source.RedisDump); ok {
			// lb.SetToLocal(ctx,keyStr,val)
			lb.log.Infof("load %s", keyStr)
			if err := lb.Load(ctx, keyStr, val); err != nil {
				lb.log.Errorf("load:%v", err)
				continue
			}
		}
	}

	return nil
}
func (lb *LayeredCuckooFilterImpl) LoadDump(ctx context.Context) error {
	ch := lb.Scan(ctx, lb.keyPrefix, lb.onScan)
	for err := range ch {
		if err != nil {
			lb.log.Error("scan", err)
		}

	}
	return nil
}
func (lc *LayeredCuckooFilterImpl) Watch(ctx context.Context) <-chan error {
	return lc.BaseLayeredCacheImpl.Watch(ctx, lc.OnMessage)
}
func (lc *LayeredCuckooFilterImpl) UnWatch() error {
	return lc.BaseLayeredCacheImpl.UnWatch()
}

func (lc *LayeredCuckooFilterImpl) Check(ctx context.Context, key string, value []byte) (bool, error) {
	vals, err := lc.BaseLayeredCacheImpl.Get(ctx, key, value)
	if err != nil {
		lc.log.Errorf("check value of %s error:%v", key, err)
		return false, err
	}
	if len(vals) == 0 {
		return false, nil
	}
	return vals[0].(bool), nil
}
func (lc *LayeredCuckooFilterImpl) Add(ctx context.Context, key string, value []byte) error {
	return lc.BaseLayeredCacheImpl.Set(ctx, key, value)
}
func (lc *LayeredCuckooFilterImpl) Del(ctx context.Context, key string, value []byte) error {
	return lc.BaseLayeredCacheImpl.Del(ctx, key, value)
}

func (lc *LayeredCuckooFilterImpl) AddLocalFilter( key string, filter local.Filter) error{
	return lc.BaseLayeredCacheImpl.local.(local.LocalFilters).AddFilter(key, filter)
}
