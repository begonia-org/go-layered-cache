package golayeredcache

import (
	"context"
	"fmt"

	"github.com/begonia-org/go-layered-cache/source"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredBloomFilter struct {
	*LayeredCacheImpl
	log       *logrus.Logger
	keyPrefix string
}

func (lb *LayeredBloomFilter) OnMessage(ctx context.Context, from string, message interface{}) error {
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
	return lb.SetToLocal(ctx, key, []byte(value))
}

// onScan is a callback function for scan BF.SCANDUMP key iterator
func (lb *LayeredBloomFilter) onScan(ctx context.Context, key interface{}) error {

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
func (lb *LayeredBloomFilter) DumpSourceToLocal(ctx context.Context) error {
	ch := lb.Scan(ctx, lb.keyPrefix, lb.onScan)
	for err := range ch {
		if err != nil {
			lb.log.Error("scan", err)
		}

	}
	return nil
}
func (lc *LayeredBloomFilter) Watch(ctx context.Context) <-chan error {
	return lc.LayeredCacheImpl.Watch(ctx, lc.OnMessage)
}
func (lc *LayeredBloomFilter) UnWatch() error {
	return lc.LayeredCacheImpl.UnWatch()
}
func NewLayeredBloomFilter(layered *LayeredCacheImpl, keyPrefix string, log *logrus.Logger) LayeredFilter {
	return &LayeredBloomFilter{
		LayeredCacheImpl: layered,
		keyPrefix:        keyPrefix,
		log:              log,
	}
}

func (lc *LayeredBloomFilter) Check(ctx context.Context, key string, value []byte) (bool, error) {
	vals, err := lc.LayeredCacheImpl.Get(ctx, key, value)
	if err != nil {
		lc.log.Errorf("check value of %s error:%v", key, err)
		return false, err
	}
	if len(vals) == 0 {
		return false, nil
	}
	return vals[0].(bool), nil
}
func (lc *LayeredBloomFilter) Add(ctx context.Context, key string, value []byte) error {
	return lc.LayeredCacheImpl.Set(ctx, key, value)
}
