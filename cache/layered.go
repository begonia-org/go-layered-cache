package cache

import (
	"context"
	"fmt"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredCacheFilter struct {
	*golayeredcache.LayeredCacheImpl
	log       *logrus.Logger
	keyPrefix string
}

func (lb *LayeredCacheFilter) OnMessage(ctx context.Context, from string, message interface{}) error {
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
	value:=values["value"].(string)
	if !ok {
		return fmt.Errorf("value is not string, got %T", values["value"])
	}

	return lb.SetToLocal(ctx, key, []byte(value))
}

// onScan is a callback function for scan BF.SCANDUMP key iterator
func (lb *LayeredCacheFilter) onScan(ctx context.Context, key interface{}) error {

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
func (lb *LayeredCacheFilter) DumpSourceToLocal(ctx context.Context) error {
	ch := lb.Scan(ctx, lb.keyPrefix, lb.onScan)
	for err := range ch {
		if err != nil {
			lb.log.Error("scan", err)
		}

	}
	return nil
}
func (lc *LayeredCacheFilter) Watch(ctx context.Context) <-chan error {
	return lc.LayeredCacheImpl.Watch(ctx, lc.OnMessage)
}
func (lc *LayeredCacheFilter) UnWatch() error {
	return lc.LayeredCacheImpl.UnWatch()
}

func NewLayeredCacheFilter(layered *golayeredcache.LayeredCacheImpl, keyPrefix string, log *logrus.Logger) *LayeredCacheFilter {
	return &LayeredCacheFilter{
		LayeredCacheImpl: layered,
		keyPrefix:        keyPrefix,
		log:              log,
	}
}

// func (lc *LayeredCacheFilter)Del()
