package gobloom

import (
	"context"
	"fmt"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type LayeredBloomFilter struct {
	*golayeredcache.LayeredCacheImpl
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
	value, ok := values["value"].([]byte)
	if !ok {
		return fmt.Errorf("value is not []byte, got %T", values["value"])
	}

	return lb.SetToLocal(ctx, key, value)
}

// onScan is a callback function for scan BF.SCANDUMP key iterator
func (lb *LayeredBloomFilter) onScan(ctx context.Context, key interface{}) error {
	// get the key from the iterator
	keyStr, ok := key.(string)
	if !ok {
		return fmt.Errorf("key is not string, got %T", key)

	}
	// wg:=&sync.WaitGroup{}
	// go func(wg *sync.WaitGroup) {
	ch := lb.Dump(ctx, keyStr)
	for v := range ch {
		if err, ok := v.(error); ok {
			lb.log.Errorf("dump:%v", err)
			continue
		}
		if val, ok := v.(golayeredcache.RedisDump); ok {
			// lb.SetToLocal(ctx,keyStr,val)
			lb.log.Infof("load %s", keyStr)
			if err := lb.Load(ctx, keyStr, val); err != nil {
				lb.log.Errorf("load:%v", err)
				continue
			}
		}
	}
	// }(wg)
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

func NewLayeredBloomFilter(layered *golayeredcache.LayeredCacheImpl, keyPrefix string, log *logrus.Logger) *LayeredBloomFilter {
	return &LayeredBloomFilter{
		LayeredCacheImpl: layered,
		keyPrefix:        keyPrefix,
		log:              log,
	}
}
