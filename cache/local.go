package cache

import (
	"context"
	"fmt"

	"github.com/allegro/bigcache/v3"
)


type LocalCacheImpl struct {
	*bigcache.BigCache
}

func NewLocalCache(cache *bigcache.BigCache) *LocalCacheImpl {
	return &LocalCacheImpl{
		BigCache: cache,
	}
}

func (lc *LocalCacheImpl) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	val,err:= lc.BigCache.Get(key.(string))
	if err != nil {
		return nil,err
	
	}
	return []interface{}{val},nil
}

// Set key value
//
// NOTE
func (lc *LocalCacheImpl) Set(ctx context.Context, key interface{}, args ...interface{}) error {
	if len(args) == 0 {
		return fmt.Errorf("args is empty")
	}
	for _,v := range args {
		value,ok := v.([]byte)
		if !ok {
			return fmt.Errorf("value is not []byte")
		}
		err := lc.BigCache.Set(key.(string), value)
		if err != nil {
			return err
		}
	
	}
	return nil
}

func (lc *LocalCacheImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {

	return lc.BigCache.Delete(key.(string))
}
func (lc *LocalCacheImpl)Load(ctx context.Context, key interface{}, args ...interface{}) error{
	if len(args) == 0 {
		return fmt.Errorf("args is empty")
	}
	for _,v := range args {
		value,ok := v.([]byte)
		if !ok {
			return fmt.Errorf("value is not []byte")
		}
		err := lc.BigCache.Set(key.(string), value)
		if err != nil {
			return err
		}
	}
	return nil
}