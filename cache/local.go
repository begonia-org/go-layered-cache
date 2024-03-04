package cache

import "github.com/allegro/bigcache/v3"

type LocalCache interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
}

type LocalCacheImpl struct {
	*bigcache.BigCache
}

func NewLocalCache(cache *bigcache.BigCache) LocalCache {
	return &LocalCacheImpl{
		BigCache: cache,
	}
}

func (lc *LocalCacheImpl) Get(key string) ([]byte, error) {
	return lc.BigCache.Get(key)
}

func (lc *LocalCacheImpl) Set(key string, value []byte) error {
	return lc.BigCache.Set(key, value)
}

func (lc *LocalCacheImpl) Del(key string) error {
	return lc.BigCache.Delete(key)
}
