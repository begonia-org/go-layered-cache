package cache

import (
	"context"
	"math/big"
	"time"

	"github.com/allegro/bigcache/v3"
)

type LocalCacheImpl struct {
	data *bigcache.BigCache
	expirations *bigcache.BigCache
}

func (lc *LocalCacheImpl) SetExpiration(key string, expiration time.Duration) error {
	ttl := time.Now().Add(expiration).UnixMicro()
	s := big.NewInt(ttl)
	err := lc.expirations.Set(key, s.Bytes())
	return err
}

func (lc *LocalCacheImpl) Set(key string, value []byte, expiration time.Duration) error {
	err := lc.data.Set(key, value)
	if err != nil {
		return err
	}
	if expiration == 0 {
		return nil
	}
	err = lc.SetExpiration(key, expiration)
	return err
}
func (lc *LocalCacheImpl) Del(ctx context.Context, key string) error {
	err := lc.data.Delete(key)
	if err != nil && err != bigcache.ErrEntryNotFound {
		return err
	}
	err = lc.expirations.Delete(key)
	if err != nil && err != bigcache.ErrEntryNotFound{
		return err
	}
	return nil
}
func (lc *LocalCacheImpl) TTL(key string) (int64, error) {
	val, err := lc.expirations.Get(key)
	if err != nil && err != bigcache.ErrEntryNotFound {
		return 0, err
	}
	if err == bigcache.ErrEntryNotFound {
		return 0, nil

	}
	s := new(big.Int)
	s.SetBytes(val)
	return s.Int64() - time.Now().UnixMicro(), nil
}

// Get key value.
// For keys with an expiration setting, a lazy deletion strategy is employed. 
// Before retrieving the value, it first checks if the key has expired. 
// If it has expired, the key is deleted and a null value is returned
func (lc *LocalCacheImpl) Get(key string) ([]byte, error) {
	ttl, err := lc.TTL(key)
	if err != nil {
		return nil, err
	}
	if ttl < 0 {
		err := lc.data.Delete(key)
		if err != nil {
			return nil, err
		}
		err = lc.expirations.Delete(key)
		if err != nil {
			return nil, err

		}
		return nil, nil
	}

	val, err := lc.data.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func NewLocalCacheImpl(ctx context.Context,maxEntries int) (*LocalCacheImpl, error) {
	config := bigcache.DefaultConfig(100 * 365 * 24 * time.Hour)
	config.CleanWindow = 0
	config.MaxEntrySize = maxEntries
	data, err := bigcache.New(ctx, config)
	if err != nil {
		return nil, err
	}
	expirations, err := bigcache.New(ctx, config)
	if err != nil {
		return nil, err
	}
	return &LocalCacheImpl{
		data:    data,
		expirations: expirations,
	}, nil
}
