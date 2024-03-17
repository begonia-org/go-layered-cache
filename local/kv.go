// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"fmt"
	"time"

	"github.com/begonia-org/go-layered-cache/cache"
)

type LocalKeyValueCacheImpl struct {
	*cache.LocalCacheImpl
}

func NewLocalCache(ctx context.Context, maxEntries int) (*LocalKeyValueCacheImpl, error) {
	local, err := cache.NewLocalCacheImpl(ctx, maxEntries)
	if err != nil {
		return nil, err
	}
	return &LocalKeyValueCacheImpl{
		LocalCacheImpl: local,
	}, nil
}

func (lc *LocalKeyValueCacheImpl) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	val, err := lc.LocalCacheImpl.Get(key.(string))
	if err != nil {
		return nil, err

	}
	return []interface{}{val}, nil
}

// Set key value
//
// NOTE
func (lc *LocalKeyValueCacheImpl) Set(ctx context.Context, key interface{}, args ...interface{}) error {
	if len(args) == 0 {
		return fmt.Errorf("args is empty")
	}
	value, ok := args[0].([]byte)
	if !ok {
		// return fmt.Errorf("value is not []byte")
		value = []byte(fmt.Sprintf("%v", args[0]))

	}
	if len(args) > 2 {
		return fmt.Errorf("args is too much")
	}
	var expire time.Duration = 0
	if len(args) == 2 {
		expire, ok = args[1].(time.Duration)
		if !ok {
			return fmt.Errorf("expire is not time.Duration")
		}
	}
	return lc.LocalCacheImpl.Set(key.(string), value, expire)

}

func (lc *LocalKeyValueCacheImpl) Del(ctx context.Context, key interface{}, args ...interface{}) error {

	return lc.LocalCacheImpl.Del(ctx, key.(string))
}
func (lc *LocalKeyValueCacheImpl) Load(ctx context.Context, key interface{}, args ...interface{}) error {
	if len(args) == 0 {
		return fmt.Errorf("args is empty")
	}
	return lc.Set(ctx, key, args...)
}
