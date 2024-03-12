package main

import (
	"context"
	"log"
	"time"

	glc "github.com/begonia-org/go-layered-cache"
	"github.com/begonia-org/go-layered-cache/gobloom"
	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type Cache struct {
	KV     glc.LayeredKeyValueCache
	bloom  glc.LayeredFilter
	cuckoo glc.LayeredCuckooFilter
}

func NewCache() *Cache {
	ctx := context.Background()
	kvWatcher := source.NewWatchOptions([]interface{}{"test:kv:channel"})
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	KvOptions := glc.LayeredBuildOptions{
		RDB:       rdb,
		Strategy:  glc.LocalThenSource,
		Watcher:   kvWatcher,
		Channel:   "test:kv:channel",
		Log:       logrus.New(),
		KeyPrefix: "cache:test:kv",
	}
	kv, err := glc.NewKeyValueCache(ctx, KvOptions, 100*100*5)
	if err != nil {
		panic(err)

	}
	bloomWatcher := source.NewWatchOptions([]interface{}{"test:bloom:channel"})
	bloomOptions := &glc.LayeredBuildOptions{
		RDB:       rdb,
		Strategy:  glc.LocalThenSource,
		Watcher:   bloomWatcher,
		Channel:   "test:bloom:channel",
		Log:       logrus.New(),
		KeyPrefix: "cache:test:bloom",
	}

	cuckooWatcher := source.NewWatchOptions([]interface{}{"test:cuckoo:channel"})
	cuckooOptions := &glc.LayeredBuildOptions{
		RDB:       rdb,
		Strategy:  glc.LocalThenSource,
		Watcher:   cuckooWatcher,
		Channel:   "test:bloom:channel",
		Log:       logrus.New(),
		KeyPrefix: "cache:test:bloom",
	}
	return &Cache{
		KV:     kv,
		bloom:  glc.NewLayeredBloom(bloomOptions, gobloom.DefaultBuildBloomOptions),
		cuckoo: glc.NewLayeredCuckoo(cuckooOptions, gocuckoo.DefaultBuildCuckooOptions),
	}
}
func (c *Cache) watcher(ctx context.Context) {
	errKV := c.KV.Watch(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errKV:
				log.Println("kv error:", err)
			}
		}
	}(ctx)
	errBloom := c.bloom.Watch(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errBloom:
				log.Println("bloom error:", err)
			}
		}
	}(ctx)
	errCuckoo := c.cuckoo.Watch(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCuckoo:
				log.Println("cuckoo error:", err)
			}
		}

	}(ctx)

}
func main() {
	cache := NewCache()
	ctx := context.Background()
	cache.watcher(ctx)
	err := cache.KV.Set(ctx, "key", []byte("value"), 0)
	if err != nil {
		panic(err)
	}
	value, err := cache.KV.Get(ctx, "key")
	if err != nil {
		panic(err)
	}
	log.Println("kv value:", string(value))

	err = cache.bloom.Add(ctx, "bloom", []byte("value"))
	if err != nil {
		panic(err)
	}
	ret, err := cache.bloom.Check(ctx, "bloom", []byte("value"))
	if err != nil {
		panic(err)
	}

	log.Println("bloom value:", ret)

	err = cache.cuckoo.Add(ctx, "cuckoo", []byte("value"))
	if err != nil {
		panic(err)
	}
	ret, err = cache.cuckoo.Check(ctx, "cuckoo", []byte("value"))
	if err != nil {
		panic(err)
	}
	log.Println("cuckoo value:", ret)
	time.Sleep(10 * time.Second)
}
