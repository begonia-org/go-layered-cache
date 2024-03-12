[简体中文](README.md)|[English](README_EN.md)
# Golang Layered Caching: Go-Layered-Cache

## A multi-level caching framework based on Redis and local memory for distributed environments

**Features:**

- Supports multi-level cache synchronization in distributed environments
- Supports bidirectional synchronization between Redis Bloom filters and local memory Bloom filters
- Allows importing Redis Bloom filter data into local memory Bloom filters using `bf.scandump`
- Supports bidirectional synchronization between Redis Cuckoo filters and local memory Cuckoo filters
- Allows importing Redis Cuckoo filter data into local memory Cuckoo filters using `cf.scandump`
- Supports Redis `set` and `get` cache synchronization
- Implements Bloom and Cuckoo filters based on the [RedisBloom](https://github.com/RedisBloom/RedisBloom) module in Golang

## Getting Started

### Installation

Supports [Go module](https://github.com/golang/go/wiki/Modules) and can be imported into your code as follows:

```go
import "github.com/begonia-org/go-layered-cache"
```

Subsequently, executing `go [build|run|test]` will automatically install the necessary dependencies.

Alternatively, you can install the `go-layered-cache` package with the following command:

```sh
$ go get -u github.com/begonia-org/go-layered-cache
```

### Example

First, you need to import the `go-layered-cache` package to use `go-layered-cache`. Below is a simple [example](example/main.go):

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/allegro/bigcache/v3"
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
	kv, err := glc.NewKeyValueCache(ctx, KvOptions, bigcache.DefaultConfig(10*time.Minute))
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
	err := cache.KV.Set(ctx, "key", []byte("value"))
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
```

Run this demo using the Go command:

```
$ go run main.go
```

## Contributing

We highly anticipate and welcome your PRs to contribute to go-layered-cache