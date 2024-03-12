# Golang 多级缓存器 Go-Layered-Cache

## 基于redis和本机内存的支持分布式环境的多级缓存框架

**特性:**

- 支持分布式环境下多级缓存同步  
- 支持redis布隆过滤器数据和本地内存布隆过滤器双向同步  
- 支持redis布隆过滤器`bf.scandump`数据导入本地内存布隆过滤器  
- 支持redis布谷鸟过滤器数据和本地内存布谷鸟过滤器双向同步  
- 支持redis布谷鸟过滤器`cf.scandump`数据导入本地内存布谷鸟过滤器 
- 支持redis `set`、`get`缓存同步
- 基于golang实现了[redisbloom](https://github.com/RedisBloom/RedisBloom)模块的布隆过滤器和布谷鸟过滤器 


## 开始

### 安装

支持 [Go module](https://github.com/golang/go/wiki/Modules) 并通过以下方式引入你的代码中

```
import "github.com/begonia-org/go-layered-cache"
```

随后执行`go [build|run|test]` 将会自动安装必须的依赖。

另外，你也可以通过下面的方式安装 `go-layered-cache` 包:

```sh
$ go get -u github.com/begonia-org/go-layered-cache
```

### 示例

首先，你需要导入 `go-layered-cache` 包以使用 `go-layered-cache`，下面是一个最简单的[示例](example/main.go):

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

使用 Go 命令来运行这个demo:

```
$ go run main.go
```





## 贡献

非常期待和欢迎您提交PR为go-layered-cache做出贡献
