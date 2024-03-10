package cache

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/allegro/bigcache/v3"
	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func mockRedis() *gomonkey.Patches {
	patches := gomonkey.ApplyFunc((*redis.Client).Process, func(cli *redis.Client, ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if len(args) > 2 {
			for _, arg := range args {
				if arg == "xread" {
					cmd.SetErr(nil)
					time.Sleep(1 * time.Second)
					return nil
				}
			}
		}
		return nil
	})
	iterSeq := []gomonkey.OutputCell{
		{Values: gomonkey.Params{true}},  // 第一次调用返回
		{Values: gomonkey.Params{false}}, // 第二次调用返回
	}
	patches.ApplyFuncSeq((*redis.ScanIterator).Next, iterSeq)
	patches.ApplyFuncReturn((*redis.Client).Scan, &redis.ScanCmd{})
	patches.ApplyFuncReturn((*redis.ScanCmd).Iterator, &redis.ScanIterator{})
	patches.ApplyFuncReturn((*redis.ScanIterator).Val, "test:cache:item2")
	patches.ApplyFuncReturn((*redis.StringCmd).Result, "item", nil)

	// patches.ApplyFuncSeq((*redis.ScanDumpCmd).Result, outSeq2)

	patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)
	return patches

}
func TestLoad(t *testing.T) {
	c.Convey("TestLoad", t, func() {
		ctx := context.Background()

		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := golayeredcache.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"cache:test"},
			WatcheAt:  "$",
		}
		patches := mockRedis()
		defer patches.Reset()
		options := golayeredcache.LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "test:cache:*",
			Entries:   1000,
			Errors:    0.01,
			Channel:   "cache:test",
			Strategy:  golayeredcache.LocalOnly,
			Log:       logrus.New(),
		}

		defaultBuildOptions := bigcache.DefaultConfig(10 * time.Minute)
		layered, _ := New(ctx, options, defaultBuildOptions)
		err := layered.DumpSourceToLocal(ctx)
		c.So(err, c.ShouldBeNil)
		time.Sleep(1 * time.Second)

		value, err := layered.Get(ctx, "test:cache:item2")
		c.So(err, c.ShouldBeNil)
		c.So(len(value), c.ShouldEqual, 1)
		c.So(string(value[0].([]byte)), c.ShouldEqual, "item")
	})
}

func TestWatch(t *testing.T) {
	c.Convey("TestWatch", t, func() {
		ctx := context.Background()

		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := golayeredcache.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"cache:test:channel"},
			WatcheAt:  "0-0",
		}
		patches := mockRedis()
		defer patches.Reset()
		options := golayeredcache.LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "test:cache:*",
			Entries:   1000,
			Errors:    0.01,
			Channel:   "cache:test:channel",
			Strategy:  golayeredcache.LocalOnly,
			Log:       logrus.New(),
		}

		defaultBuildOptions := bigcache.DefaultConfig(10 * time.Minute)
		layered1, _ := New(ctx, options, defaultBuildOptions)

		err := layered1.Set(ctx, "test:cache:item2", []byte("item"))
		c.So(err, c.ShouldBeNil)
		// ctx1, _ := context.WithCancel(ctx)
		pathch1 := gomonkey.ApplyFunc((*redis.XStreamSliceCmd).Result, func(_ *redis.XStreamSliceCmd) ([]redis.XStream, error) {
			return []redis.XStream{{Stream: "cache:test:channel",
				Messages: []redis.XMessage{{ID: "1-1",
					Values: map[string]interface{}{"value": "item", "key": "test:cache:item2"}}}}}, nil

		})
		layered1.Watch(ctx)
		layered2, _ := New(ctx, options, defaultBuildOptions)
		time.Sleep(1500 * time.Millisecond)
		// cancel()
		// ctx2, _ := context.WithCancel(ctx)
		layered2.Watch(ctx)
		time.Sleep(2500 * time.Millisecond)
		vals, err := layered2.Get(ctx, "test:cache:item2")
		c.So(err, c.ShouldBeNil)
		c.So(len(vals), c.ShouldEqual, 1)
		c.So(string(vals[0].([]byte)), c.ShouldEqual, "item")
		pathch1.Reset()

		pathch2 := gomonkey.ApplyFunc((*redis.XStreamSliceCmd).Result, func(_ *redis.XStreamSliceCmd) ([]redis.XStream, error) {
			return []redis.XStream{{Stream: "cache:test:channel",
				Messages: []redis.XMessage{{ID: "1-2",
					Values: map[string]interface{}{"value": "item", "key": "test:cache:item2", "op": "delete"}}}}}, nil

		})
		err = layered2.Del(ctx, "test:cache:item2")
		c.So(err, c.ShouldBeNil)

		time.Sleep(3500 * time.Millisecond)
		vals, err = layered1.Get(ctx, "test:cache:item2")

		c.So(err, c.ShouldEqual, bigcache.ErrEntryNotFound)
		c.So(len(vals), c.ShouldEqual, 0)
		pathch2.Reset()
	})
}
