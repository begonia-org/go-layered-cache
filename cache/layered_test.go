package cache

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/allegro/bigcache/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func TestLoad(t *testing.T) {
	c.Convey("TestLoad", t, func() {
		ctx := context.Background()
		RDB := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})

		cache := New(ctx, &LayeredCacheConfig{
			Config: bigcache.DefaultConfig(10 * time.Minute),
			RDB:    RDB,
			Log:    logrus.New(),

			ReadStrategy: LocalOnly,
		})
		patches := gomonkey.ApplyFuncReturn((*redis.Client).Scan, &redis.ScanCmd{})
		patches.ApplyFuncReturn((*redis.ScanCmd).Result, []string{"cache:test:item1", "cache:test:item2", "cache:test:item2"}, uint64(0), nil)
		outSeq := []gomonkey.OutputCell{
			{
				Values: gomonkey.Params{"1", nil},
			},
			{
				Values: gomonkey.Params{"2", nil},
			},
			{
				Values: gomonkey.Params{"3", nil},
			},
		}
		argsSeq := []gomonkey.OutputCell{
			{
				Values: gomonkey.Params{[]interface{}{"0", "cache:test:item1"}},
			},
			{
				Values: gomonkey.Params{[]interface{}{"0", "cache:test:item2"}},
			},
			{
				Values: gomonkey.Params{[]interface{}{"0", "cache:test:item3"}},
			},
		}
		patches = patches.ApplyFuncSeq((*redis.StringCmd).Result, outSeq)
		patches = patches.ApplyFuncSeq((*redis.StringCmd).Args, argsSeq)
		patches = patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)
		defer patches.Reset()
		cache.LoadSourceCache(ctx, "cache:test:*")
		time.Sleep(1 * time.Second)
		value, err := cache.Get(ctx, "cache:test:item1")
		c.So(err, c.ShouldBeNil)
		c.So(string(value), c.ShouldEqual, "1")
	})
}
func watcherMock() *gomonkey.Patches {
	// patches := make([]*gomonkey.Patches, 0)
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

	streamSeq := []gomonkey.OutputCell{
		{
			Values: gomonkey.Params{[]redis.XStream{{Stream: "cache:test:channel1:channel",
				Messages: []redis.XMessage{
					{ID: "1-0",
						Values: map[string]interface{}{"value": "2", "key": "cache:test:item10"}}}}}, nil},
		},
		{
			Values: gomonkey.Params{[]redis.XStream{{Stream: "cache:test:channel1:channel",
				Messages: []redis.XMessage{
					{ID: "1-0",
						Values: map[string]interface{}{"value": "2", "key": "cache:test:item10", "op": "delete"}}}}}, nil},
		},
	}
	patches = patches.ApplyFuncSeq((*redis.XStreamSliceCmd).Result, streamSeq)


	patches = patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)

	return patches
}
func TestWatch(t *testing.T) {
	c.Convey("TestWatch", t, func() {
		ctx := context.Background()
		RDB := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		patches := watcherMock()
		defer patches.Reset()
		cache := New(ctx, &LayeredCacheConfig{
			Config:       bigcache.DefaultConfig(10 * time.Minute),
			RDB:          RDB,
			Log:          logrus.New(),
			WatchKey:     "cache:test:channel1",
			ReadStrategy: LocalOnly,
		})

		errChan := cache.WatchSource(ctx)
		go func() {
			for err := range errChan {
				if err != nil {
					t.Error(err)
				}
			}
		}()

		cache2 := New(ctx, &LayeredCacheConfig{
			Config:       bigcache.DefaultConfig(10 * time.Minute),
			RDB:          RDB,
			Log:          logrus.New(),
			WatchKey:     "cache:test:channel1",
			ReadStrategy: LocalOnly,
		})

		err := cache2.Set(ctx, "cache:test:item10", []byte("2"), 0)
		c.So(err, c.ShouldBeNil)
		time.Sleep(1500 * time.Millisecond)
		value, err := cache.Get(ctx, "cache:test:item10")
		c.So(err, c.ShouldBeNil)
		c.So(string(value), c.ShouldEqual, "2")
		err = cache2.Del(ctx, "cache:test:item10")
		c.So(err, c.ShouldBeNil)
		time.Sleep(1 * time.Second)
		_, err = cache.Get(ctx, "cache:test:item10")
		c.So(err, c.ShouldEqual,bigcache.ErrEntryNotFound)

	})
}
