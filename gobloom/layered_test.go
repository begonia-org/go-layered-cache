package gobloom

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	golayeredcache "github.com/begonia-org/go-layered-cache"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func mockRedis() *gomonkey.Patches {
	patches := gomonkey.ApplyFunc((*redis.Client).Process, func(cli *redis.Client, ctx context.Context, cmd redis.Cmder) error {

		return nil
	})
	iterSeq := []gomonkey.OutputCell{
		{Values: gomonkey.Params{true}},  // 第一次调用返回
		{Values: gomonkey.Params{false}}, // 第二次调用返回
	}
	patches.ApplyFuncSeq((*redis.ScanIterator).Next, iterSeq)
	patches.ApplyFuncReturn((*redis.Client).Scan, &redis.ScanCmd{})
	patches.ApplyFuncReturn((*redis.ScanCmd).Iterator, &redis.ScanIterator{})
	patches.ApplyFuncReturn((*redis.ScanIterator).Val, "bf:cache:test")
	headers, err := os.ReadFile("testdata/header.bin")
	if err != nil {
		panic(err)
	}
	hDump := &redis.ScanDumpCmd{}
	hDump.SetVal(redis.ScanDump{Data: golayeredcache.BytesToString(headers), Iter: 1})
	bdata, err := os.ReadFile("testdata/bloom.bin")
	if err != nil {
		panic(err)

	}
	dDump := &redis.ScanDumpCmd{}
	dDump.SetVal(redis.ScanDump{Data: golayeredcache.BytesToString(bdata), Iter: 1385})
	outSeq := []gomonkey.OutputCell{
		{Values: gomonkey.Params{hDump}},                // 第一次调用返回
		{Values: gomonkey.Params{dDump}},                // 第二次调用返回
		{Values: gomonkey.Params{&redis.ScanDumpCmd{}}}, // 第二次调用返回
	}
	outSeq2 := []gomonkey.OutputCell{
		{Values: gomonkey.Params{redis.ScanDump{Data: golayeredcache.BytesToString(headers), Iter: 1}, nil}},  // 第一次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: golayeredcache.BytesToString(bdata), Iter: 1385}, nil}}, // 第二次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: "", Iter: 0}, nil}},                                     // 第二次调用返回
	}
	patches.ApplyFuncSeq((*redis.Client).BFScanDump, outSeq)
	patches.ApplyFuncSeq((*redis.ScanDumpCmd).Result, outSeq2)

	patches.ApplyFunc((*redis.Client).Process, func(cli *redis.Client, ctx context.Context, cmd redis.Cmder) error {
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

	// streamSeq := []gomonkey.OutputCell{
	// 	{
	// 		Values: gomonkey.Params{[]redis.XStream{{Stream: "bf:test",
	// 			Messages: []redis.XMessage{
	// 				{ID: "1-0",
	// 					Values: map[string]interface{}{"value": "test0103102", "key": "bf:cache:test01"}}}}}, nil},
	// 	},
	// }
	patches.ApplyFuncReturn((*redis.XStreamSliceCmd).Result, []redis.XStream{{Stream: "bf:test",
		Messages: []redis.XMessage{
			{ID: "1-0",
				Values: map[string]interface{}{"value": "test0103102", "key": "bf:cache:test01"}}}}}, nil)

	patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)
	return patches

}
func TestLayeredLoad(t *testing.T) {

	c.Convey("TestLayeredLoad", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := golayeredcache.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 10,
			Channels:  []interface{}{"bf:test"},
			WatcheAt:  "$",
		}
		patches := mockRedis()
		defer patches.Reset()
		options := &golayeredcache.LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "bf:cache",
			Entries:   1000,
			Errors:    0.01,
			Channel:   "bf:test",
			Strategy:  golayeredcache.LocalOnly,
			Log:       logrus.New(),
		}
		layered := New(options, &BloomBuildOptions{
			Entries:      1000,
			Errors:       0.01,
			BloomOptions: BLOOM_OPT_NOROUND | BLOOM_OPT_FORCE64 | BLOOM_OPT_NO_SCALING,
			Growth:       2,
		})
		err := layered.DumpSourceToLocal(context.Background())
		c.So(err, c.ShouldBeNil)
		time.Sleep(1 * time.Second)
		values, err := layered.GetFromLocal(context.Background(), "bf:cache:test", []byte("item1"))
		c.So(err, c.ShouldBeNil)
		c.So(values, c.ShouldNotBeNil)
		for _, v := range values {
			c.So(v, c.ShouldEqual, true)
		}
		err = layered.SetToLocal(context.Background(), "bf:cache:test", []byte("item2"))
		c.So(err, c.ShouldBeNil)
		// source:=NewBloomSourceImpl(, "test", nil)
	})
}

func TestWatch(t *testing.T) {
	c.Convey("TestWatch", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := golayeredcache.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"bf:test"},
			WatcheAt:  "$",
		}
		patches := mockRedis()
		defer patches.Reset()
		options := &golayeredcache.LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "bf:cache",
			Entries:   1000,
			Errors:    0.01,
			Channel:   "bf:test",
			Strategy:  golayeredcache.LocalOnly,
			Log:       logrus.New(),
		}
		defaultBuildBloomOptions := &BloomBuildOptions{
			Entries:      1000,
			Errors:       0.01,
			BloomOptions: BLOOM_OPT_NOROUND | BLOOM_OPT_FORCE64 | BLOOM_OPT_NO_SCALING,
			Growth:       2,
		}
		layered1 := New(options, defaultBuildBloomOptions)
		t.Log("set success")

		layered1.Watch(context.Background())
		t.Log("watch success")
		layered2 := New(options, defaultBuildBloomOptions)
		layered2.Watch(context.Background())
		t.Log("watch success2")
		time.Sleep(1 * time.Second)
		ctx := context.Background()
		err := layered1.Set(ctx, "bf:cache:test01", []byte("test0103102"))
		c.So(err, c.ShouldBeNil)
		time.Sleep(1 * time.Second)
		vals, err := layered2.Get(context.Background(), "bf:cache:test01", []byte("test0103102"))
		c.So(err, c.ShouldBeNil)
		c.So(vals, c.ShouldNotBeNil)
		for _, v := range vals {
			c.So(v.(bool), c.ShouldBeTrue)
		}
	})
}
