package golayeredcache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/begonia-org/go-layered-cache/gobloom"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/begonia-org/go-layered-cache/utils"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func mockBloomRedis() *gomonkey.Patches {
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
	hDump.SetVal(redis.ScanDump{Data: utils.BytesToString(headers), Iter: 1})
	bdata, err := os.ReadFile("testdata/bloom.bin")
	if err != nil {
		panic(err)

	}
	dDump := &redis.ScanDumpCmd{}
	dDump.SetVal(redis.ScanDump{Data: utils.BytesToString(bdata), Iter: 1385})
	outSeq := []gomonkey.OutputCell{
		{Values: gomonkey.Params{hDump}},                // 第一次调用返回
		{Values: gomonkey.Params{dDump}},                // 第二次调用返回
		{Values: gomonkey.Params{&redis.ScanDumpCmd{}}}, // 第二次调用返回
	}
	outSeq2 := []gomonkey.OutputCell{
		{Values: gomonkey.Params{redis.ScanDump{Data: utils.BytesToString(headers), Iter: 1}, nil}},  // 第一次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: utils.BytesToString(bdata), Iter: 1385}, nil}}, // 第二次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: "", Iter: 0}, nil}},                            // 第二次调用返回
	}
	patches.ApplyFuncSeq((*redis.Client).BFScanDump, outSeq)
	patches.ApplyFuncSeq((*redis.ScanDumpCmd).Result, outSeq2)

	patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)
	return patches

}
func TestBloomLayeredLoad(t *testing.T) {

	c.Convey("TestLayeredLoad", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := source.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 10,
			Channels:  []interface{}{"bf:test"},
			WatcheAt:  "$",
		}
		patches := mockBloomRedis()
		defer patches.Reset()
		options := &LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "bf:cache",

			Channel:  "bf:test",
			Strategy: LocalOnly,
			Log:      logrus.New(),
		}
		layered := NewLayeredBloom(options, &gobloom.BloomBuildOptions{
			Entries:      1000,
			Errors:       0.01,
			BloomOptions: gobloom.BLOOM_OPT_NOROUND | gobloom.BLOOM_OPT_FORCE64 | gobloom.BLOOM_OPT_NO_SCALING,
			Growth:       2,
		})
		err := layered.LoadDump(context.Background())
		c.So(err, c.ShouldBeNil)
		time.Sleep(1 * time.Second)
		values, err := layered.Check(context.Background(), "bf:cache:test", []byte("item1"))
		c.So(err, c.ShouldBeNil)
		c.So(values, c.ShouldEqual, true)

		err = layered.Add(context.Background(), "bf:cache:test", []byte("item2"))
		c.So(err, c.ShouldBeNil)
		// source:=NewBloomSourceImpl(, "test", nil)
	})
}

func TestBloomWatch(t *testing.T) {
	c.Convey("TestWatch", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := source.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"bf:test"},
			WatcheAt:  "$",
		}
		patches := mockRedis()
		defer patches.Reset()
		options := &LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "bf:cache",

			Channel:  "bf:test",
			Strategy: LocalOnly,
			Log:      logrus.New(),
		}
		defaultBuildBloomOptions := &gobloom.BloomBuildOptions{
			Entries:      1000,
			Errors:       0.01,
			BloomOptions: gobloom.BLOOM_OPT_NOROUND | gobloom.BLOOM_OPT_FORCE64 | gobloom.BLOOM_OPT_NO_SCALING,
			Growth:       2,
		}
		layered1 := NewLayeredBloom(options, defaultBuildBloomOptions)

		layered1.Watch(context.Background())
		layered2 := NewLayeredBloom(options, defaultBuildBloomOptions)
		layered2.Watch(context.Background())
		time.Sleep(1 * time.Second)
		ctx := context.Background()
		pathch1 := gomonkey.ApplyFunc((*redis.XStreamSliceCmd).Result, func(_ *redis.XStreamSliceCmd) ([]redis.XStream, error) {
			return []redis.XStream{{Stream: "bf:test",
				Messages: []redis.XMessage{{ID: "1-0",
					Values: map[string]interface{}{"value": "test0103102", "key": "bf:cache:test01"}}}}}, nil

		})
		err := layered1.Add(ctx, "bf:cache:test01", []byte("test0103102"))
		c.So(err, c.ShouldBeNil)
		time.Sleep(3 * time.Second)
		vals, err := layered2.Check(context.Background(), "bf:cache:test01", []byte("test0103102"))
		pathch1.Reset()
		c.So(err, c.ShouldBeNil)
		c.So(vals, c.ShouldEqual, true)
	})
}
