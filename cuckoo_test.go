package golayeredcache

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/source"
	"github.com/begonia-org/go-layered-cache/utils"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func mockCuckooRedis() *gomonkey.Patches {
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

	headers := []byte{4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 20, 0, 1, 0}
	hDump := &redis.ScanDumpCmd{}
	hDump.SetVal(redis.ScanDump{Data: utils.BytesToString(headers), Iter: 1})
	bdata := []byte{0, 0, 180, 180, 7, 25, 0, 0}
	dDump := &redis.ScanDumpCmd{}
	dDump.SetVal(redis.ScanDump{Data: utils.BytesToString(bdata), Iter: 9})

	dDump.SetVal(redis.ScanDump{Data: utils.BytesToString(bdata), Iter: 1385})
	outSeq := []gomonkey.OutputCell{
		{Values: gomonkey.Params{hDump}},                // 第一次调用返回
		{Values: gomonkey.Params{dDump}},                // 第二次调用返回
		{Values: gomonkey.Params{&redis.ScanDumpCmd{}}}, // 第二次调用返回
	}
	outSeq2 := []gomonkey.OutputCell{
		{Values: gomonkey.Params{redis.ScanDump{Data: utils.BytesToString(headers), Iter: 1}, nil}}, // 第一次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: utils.BytesToString(bdata), Iter: 9}, nil}},   // 第二次调用返回
		{Values: gomonkey.Params{redis.ScanDump{Data: "", Iter: 0}, nil}},                           // 第二次调用返回
	}
	patches.ApplyFuncSeq((*redis.Client).BFScanDump, outSeq)
	patches.ApplyFuncSeq((*redis.ScanDumpCmd).Result, outSeq2)

	patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)
	return patches

}
func TestCuckooLoad(t *testing.T) {
	c.Convey("test cuckoo load", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := source.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"cf:test"},
			WatcheAt:  "$",
		}
		patches := mockCuckooRedis()
		defer patches.Reset()
		options := &LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "cf:cache",
			// Entries:   1000,
			// Errors:    0.01,
			Channel:  "cf:test",
			Strategy: LocalOnly,
			Log:      logrus.New(),
		}
		defaultBuildOptions := gocuckoo.DefaultBuildCuckooOptions
		layered := NewLayeredCuckoo(options, defaultBuildOptions)
		ctx := context.Background()
		err := layered.LoadDump(ctx)
		c.So(err, c.ShouldBeNil)
		ret, err := layered.Check(ctx, "bf:cache:test", []byte("item3"))
		c.So(err, c.ShouldBeNil)
		c.So(ret, c.ShouldBeTrue)
		ret1, err := layered.Check(ctx, "bf:cache:test", []byte("item3ffffffffas"))
		c.So(err, c.ShouldBeNil)
		c.So(ret1, c.ShouldBeFalse)
	})
}

func TestCuckooWatch(t *testing.T) {
	c.Convey("test cuckoo watch", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:16379",
			DB:   0,
		})
		watcher := source.WatchOptions{
			Block:     3 * time.Second,
			BatchSize: 1,
			Channels:  []interface{}{"cf:test"},
			WatcheAt:  "0",
		}
		patches := mockCuckooRedis()
		defer patches.Reset()

		options := &LayeredBuildOptions{
			RDB:       rdb,
			Watcher:   &watcher,
			KeyPrefix: "cf:cache",
			Channel:  "cf:test",
			Strategy: LocalOnly,
			Log:      logrus.New(),
		}
		defaultBuildOptions := gocuckoo.DefaultBuildCuckooOptions
		layered1 := NewLayeredCuckoo(options, defaultBuildOptions)
		
		ctx := context.Background()

		err := layered1.Add(ctx, "cf:cache:test", []byte("item4"))
		c.So(err, c.ShouldBeNil)
		ctx1, _ := context.WithCancel(ctx)
		pathch1 := gomonkey.ApplyFunc((*redis.XStreamSliceCmd).Result, func(_ *redis.XStreamSliceCmd) ([]redis.XStream, error) {
			return []redis.XStream{{Stream: "cf:test",
				Messages: []redis.XMessage{{ID: "1-0",
					Values: map[string]interface{}{"value": "item4", "key": "cf:cache:test"}}}}}, nil

		})
		layered1.Watch(ctx1)
		layered2 := NewLayeredCuckoo(options, defaultBuildOptions)
		time.Sleep(1500 * time.Millisecond)
		// cancel()
		ctx2, _ := context.WithCancel(ctx)
		layered2.Watch(ctx2)
		time.Sleep(1500 * time.Millisecond)
		ret, err := layered2.Check(ctx, "cf:cache:test", []byte("item4"))
		c.So(err, c.ShouldBeNil)
		c.So(ret, c.ShouldBeTrue)
		pathch1.Reset()
		pathch2 := gomonkey.ApplyFunc((*redis.XStreamSliceCmd).Result, func(_ *redis.XStreamSliceCmd) ([]redis.XStream, error) {
			return []redis.XStream{{Stream: "cf:test",
				Messages: []redis.XMessage{{ID: "1-0",
					Values: map[string]interface{}{"value": "item4", "key": "cf:cache:test", "op": "delete"}}}}}, nil

		})
		err = layered2.Del(ctx, "cf:cache:test", []byte("item4"))
		c.So(err, c.ShouldBeNil)

		time.Sleep(1500 * time.Millisecond)
		ret, err = layered1.Check(ctx, "cf:cache:test", []byte("item4"))
		c.So(err, c.ShouldBeNil)
		c.So(ret, c.ShouldBeFalse)
		pathch2.Reset()
	})
}
