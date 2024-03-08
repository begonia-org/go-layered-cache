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
		}
		patches := mockRedis()
		defer patches.Reset()
		redSource := golayeredcache.NewDataSourceFromRedis(rdb, &watcher)
		log := logrus.New()
		source := NewBloomSourceImpl(redSource, "bf:test", log)
		local := NewLocalBloomFilters(make(map[string]*GoBloomChain))
		layered := NewLayeredBloomFilter(golayeredcache.NewLayeredCacheImpl(source, local, log, golayeredcache.LocalOnly), "bf:cache:*", log)
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
