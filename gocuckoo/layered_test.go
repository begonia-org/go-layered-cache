package gocuckoo

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func TestLoad(t *testing.T) {
	c.Convey("TestLoad", t, func() {
		cfg := &LayeredCuckooFilterConfig{
			Filters: map[string]GoCuckooFilter{
				"cf": NewGoCuckooFilter(1000, 2, 20, 2),
			},
			Rdb: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				DB:   0,
			}),
			Log: logrus.New(),
		}
		filter := New(cfg)
		ctx := context.Background()

		headers := []byte{4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 20, 0, 1, 0}
		hDump := &redis.ScanDumpCmd{}
		hDump.SetVal(redis.ScanDump{Data: BytesToString(headers), Iter: 1})
		bdata := []byte{0, 0, 180, 180, 7, 25, 0, 0}
		dDump := &redis.ScanDumpCmd{}
		dDump.SetVal(redis.ScanDump{Data: BytesToString(bdata), Iter: 9})
		lasted := &redis.ScanDumpCmd{}
		lasted.SetVal(redis.ScanDump{Data: "", Iter: 0})
		outSeq := []gomonkey.OutputCell{
			{Values: gomonkey.Params{hDump}},  // 第一次调用返回
			{Values: gomonkey.Params{dDump}},  // 第二次调用返回
			{Values: gomonkey.Params{lasted}}, // 第三次调用返回
		}
		outSeq2 := []gomonkey.OutputCell{
			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(headers), Iter: 1}, nil}}, // 第一次调用返回
			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(bdata), Iter: 9}, nil}},   // 第二次调用返回
			{Values: gomonkey.Params{redis.ScanDump{Data: "", Iter: 0}, redis.Nil}},               // 第三次调用返回
		}
		patches := gomonkey.ApplyFuncSeq((*redis.Client).CFScanDump, outSeq)
		patches2 := gomonkey.ApplyFuncSeq((*redis.ScanDumpCmd).Result, outSeq2)

		defer patches.Reset()
		defer patches2.Reset()
		err := filter.LoadFromSource(ctx)
		c.So(err, c.ShouldBeNil)
		ok := filter.Check(ctx, "cf", []byte("test"))
		c.So(ok, c.ShouldBeFalse)
		c.So(filter.Check(ctx, "cf", []byte("item3")), c.ShouldBeTrue)
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
			Values: gomonkey.Params{[]redis.XStream{{Stream: "cf:channel",
				Messages: []redis.XMessage{{ID: "1-0",
					Values: map[string]interface{}{"data": "item4", "key": "cf"}}}}}, nil},
		},
		{
			Values: gomonkey.Params{[]redis.XStream{{Stream: "cf:channel",
				Messages: []redis.XMessage{{ID: "1-0",
					Values: map[string]interface{}{"data": "item4", "key": "cf", "op": "delete"}}}}}, nil},
		},
	}
	patches = patches.ApplyFuncSeq((*redis.XStreamSliceCmd).Result, streamSeq)

	patches = patches.ApplyFuncReturn((*redis.Pipeline).Exec, nil, nil)

	return patches
}

func TestWatch(t *testing.T) {
	c.Convey("TestWatch", t, func() {
		cfg := &LayeredCuckooFilterConfig{
			Filters: map[string]GoCuckooFilter{
				"cf": NewGoCuckooFilter(1000, 2, 20, 2),
			},
			Rdb: redis.NewClient(&redis.Options{
				Addr: "localhost:16379",
				DB:   0,
			}),
			Log:                    logrus.New(),
			ReadStreamMessageBlock: 4 * time.Second,
			ReadStreamMessageSize:  10,
		}
		filter := New(cfg)
		patches := watcherMock()
		patches.ApplyFuncSeq((*redis.Pipeline).Exec, []gomonkey.OutputCell{{Values: gomonkey.Params{nil, nil}}, {Values: gomonkey.Params{nil, nil}}})
		defer patches.Reset()

		ctx := context.Background()
		errChannel := make(chan error)
		go filter.WatchSource(ctx, errChannel)
		go func(ctx context.Context) {
			for err := range errChannel {
				if err != nil {
					t.Error(err)
				}
			}

		}(ctx)

		//
		cfg2 := &LayeredCuckooFilterConfig{
			Filters: map[string]GoCuckooFilter{
				"cf": NewGoCuckooFilter(1000, 2, 20, 2),
			},
			Rdb: redis.NewClient(&redis.Options{
				Addr: "localhost:16379",
				DB:   0,
			}),
			Log: logrus.New(),
		}
		filter2 := New(cfg2)
		err := filter2.Insert(ctx, "cf", []byte("item4"))
		c.So(err, c.ShouldBeNil)
		time.Sleep(1500 * time.Millisecond)
		c.So(filter.Check(ctx, "cf", []byte("item4")), c.ShouldBeTrue)
		err = filter2.Delete(ctx, "cf", []byte("item4"))
		c.So(err, c.ShouldBeNil)
		c.So(filter2.Check(ctx, "cf", []byte("item4")), c.ShouldBeFalse)
		time.Sleep(1 * time.Second)
		c.So(filter.Check(ctx, "cf", []byte("item4")), c.ShouldBeFalse)

	})
}
