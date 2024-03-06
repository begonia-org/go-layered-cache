package redcuckoo

import (
	"context"
	"testing"
	"unsafe"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	c "github.com/smartystreets/goconvey/convey"
)

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func TestCuckoo(t *testing.T) {
	cf := NewCuckooFilter(1000, 2, 500, 1)
	defer cf.Release()

	c.Convey("TestCuckoo", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "",
			Password: "",
			DB:       0,
		})
		headers := []byte{4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 20, 0, 1, 0}
		hDump := &redis.ScanDumpCmd{}
		hDump.SetVal(redis.ScanDump{Data: BytesToString(headers), Iter: 1})
		bdata := []byte{0, 0, 180, 180, 7, 25, 0, 0}
		dDump := &redis.ScanDumpCmd{}
		dDump.SetVal(redis.ScanDump{Data: BytesToString(bdata), Iter: 9})
		outSeq := []gomonkey.OutputCell{
			{Values: gomonkey.Params{hDump}}, // 第一次调用返回
			{Values: gomonkey.Params{dDump}}, // 第二次调用返回
		}
		outSeq2 := []gomonkey.OutputCell{
			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(headers), Iter: 1}}}, // 第一次调用返回
			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(bdata), Iter: 9}}},   // 第二次调用返回
		}
		patches := gomonkey.ApplyFuncSeq((*redis.Client).CFScanDump, outSeq)
		patches2 := gomonkey.ApplyFuncSeq((*redis.ScanDumpCmd).Val, outSeq2)
	
		defer patches.Reset()
		defer patches2.Reset()
		val := rdb.CFScanDump(context.Background(), "cf", 0).Val()
	
		// rdb.CFScanDump(context.Background(), "test2", 0).String()
		dump := val.Data
		cf.LoadFrom(dump, val.Iter)
		val = rdb.CFScanDump(context.Background(), "cf", val.Iter).Val()
		// rdb.CFScanDump(context.Background(), "test2", 0).String()
		dump = val.Data
	
		cf.LoadFrom(dump, val.Iter)
		c.So(cf.Check([]byte("item1")), c.ShouldEqual, true)

		ok := cf.Check([]byte("item3"))
		c.So(ok, c.ShouldEqual, true)
		ok = cf.Check([]byte("item3dddff"))
		c.So(ok, c.ShouldEqual, false)
		cf.Delete([]byte("item1"))
		c.So(cf.Check([]byte("item1")), c.ShouldBeFalse)
		cf.Insert([]byte("item6"), false)
		c.So(cf.Check([]byte("item6")), c.ShouldBeTrue)
	})

}
