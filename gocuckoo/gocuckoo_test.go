package gocuckoo

import (
	"testing"
	"unsafe"

	golayeredcache "github.com/begonia-org/go-layered-cache"
	c "github.com/smartystreets/goconvey/convey"
)

func TestCuckoo(t *testing.T) {
	c.Convey("TestCuckoo", t, func() {
		cf := NewGoCuckooFilter(DefaultBuildBloomOptions)
		hash := golayeredcache.MurmurHash64A([]byte("item1"), 0)
		c.So(hash, c.ShouldEqual, 8732656736511103026)
		c.So(hash%255+1, c.ShouldEqual, 7)
		err := cf.Insert([]byte("item1"))
		c.So(err, c.ShouldEqual, nil)
		ok := cf.Check([]byte("item1"))
		c.So(ok, c.ShouldEqual, true)

		ok = cf.Check([]byte("item2"))
		c.So(ok, c.ShouldEqual, false)
		ok = cf.Delete([]byte("item1"))
		c.So(ok, c.ShouldEqual, true)

		ok = cf.Check([]byte("item1"))
		c.So(ok, c.ShouldEqual, false)

	})
}
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// func TestCuckooLoadFrom(t *testing.T) {
// 	c.Convey("TestCuckooLoadFrom", t, func() {
// 		rdb := redis.NewClient(&redis.Options{
// 			Addr:     "",
// 			Password: "",
// 			DB:       0,
// 		})

// 		headers := []byte{4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 20, 0, 1, 0}
// 		hDump := &redis.ScanDumpCmd{}
// 		hDump.SetVal(redis.ScanDump{Data: BytesToString(headers), Iter: 1})
// 		bdata := []byte{0, 0, 180, 180, 7, 25, 0, 0}
// 		dDump := &redis.ScanDumpCmd{}
// 		dDump.SetVal(redis.ScanDump{Data: BytesToString(bdata), Iter: 9})
// 		outSeq := []gomonkey.OutputCell{
// 			{Values: gomonkey.Params{hDump}}, // 第一次调用返回
// 			{Values: gomonkey.Params{dDump}}, // 第二次调用返回
// 		}
// 		outSeq2 := []gomonkey.OutputCell{
// 			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(headers), Iter: 1}}}, // 第一次调用返回
// 			{Values: gomonkey.Params{redis.ScanDump{Data: BytesToString(bdata), Iter: 9}}},   // 第二次调用返回
// 		}
// 		patches := gomonkey.ApplyFuncSeq((*redis.Client).CFScanDump, outSeq)
// 		patches2 := gomonkey.ApplyFuncSeq((*redis.ScanDumpCmd).Val, outSeq2)

// 		defer patches.Reset()
// 		defer patches2.Reset()
// 		val := rdb.CFScanDump(context.Background(), "cf", 0).Val()
// 		dump := val.Data
// 		cf := &GoCuckooFilterImpl{}
// 		err := cf.LoadFrom(&RedisDump{Data: dump, Iter: uint64(val.Iter)})
// 		c.So(err, c.ShouldEqual, nil)
// 		c.So(cf.numItems, c.ShouldBeGreaterThan, 0)
// 		val = rdb.CFScanDump(context.Background(), "cf", val.Iter).Val()
// 		err = cf.LoadFrom(&RedisDump{Data: val.Data, Iter: uint64(val.Iter)})
// 		c.So(err, c.ShouldEqual, nil)
// 		ok := cf.Check([]byte("item3"))
// 		c.So(ok, c.ShouldEqual, true)
// 		ok = cf.Check([]byte("item3dddff"))
// 		c.So(ok, c.ShouldEqual, false)
// 	})
// }
