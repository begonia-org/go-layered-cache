package gocuckoo

import (
	"context"
	"fmt"
	"testing"

	golayeredbloom "github.com/begonia-org/go-layered-bloom"
	"github.com/redis/go-redis/v9"
	c "github.com/smartystreets/goconvey/convey"
)

func TestCuckoo(t *testing.T) {
	c.Convey("TestCuckoo", t, func() {
		cf := New(1000, 2, 500, 1)
		hash := golayeredbloom.MurmurHash64A([]byte("item1"), 0)
		t.Log("hash2:", hash)
		t.Log("cf:", hash)
		t.Log("cf fp:", hash%255+1)
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

func TestCuckooLoadFrom(t *testing.T) {
	c.Convey("TestCuckooLoadFrom", t, func() {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		})
		val := rdb.CFScanDump(context.Background(), "cf", 0).Val()

		// rdb.CFScanDump(context.Background(), "test2", 0).String()
		dump := val.Data
		fmt.Printf("dump:%v\n", dump)
		cf := &GoCuckooFilterImpl{}
		err := cf.LoadFrom(&RedisDump{Data: dump, Iter: uint64(val.Iter)})
		c.So(err, c.ShouldEqual, nil)
		c.So(cf.numItems, c.ShouldBeGreaterThan, 0)
		val = rdb.CFScanDump(context.Background(), "cf", val.Iter).Val()
		err = cf.LoadFrom(&RedisDump{Data: val.Data, Iter: uint64(val.Iter)})
		c.So(err, c.ShouldEqual, nil)
		ok := cf.Check([]byte("item3"))
		c.So(ok, c.ShouldEqual, true)
		ok = cf.Check([]byte("item3dddff"))
		c.So(ok, c.ShouldEqual, false)
	})
}
