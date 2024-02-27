package golayeredbloom

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
)

func TestBloom(t *testing.T) {
	c.Convey("TestBloom", t, func() {
		patch := gomonkey.ApplyFunc((*LayeredBloomFilter).cache, func(_ *LayeredBloomFilter, _ context.Context, _ string, _ []uint) error {
			return nil
		})
		defer patch.Reset()

		pubsub := NewBloomPubSub(&redis.Client{}, "group", "consumer", logrus.New())
		bf := NewLayeredBloomFilter(pubsub, "test", "test01")
		ok, err := bf.TestOrAdd(context.TODO(), "test", []byte("hello"), 10000, 0.01)
		c.So(err, c.ShouldEqual, nil)
		c.So(ok, c.ShouldEqual, false)

		ok, err = bf.TestOrAdd(context.TODO(), "test", []byte("hello"), 10000, 0.01)
		c.So(err, c.ShouldEqual, nil)
		c.So(ok, c.ShouldEqual, true)

		ok, err = bf.TestOrAdd(context.TODO(), "test2", []byte("hello"), 10000, 0.01)
		c.So(err, c.ShouldEqual, nil)
		c.So(ok, c.ShouldEqual, false)

	})

}

func TestPubSubBloom(t *testing.T) {
	c.Convey("test publish and subscribe bloom is ok", t, func() {
		db, mock := redismock.NewClientMock()
		bfTest := newBloomFilter(10000, 0.01)
		testLocations := bfTest.Locations([]byte("hello"))
		mock.ExpectTxPipeline()
		for _, location := range testLocations {
			mock.ExpectSetBit("test", int64(location), 1).SetVal(1)
		}

		mock.ExpectXAdd(&redis.XAddArgs{
			Stream: "test",
			Values: &BloomBroadcastMessage{
				From:      "test01",
				BloomKey:  "test",
				Locations: testLocations,
			},
		}).SetVal("ok")
		mock.ExpectTxPipelineExec().SetErr(nil)
		for _, location := range testLocations {
			mock.ExpectGetBit("test", int64(location)).SetVal(1)
		}
		pubsub := NewBloomPubSub(db, "group", "consumer", logrus.New())
		bf := NewLayeredBloomFilter(pubsub, "test", "test01")
		ok, err := bf.TestOrAdd(context.TODO(), "test", []byte("hello"), 10000, 0.01)
		c.So(err, c.ShouldEqual, nil)
		c.So(ok, c.ShouldEqual, false)
		for _, location := range testLocations {
			v := db.GetBit(context.Background(), "test", int64(location)).Val()
			c.So(v, c.ShouldEqual, 1)
		}
		ok, err = bf.TestOrAdd(context.TODO(), "test", []byte("hello"), 10000, 0.01)
		c.So(err, c.ShouldEqual, nil)
		c.So(ok, c.ShouldEqual, true)
		time.Sleep(1 * time.Second)

		testLocations = bfTest.Locations([]byte("world"))
		msgs := []redis.XMessage{
			{
				ID: "0-0",
				Values: map[string]interface{}{
					"from":      "test02",
					"bloom_key": "test",
					"locations": testLocations,
				},
			},
		}

		pubsub2 := NewBloomPubSub(db, "group", "consumer2", logrus.New())
		bf2 := NewLayeredBloomFilter(pubsub2, "test", "test02")

		mock.ExpectTxPipeline()
		for _, location := range testLocations {
			mock.ExpectSetBit("test", int64(location), 1).SetVal(1)
		}

		mock.ExpectXAdd(&redis.XAddArgs{
			Stream: "test",
			Values: &BloomBroadcastMessage{
				From:      "test02",
				BloomKey:  "test",
				Locations: testLocations,
			},
		}).SetVal("ok")
		mock.ExpectTxPipelineExec().SetErr(nil)
		ok, err = bf2.TestOrAdd(context.TODO(), "test", []byte("world"), 10000, 0.01)
		c.So(err, c.ShouldBeNil)
		c.So(ok, c.ShouldEqual, false)

		// wait for the message to be processed
		time.Sleep(1 * time.Second)
		mock.ExpectXReadGroup(&redis.XReadGroupArgs{
			Group:    "group",
			Consumer: "consumer",
			Streams:  []string{"test", "0-0"},
			Block:    0,
			Count:    1,
		}).SetVal([]redis.XStream{
			{
				Stream:   "test",
				Messages: msgs,
			},
		})

		mock.ExpectXAck("test", "group", "0-0").SetVal(1)
		go func(ctx context.Context) {
			errChan := bf.OnStart(ctx)
			for _ = range errChan {
				// t.Log(err)
			}
		}(context.Background())
		time.Sleep(1 * time.Second)
		// the message should be processed and add the bitset to test01
		ok, err = bf.TestOrAdd(context.TODO(), "test", []byte("world"), 10000, 0.01)
		c.So(err, c.ShouldBeNil)
		c.So(ok, c.ShouldEqual, true)
	})

}
