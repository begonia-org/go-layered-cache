package golayeredbloom

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestCuckoo(t *testing.T) {
	cf := NewCuckooFilter(1000, 2, 500, 1)
	// defer cf.Release()
	// 817448764559060972
	hash := cf.Hash([]byte("item1"))
	hash2:=MurmurHash64A([]byte("item1"),0)
	t.Log("hash2:",hash2)
	t.Log("cf:", hash)
	t.Log("cf fp:", hash%255+1)

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	val := rdb.CFScanDump(context.Background(), "cf", 0).Val()

	// rdb.CFScanDump(context.Background(), "test2", 0).String()
	dump := val.Data
	fmt.Printf("dump:%v\n", dump)
	cf.LoadFrom(dump, val.Iter)
	val = rdb.CFScanDump(context.Background(), "cf", val.Iter).Val()
	// rdb.CFScanDump(context.Background(), "test2", 0).String()
	dump = val.Data
	bData := StringToBytes(dump)
	t.Log("bDta len:", len(bData))
	for _, b := range bData {
		fmt.Printf("bData:%d ", b)
	}
	fmt.Printf("dump2:%v,%d\n", dump, val.Iter)
	cf.LoadFrom(dump, val.Iter)
	t.Log(cf.Check([]byte("item2")))
	defer cf.Release()
}
