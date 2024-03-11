package local

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/begonia-org/go-layered-cache/gocuckoo"
	"github.com/begonia-org/go-layered-cache/source"
)

type LocalCuckooFilters struct {
	filters map[string]*gocuckoo.GoCuckooFilterImpl
	mux     sync.RWMutex
	// bloomBuildOptions *BloomBuildOptions
	cuckooBuildOptions *gocuckoo.CuckooBuildOptions
}

func (lcf *LocalCuckooFilters) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	lcf.mux.RLock()
	defer lcf.mux.RUnlock()
	f, ok := lcf.filters[key.(string)]
	if !ok {
		return nil, fmt.Errorf("not found such bloom filter %s", key)
		// return nil, nil
	}
	result := make([]interface{}, len(args))
	for i, arg := range args {
		val, ok := arg.([]byte)
		if !ok {
			return nil, fmt.Errorf("arg is not []byte")
		}
		result[i] = f.Check(val)
	}
	return result, nil
}
func (lcf *LocalCuckooFilters) Set(ctx context.Context, key interface{}, args ...interface{}) error {
	lcf.mux.Lock()
	defer lcf.mux.Unlock()
	f, ok := lcf.filters[key.(string)]
	if !ok {
		f = gocuckoo.NewGoCuckooFilter(lcf.cuckooBuildOptions)
		if f == nil {
			return fmt.Errorf("create bloom filter failed")
		}
		lcf.filters[key.(string)] = f
	}
	for _, arg := range args {
		err := f.Insert(arg.([]byte))
		if err != nil {
			return err

		}
	}
	return nil
}
func (lcf *LocalCuckooFilters) AddFilter(key string, filter *gocuckoo.GoCuckooFilterImpl) {
	lcf.mux.Lock()
	defer lcf.mux.Unlock()
	lcf.filters[key] = filter
}

func (lcf *LocalCuckooFilters) loadHeader(key string, data []byte) error {

	header := gocuckoo.RedisCFHeader{}
	headerSize := binary.Size(gocuckoo.RedisCFHeader{})
	if len(data) < headerSize {
		return fmt.Errorf("data too short to contain a redisCFHeader")
	}

	headerReader := bytes.NewReader(data[:headerSize])
	err := binary.Read(headerReader, binary.LittleEndian, &header)
	if err != nil {
		return err
	}
	cf := header.NewRedisCuckooFilterImpl()
	lcf.AddFilter(key, cf)
	return nil

}

func (lcf *LocalCuckooFilters) loadDump(key string, iter uint64, data []byte) error {

	cf, ok := lcf.filters[key]
	if !ok {
		return fmt.Errorf("not found such filter:%s", key)
	}
	datalen := len(data)
	if datalen == 0 || iter <= 0 || uint64(iter-1) < uint64(datalen) {
		return fmt.Errorf("invalid data len:%d,iter:%d", datalen, iter)
	}

	offset := iter - uint64(datalen) - 1
	var currentSize uint64
	var filterIx int = 0
	var filter *gocuckoo.SubCF
	for filterIx < int(cf.GetFiltersNum()) {
		filter = cf.GetFilter(filterIx)

		currentSize = uint64(filter.BucketSize) * filter.NumBuckets
		if offset < currentSize {
			break
		}
		offset -= currentSize
		filterIx++
	}
	if filter == nil || (offset > math.MaxUint64-uint64(datalen)) || uint64(filter.BucketSize)*filter.NumBuckets < offset+uint64(datalen) {
		return fmt.Errorf("invalid filter,%v,offset:%d", filter, offset)
	}
	// copy data to filter
	for i := 0; i < datalen; i++ {
		// 计算二维数组的具体位置
		bucketIndex := (int(offset) + i) / int(filter.BucketSize)
		bucketOffset := (int(offset) + i) % int(filter.BucketSize)
		if bucketIndex < len(filter.Data) && bucketOffset < len(filter.Data[bucketIndex]) {

			filter.Data[bucketIndex][bucketOffset] = data[i]
		} else {
			// 超出界限
			return fmt.Errorf("out of range")
		}
	}
	return nil
}
func (lcf *LocalCuckooFilters) Load(ctx context.Context, key interface{}, args ...interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("args is empty")
	}
	keyStr := key.(string)
	val, ok := args[0].(source.RedisDump)
	if !ok {
		return fmt.Errorf("args[0] is not golayeredcache.RedisDump")
	}
	if val.Data == nil || len(val.Data) == 0 {
		return nil
	}
	if val.Iter == 1 {
		return lcf.loadHeader(keyStr, val.Data)
	}
	return lcf.loadDump(keyStr, val.Iter, val.Data)

}
func (lcf *LocalCuckooFilters) Del(ctx context.Context, key interface{}, args ...interface{}) error {
	lcf.mux.Lock()
	defer lcf.mux.Unlock()
	f := lcf.filters[key.(string)]
	if f == nil {
		return fmt.Errorf("not found such filter:%s", key)
	}
	for _, arg := range args {
		val, ok := arg.([]byte)
		if !ok {
			return fmt.Errorf("arg is not []byte")
		}
		f.Delete(val)
	}
	return nil
}

// DelOnLocal(ctx context.Context,key interface{},args ...interface{})error
func NewLocalCuckooFilters(filters map[string]*gocuckoo.GoCuckooFilterImpl, buildOptions *gocuckoo.CuckooBuildOptions) *LocalCuckooFilters {
	return &LocalCuckooFilters{
		filters:            filters,
		cuckooBuildOptions: buildOptions,
		mux:                sync.RWMutex{},
	}
}
