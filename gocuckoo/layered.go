package gocuckoo

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type LayeredCuckooFilter interface{}
type SourceCuckooFilter interface {
	GetSourceData(cxt context.Context, source interface{}) <-chan interface{}
}

type LayeredCuckooFilterInfo struct {
	localCuckooFilter GoCuckooFilter
	source            SourceCuckooFilter
	key               string
}
type SourceCuckooFilterImpl struct {
	rdb *redis.Client
}

func (scf *SourceCuckooFilterImpl) GetSourceData(cxt context.Context, source interface{}) <-chan interface{} {
	key := source.(string)
	cur := 0
	ch := make(chan interface{})
	defer close(ch)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				ch<-fmt.Errorf("context done,%w",ctx.Err())
				return
			default:
				val, err := scf.rdb.CFScanDump(cxt, key, int64(cur)).Result()
				if err != nil {
					ch <- err
					return
				}
				if val.Iter == 0 {
					return
				}
				ch <- &RedisDump{Data: val.Data, Iter: uint64(val.Iter)}
				cur = int(val.Iter)
			}

		}
	}(cxt)
	return ch

}
func (lc *LayeredCuckooFilterInfo) initFromSource(ctx context.Context) error {
	// CuckooFilter_Insert(CuckooFilter *filter, CuckooHash hash)
	sourceData := lc.source.GetSourceData(ctx, lc.key)
	for data := range sourceData {
		if err, ok := data.(error); ok {
			return err
		}
		if dump, ok := data.(*RedisDump); ok {
			err := lc.localCuckooFilter.LoadFrom(dump)
			if err != nil {
				return err
			}
		}
	}
	return nil

}
