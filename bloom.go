package golayeredbloom

import (
	"context"
	"fmt"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type bloomFilter struct {
	*bloom.BloomFilter
}
type BloomConfig struct{
	N uint
	P float64
	Key string
}
// baseHashes returns the four hash values of data that are used to create k
// hashes
func BaseHashes(data []byte) [4]uint64 {
	var d digest128 // murmur hashing
	hash1, hash2, hash3, hash4 := d.sum256(data)
	return [4]uint64{
		hash1, hash2, hash3, hash4,
	}
}

// location returns the ith hashed location using the four base hash values
func location(h [4]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

func newBloomFilter(n uint, p float64) *bloomFilter {
	return &bloomFilter{bloom.NewWithEstimates(n, p)}
}
func (bf *bloomFilter) location(h [4]uint64, i uint) uint {
	return uint(location(h, i) % uint64(bf.Cap()))
}
func (bf *bloomFilter) Locations(data []byte) []uint {
	h := BaseHashes(data)
	locations := make([]uint, bf.K())
	for i := uint(0); i < uint(bf.K()); i++ {
		loc := bf.location(h, i)
		locations[i] = loc
	}
	return locations
}

type LayeredBloomFilter struct {
	pubsub     BloomPubSub
	filters    map[string]*bloomFilter
	mux        sync.RWMutex
	name       string
	pubsubName string
}

func NewLayeredBloomFilter(pubsub BloomPubSub, pubsubName ChannelName, name ConsumerName) *LayeredBloomFilter {
	return &LayeredBloomFilter{
		pubsub:     pubsub,
		filters:    make(map[string]*bloomFilter),
		name:       string(name),
		pubsubName: string(pubsubName),
	}
}

func (bf *LayeredBloomFilter) TestOrAdd(ctx context.Context, key string, value []byte, n uint, p float64) (bool, error) {
	bf.mux.Lock()
	defer bf.mux.Unlock()
	filter, ok := bf.filters[key]
	if !ok {
		filter = newBloomFilter(n, p)
		bf.filters[key] = filter
	}
	ok = filter.TestOrAdd(value)
	if !ok {
		locations := filter.Locations(value)
		err := bf.cache(ctx, key, locations)
		return ok, err
	}
	return ok, nil

}
func (bf *LayeredBloomFilter) Test(ctx context.Context, key string, value []byte) bool {
	filter, ok := bf.filters[key]
	if !ok {
		return false
	}
	return filter.Test(value)
}
func (bf *LayeredBloomFilter) Add(ctx context.Context, key string, value []byte, n uint, p float64) error {
	bf.mux.Lock()
	defer bf.mux.Unlock()
	filter, ok := bf.filters[key]
	if !ok {
		filter = newBloomFilter(n, p)
		bf.filters[key] = filter
	}
	ok = filter.TestOrAdd(value)
	if !ok {
		locations := filter.Locations(value)
		err := bf.cache(ctx, key, locations)
		return err
	}
	return nil
}
func (bf *LayeredBloomFilter) appendBitSet(ctx context.Context, key string, values []uint) error {
	bf.mux.Lock()
	defer bf.mux.Unlock()
	filter, ok := bf.filters[key]
	if !ok {
		return fmt.Errorf("No filter found for key: %s", key)
	}
	for _, val := range values {
		filter.BitSet().Set(val)
	}
	return nil
}
func (bf *LayeredBloomFilter) cache(ctx context.Context, key string, values []uint) error {
	message := &BloomBroadcastMessage{
		From:      bf.name,
		BloomKey:  key,
		Locations: values,
	}
	return bf.pubsub.Publish(ctx, bf.pubsubName, message)
}

func (bf *LayeredBloomFilter) watch(ctx context.Context) <-chan error {
	return bf.pubsub.Subscribe(ctx, bf.pubsubName, bf)
}
func (bf *LayeredBloomFilter) OnStart(ctx context.Context) <-chan error {
	errChan := bf.watch(ctx)
	return errChan
}

func (bf *LayeredBloomFilter) LoadFrom(ctx context.Context, keys []*BloomConfig) error {
	bf.mux.Lock()
	defer bf.mux.Unlock()
	for _, key := range keys {
		locations, err := bf.pubsub.Get(ctx, key.Key)
		if err != nil {
			return err
		}
		filter, ok := bf.filters[key.Key]
		if !ok {
			filter = newBloomFilter(key.N, key.P)
			bf.filters[key.Key] = filter
		}
		for _, val := range locations {
			filter.BitSet().Set(val)
		}
	}
	return nil
}
