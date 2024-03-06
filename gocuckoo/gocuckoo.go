package gocuckoo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	golayeredbloom "github.com/begonia-org/go-layered-bloom"
)

// Equivalent to `typedef uint8_t CuckooFingerprint;`
type CuckooFingerprint uint8

// Equivalent to `typedef uint64_t CuckooHash;`
type CuckooHash uint64

type CuckooBucket []uint8

// `typedef uint8_t MyCuckooBucket;` is directly equivalent to defining a new type based on uint8.
// This is a straightforward type definition in Go as well.
type MyCuckooBucket uint8

// Constants equivalent to #define macros
const (
	CF_DEFAULT_MAX_ITERATIONS = 20
	CF_DEFAULT_BUCKETSIZE     = 2
	CF_DEFAULT_EXPANSION      = 1
	CF_MAX_EXPANSION          = 32768
	CF_MAX_ITERATIONS         = 65535
	CF_MAX_BUCKET_SIZE        = 255                // 8 bits, see struct SubCF
	CF_MAX_NUM_BUCKETS        = 0x00FFFFFFFFFFFFFF // 56 bits, see struct SubCF
	CF_MAX_NUM_FILTERS        = math.MaxUint16     // 16 bits, see struct CuckooFilter

)

var (
	ErrNoSpace  = errors.New("no space")
	ErrRelocate = errors.New("relocate failed")
)

// CuckooFilter struct equivalent
type CuckooFilter struct {
}
type GoCuckooFilter interface {
	Insert(val []byte) error
	Check(val []byte) bool
	Delete(val []byte) bool
	LoadFrom(data interface{}) error
}
type RedisDump struct {
	Data string
	Iter uint64
}
type redisCFHeader struct {
	NumItems      uint64
	NumBuckets    uint64
	NumDeletes    uint64
	NumFilters    uint64
	BucketSize    uint16
	MaxIterations uint16
	Expansion     uint16
}

type GoCuckooFilterImpl struct {
	numBuckets    uint64
	numItems      uint64
	numDeletes    uint64
	numFilters    uint16
	bucketSize    uint16
	maxIterations uint16
	expansion     uint16
	filters       []*SubCF
	mux           sync.RWMutex
}
type LookupParams struct {
	H1     CuckooHash        // First hash value
	H2     CuckooHash        // Second hash value
	Fp     CuckooFingerprint // Fingerprint
	Unique bool
}
// NewGoCuckooFilter creates a new instance of a GoCuckooFilter. This filter is an implementation
// of the Cuckoo Filter algorithm, inspired by the implementation found in the RedisBloom project.
// A Cuckoo Filter is an efficient data structure for fast determination of element membership
// with a low false positive rate, particularly suitable for scenarios requiring quick lookups
// and storage for large datasets.
//
// Parameters:
// - capacity: The initial capacity of the filter, indicating the number of elements it can store.
//             The actual capacity is adjusted to the nearest power of two based on the bucketSize.
// - bucketSize: The number of elements each bucket can store. Buckets are the basic storage units
//               used within the filter.
// - maxIterations: The maximum number of iterations for an element to be kicked out of its original
//                  position and attempt to find a new position during insertion. This parameter helps
//                  prevent the insertion operation from entering an infinite loop.
// - expansion: The multiplier for filter expansion. Expansion is achieved by increasing the number
//              of buckets, and this parameter controls the degree of expansion.
//
// Returns:
// A GoCuckooFilter instance that has been initialized and is ready for element addition and lookup operations.
//
// Note:
// If the provided capacity or expansion is not a power of two, they will be adjusted to the nearest
// larger power of two. This adjustment optimizes the internal structure of the filter for efficiency.
func NewGoCuckooFilter(capacity uint64, bucketSize uint16, maxIterations uint16, expansion uint16) GoCuckooFilter {
	filter := &GoCuckooFilterImpl{
		expansion:     uint16(GetNextN2(uint64(expansion))),
		bucketSize:    bucketSize,
		maxIterations: maxIterations,
		numBuckets:    GetNextN2(capacity / uint64(bucketSize)),
		mux:           sync.RWMutex{},
	}
	filter.Grow()
	return filter
}
func NewLookupParams(hash CuckooHash) *LookupParams {
	return &LookupParams{
		Fp:     CuckooFingerprint(hash%255 + 1),
		H1:     hash,
		H2:     GetAltHash(CuckooFingerprint(hash%255+1), hash),
		Unique: true,
	}
}

// GetAltHash computes an alternative hash value based on a fingerprint and an initial hash (index).
func GetAltHash(fp CuckooFingerprint, index CuckooHash) CuckooHash {
	// Go中的类型转换需要显式进行，这里将fp转换为CuckooHash以进行计算
	return index ^ (CuckooHash(fp) * 0x5bd1e995)
}

func (filter *GoCuckooFilterImpl) Grow() {
	// Calculate the growth factor based on the expansion rate and the number of existing filters.
	growth := uint64(math.Pow(float64(filter.expansion), float64(filter.numFilters)))

	// Create a new SubCF with the calculated size.
	newFilter := &SubCF{
		BucketSize: uint8(filter.bucketSize),
		NumBuckets: filter.numBuckets * growth,
		Data:       allocateCuckooBuckets(int(filter.numBuckets*growth), int(filter.bucketSize)),
	}

	filter.filters = append(filter.filters, newFilter)

	// Increment the number of filters.
	filter.numFilters++

}

func (filter *GoCuckooFilterImpl) swapFPs(a *uint8, b *uint8) {
	*a, *b = *b, *a
}

// KOInsert attempts to insert a fingerprint into the filter, returning an error if the filter is full.
// static CuckooInsertStatus Filter_KOInsert(CuckooFilter *filter, SubCF *curFilter,
//
//	const LookupParams *params)
func (cf *GoCuckooFilterImpl) KOInsert(params *LookupParams, curFilter *SubCF) error {
	maxIterations := cf.maxIterations
	numBuckets := cf.numBuckets
	bucketSize := cf.bucketSize
	fp := uint8(params.Fp)

	var counter uint16 = 0
	var victimIx uint32 = 0
	ii := uint64(params.H1) % numBuckets

	for counter < maxIterations {
		counter++
		bucket := curFilter.Data[uint32(ii)]
		cf.swapFPs(&bucket[victimIx], &fp)
		newHash := GetAltHash(CuckooFingerprint(fp), CuckooHash(ii)) % CuckooHash(numBuckets)
		ii = uint64(newHash)
		if ok := bucket.TryInsert(CuckooFingerprint(fp)); ok {
			return nil
		}

		victimIx = (victimIx + 1) % uint32(bucketSize)
	}

	return ErrNoSpace
}

// InsertFP inserts a fingerprint into the filter, returning an error if the filter is full.
// static CuckooInsertStatus CuckooFilter_InsertFP(CuckooFilter *filter, const LookupParams *params)
func (cf *GoCuckooFilterImpl) Insert(val []byte) error {
	params := NewLookupParams(CuckooHash(golayeredbloom.MurmurHash64A(val, 0)))
	if params.Unique && cf.Check(val) {
		return nil
	}
	return cf.insert(params)
}
func (cf *GoCuckooFilterImpl) insert(params *LookupParams) error {
	cf.mux.Lock()
	defer cf.mux.Unlock()
	for ii := cf.numFilters; ii > 0; ii-- {
		slot := cf.filters[ii-1].TryInsert(params)
		if slot {
			cf.numItems++
			return nil
		}
	}

	// No space. Time to evict!
	err := cf.KOInsert(params, cf.filters[cf.numFilters-1])
	if err == nil {
		cf.numItems++
		return nil
	}

	if cf.expansion == 0 {
		return ErrNoSpace
	}

	cf.Grow()

	// Try to insert the filter again
	return cf.insert(params)
}

// Check returns true if the filter contains the given fingerprint.
// static int CuckooFilter_CheckFP(const CuckooFilter *filter, const LookupParams *params)
func (cf *GoCuckooFilterImpl) Check(val []byte) bool {
	params := NewLookupParams(CuckooHash(golayeredbloom.MurmurHash64A(val, 0)))
	cf.mux.RLock()
	defer cf.mux.RUnlock()
	for ii := 0; ii < int(cf.numFilters); ii++ {
		if cf.filters[ii].Find(params) {
			return true
		}
	}
	return false
}

// Delete removes the fingerprint from the filter, returning true if the fingerprint was found.
// int CuckooFilter_Delete(CuckooFilter *filter, CuckooHash hash)
func (cf *GoCuckooFilterImpl) Delete(val []byte) bool {
	params := NewLookupParams(CuckooHash(golayeredbloom.MurmurHash64A(val, 0)))
	cf.mux.Lock()
	defer cf.mux.Unlock()
	for _, filter := range cf.filters {
		if filter.Delete(params) {
			cf.numItems--
			cf.numDeletes++
			if cf.numFilters > 1 && cf.numDeletes > uint64(float64(cf.numItems)*0.10) {
				cf.compact(false)
			}
			return true
		}
	}
	return false
}

// relocateSlot attempts to move a slot from one bucket to another filter
// static int relocateSlot(CuckooFilter *cf, CuckooBucket bucket, uint16_t filterIx, uint64_t bucketIx, uint16_t slotIx)
func (cf *GoCuckooFilterImpl) relocateSlot(bucket CuckooBucket, filterIx uint16, bucketIx uint64, slotIx uint16) error {
	fp := bucket[slotIx]
	if fp == 0 {
		return nil
	}
	params := NewLookupParams(CuckooHash(bucketIx))
	params.Fp = CuckooFingerprint(fp)
	for ii := uint16(0); ii < filterIx; ii++ {
		slot := cf.filters[ii].TryInsert(params)
		if slot {
			bucket[slotIx] = 0
			return nil
		}
	}
	return ErrRelocate
}

// compactSingle attempts to compact a single filter by moving all of its slots down a filter.
// static uint64_t CuckooFilter_CompactSingle(CuckooFilter *cf, uint16_t filterIx)
func (cf *GoCuckooFilterImpl) compactSingle(filterIx uint16) error {
	currentFilter := cf.filters[filterIx]
	filter := currentFilter.Data
	isOk := true
	for bucketIx := uint64(0); bucketIx < currentFilter.NumBuckets; bucketIx++ {
		for slotIx := uint16(0); slotIx < uint16(currentFilter.BucketSize); slotIx++ {
			err := cf.relocateSlot(filter[bucketIx], filterIx, bucketIx, slotIx)
			if err != nil {
				isOk = false
			}
		}
	}
	// we free a filter only if it the latest one
	if isOk && (filterIx == cf.numFilters-1) {
		currentFilter.Data = currentFilter.Data[:len(currentFilter.Data)-1]
		cf.numFilters--
	}
	return nil
}

// Compact attempts to compact the filter by moving elements to older filters. If the latest filter is emptied, it is freed.
// /**
//   - Attempt to move elements to older filters. If latest filter is emptied, it is freed.
//   - `bool` determines whether to continue iteration on other filters once a filter cannot
//   - be freed and therefore following filter cannot be freed either.
//     */
//     void CuckooFilter_Compact(CuckooFilter *cf, bool cont)
func (cf *GoCuckooFilterImpl) compact(cont bool) {
	for ii := cf.numFilters; ii > 1; ii-- {
		if err := cf.compactSingle(ii - 1); err != nil && !cont {
			break
		}
	}
	cf.numDeletes = 0
}

func (cf *GoCuckooFilterImpl) LoadFrom(data interface{}) error {
	dump, ok := data.(*RedisDump)
	if !ok {
		return errors.New("invalid data type")
	}
	cf.mux.Lock()
	defer cf.mux.Unlock()
	if dump.Iter == 1 {
		header := &redisCFHeader{}

		data := RedisStringToBytes(dump.Data)
		if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, header); err != nil {
			return err
		}
		cf.loadHeader(header)
		return nil
	}
	return cf.loadDump(dump)

}

// loadHeader loads the header from a RedisModuleString and sets the filter's fields accordingly.
// 
// Base on int CF_LoadHeader(const CuckooFilter *cf, const char *data, size_t datalen)
// It will reinitialize the filter with the given header.
func (cf *GoCuckooFilterImpl) loadHeader(header *redisCFHeader) {
	cf.numBuckets = header.NumBuckets
	cf.numFilters = uint16(header.NumFilters)
	cf.numItems = header.NumItems
	cf.numDeletes = header.NumDeletes
	cf.bucketSize = header.BucketSize
	cf.maxIterations = header.MaxIterations
	cf.expansion = header.Expansion
	cf.filters = make([]*SubCF, cf.numFilters)

	for ii := 0; ii < int(cf.numFilters); ii++ {
		numBuckets := cf.numBuckets * uint64(math.Pow(float64(cf.expansion), float64(ii)))
		cf.filters[ii] = &SubCF{
			BucketSize: uint8(cf.bucketSize),
			NumBuckets: numBuckets,
			Data:       allocateCuckooBuckets(int(numBuckets), int(cf.bucketSize)),
		}
	}
}

// loadDump loads a ReddisModuleString into the filter at the given position.
// int CF_LoadEncodedChunk(const CuckooFilter *cf, long long pos, const char *data, size_t datalen)
func (cf *GoCuckooFilterImpl) loadDump(dump *RedisDump) error {
	data := RedisStringToBytes(dump.Data)
	if dump.Iter == 1 {
		return fmt.Errorf("invalid iter:%d", dump.Iter)
	}
	datalen := len(data)
	if datalen == 0 || dump.Iter <= 0 || uint64(dump.Iter-1) < uint64(datalen) {
		return fmt.Errorf("invalid data len:%d,iter:%d", datalen, dump.Iter)
	}
	offset := dump.Iter - uint64(datalen) - 1
	var currentSize uint64
	var filterIx int = 0
	var filter *SubCF
	for filterIx < int(cf.numFilters) {
		filter = cf.filters[filterIx]

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
