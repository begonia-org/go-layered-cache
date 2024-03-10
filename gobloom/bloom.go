package gobloom

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	golayeredcache "github.com/begonia-org/go-layered-cache"
)

// // accurately.
// #define BLOOM_OPT_NOROUND 1

// // Entries is actually the number of bits, not the number of entries to reserve
// #define BLOOM_OPT_ENTS_IS_BITS 2

// // Always force 64 bit hashing, even if too small
// #define BLOOM_OPT_FORCE64 4

// // Disable auto-scaling. Saves memory
// #define BLOOM_OPT_NO_SCALING 8
type GoBloomOptions uint8

const (
	BLOOM_OPT_NOROUND GoBloomOptions = 1
	// Entries is actually the number of bits, not the number of entries to reserve
	BLOOM_OPT_ENTS_IS_BITS GoBloomOptions = 2
	BLOOM_OPT_FORCE64      GoBloomOptions = 4
	BLOOM_OPT_NO_SCALING   GoBloomOptions = 8
)

type GoBloomFilter interface {
	Test(data []byte) bool
	Add(data []byte) bool
	Reset()
	SetBit(index uint64, value uint8)
	LoadBytes(offset uint64, data []byte) error
	// TestOrAdd(data []byte) bool
	GetItems() uint64
	GetEntries() uint64
	GetBytesNumber() uint64
	GetErrorRate() float64
}

type baseHash struct {
	a uint64
	b uint64
}
type GoBloomFilterImpl struct {
	// 预估的元素个数
	entries uint64
	// 数组的大小字节单位
	bytes uint64
	// 数组的大小bit单位
	bits uint64
	// 实际存储的元素个数
	items uint64
	// 计算位置时的模
	mod uint64
	// 容错误率
	errorRate float64
	// 每个元素占用的bit数
	bpe float64
	// 哈希函数的个数
	hashes uint32
	// bit数组
	buf []byte
	// 是否强制使用64位哈希函数
	force64 bool
	// 存储的元素个数的2次幂
	n2  uint8
	mux sync.RWMutex
}

//	typedef struct __attribute__((packed)) {
//	    uint64_t bytes;
//	    uint64_t bits;
//	    uint64_t size;
//	    double error;
//	    double bpe;
//	    uint32_t hashes;
//	    uint64_t entries;
//	    uint8_t n2;
//	} dumpedChainLink;
type dumpedChainLink struct {
	Bytes   uint64
	Bits    uint64
	Size    uint64
	Error   float64
	Bpe     float64
	Hashes  uint32
	Entries uint64
	N2      uint8
}

// 对应于C中的dumpedChainHeader结构体，注意links字段在Go中的处理
type dumpedChainHeader struct {
	Size     uint64
	Nfilters uint32
	Options  uint32
	Growth   uint32
	// Links    []dumpedChainLink // 在Go中，这样的动态数组需要特别处理
}
type BloomBuildOptions struct {
	Entries      uint64
	Errors       float64
	BloomOptions GoBloomOptions
	Growth       uint8
}
// DefaultBuildBloomOptions is the default options for building a new bloom filter.
// The options values same as the redisbloom C version.
var DefaultBuildBloomOptions = &BloomBuildOptions{
	Entries:      100,
	Errors:       0.01,
	BloomOptions: BLOOM_OPT_NOROUND | BLOOM_OPT_FORCE64 | BLOOM_OPT_NO_SCALING,
	Growth:       2,
}
func NewGoBloom(buildOptions *BloomBuildOptions) *GoBloomFilterImpl {
	entries:=buildOptions.Entries
	errorRate:=buildOptions.Errors
	options:=buildOptions.BloomOptions
	if entries < 1 || errorRate <= 0 || errorRate >= 1.0 {
		return nil
	}
	//  tightening := (options & BLOOM_OPT_NO_SCALING) ? 1 : ERROR_TIGHTENING_RATIO;
	tightening := 1.00
	if options&BLOOM_OPT_NO_SCALING != 0 {
		tightening = 0.5
	}
	errorRate = errorRate * tightening
	bpe := calcBpe(errorRate)
	var bits uint64
	var n2 uint8
	// 预估数据量作为存储空间的大小
	if options&BLOOM_OPT_ENTS_IS_BITS != 0 {
		if entries > 64 {
			return nil
		}
		n2 = uint8(entries)
		bits = 1 << n2
		entries = bits / uint64(bpe)
	} else if options&BLOOM_OPT_NOROUND != 0 {
		// 不进行任何舍入。节省内存
		bits = uint64(float64(entries) * bpe)
		if bits == 0 {
			bits = 1
		}
		n2 = 0
	} else {
		bn2 := math.Log2(float64(float64(entries) * bpe))
		if bn2 > 63 || bn2 == math.Inf(1) {
			return nil
		}
		n2 = uint8(bn2 + 1)
		bits = 1 << n2
		// 确定更多项目的额外位数。我们将位数舍入到最接近的2的幂。这意味着我们可能有多达2倍的位可用。
		bitDiff := bits - (entries * uint64(bpe))
		// 可以存储的额外项目数是额外位数除以每个元素的位数
		itemDiff := bitDiff / uint64(bpe)
		// 更新实际的元素个数
		entries += itemDiff
	}
	// 将位数转换为字节数
	var bytes uint64
	if bits%64 != 0 {
		bytes = ((bits / 64) + 1) * 8
	} else {
		bytes = bits / 8
	}
	bits = bytes * 8

	// 计算哈希函数的个数
	hashes := uint32(math.Ceil(math.Ln2 * bpe))
	mod := bits
	if n2 > 0 {
		mod = 1 << n2
	}
	// fmt.Printf("entries:%v,bytes:%v,bits:%v,errorRate:%v,bpe:%v,hashes:%v,n2:%d\n", entries, bytes, bits, errorRate, bpe, hashes, n2)
	return &GoBloomFilterImpl{
		entries:   entries,
		bytes:     bytes,
		bits:      bits,
		errorRate: errorRate,
		bpe:       bpe,
		hashes:    hashes,
		buf:       make([]byte, bytes),
		force64:   options&BLOOM_OPT_FORCE64 != 0,
		n2:        n2,
		mod:       mod,
		items:     0,
		mux:       sync.RWMutex{},
	}
}

func (g *GoBloomFilterImpl) Hash64(data []byte) baseHash {
	a := golayeredcache.MurmurHash64A(data, 0xc6a4a7935bd1e995)
	b := golayeredcache.MurmurHash64A(data, a)
	return baseHash{a, b}
}

func (g *GoBloomFilterImpl) Test(data []byte) bool {
	g.mux.RLock()
	defer g.mux.RUnlock()
	hashval := g.Hash64(data)

	// fmt.Printf("hashval:%v\n", hashval)
	for i := uint64(0); i < uint64(g.hashes); i++ {
		byteIndex, mask := g.getByteAndMask(hashval, i)
		c := g.buf[byteIndex] // 读取内存
		if c&mask != 0 {
			return true
		}

	}
	return false
}
func (g *GoBloomFilterImpl) getByteAndMask(hashval baseHash, index uint64) (byteIndex uint64, mask uint8) {

	x := (hashval.a + index*hashval.b) % g.mod
	byteIndex = x >> 3  // 计算字节索引
	mask = 1 << (x % 8) // 计算掩码
	// fmt.Printf("index:%d,byteIndex:%v,mask:%v,mod:%d,x:%d,sum:%d\n", index, byteIndex, mask, g.mod, x, (hashval.a + index*hashval.b))
	return
}
func (g *GoBloomFilterImpl) Add(data []byte) bool {
	if g.Test(data) {
		return false

	}
	g.mux.Lock()
	defer g.mux.Unlock()
	hashval := g.Hash64(data)
	// fmt.Printf("hashes:%v\n", g.hashes)
	for i := uint64(0); i < uint64(g.hashes); i++ {
		byteIndex, mask := g.getByteAndMask(hashval, i)
		c := g.buf[byteIndex]
		// fmt.Printf("byteIndex:%v,mask:%v\n", byteIndex, c|mask)
		g.buf[byteIndex] = c | mask
	}
	atomic.AddUint64(&g.items, 1)
	return true
}

func (g *GoBloomFilterImpl) Reset() {
	g.buf = make([]byte, g.bytes)
}
func (g *GoBloomFilterImpl) TestOrAdd(data []byte) bool {
	if g.Test(data) {
		return true
	}
	g.Add(data)
	return false
}
func (g *GoBloomFilterImpl) SetBit(index uint64, value uint8) {
	g.mux.Lock()
	defer g.mux.Unlock()
	g.buf[index] = value
}
func (g *GoBloomFilterImpl) LoadBytes(offset uint64, data []byte) error {
	g.mux.Lock()
	defer g.mux.Unlock()
	if offset > g.bytes {
		return fmt.Errorf("offset out of range")
	}
	// fmt.Printf("offset:%v,bytes:%v\n", offset, g.bytes)

	copy(g.buf[offset:], data)
	return nil

}
func (g *GoBloomFilterImpl) GetItems() uint64 {
	return atomic.LoadUint64(&g.items)
}

func (g *GoBloomFilterImpl) GetEntries() uint64 {
	return g.entries
}

func (g *GoBloomFilterImpl) GetErrorRate() float64 {
	return g.errorRate
}

func (g *GoBloomFilterImpl) GetBytesNumber() uint64 {
	return g.bytes
}
