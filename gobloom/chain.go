package gobloom

import (
	"sync"
	"sync/atomic"
)

/** A chain of one or more bloom filters */
type GoBloomChain struct {
	filters  []GoBloomFilter
	capacity uint64
	nfilters uint64
	// 容错误率
	options GoBloomOptions
	growth  uint8
	mux     sync.RWMutex
}

// NewGoBloomChain 创建一个新的布隆过滤器链
// 在布隆过滤器中，扩容时调整容错率（错误率，`error_rate`）乘以一个因子（在这个例子中是`0.5`，即`ERROR_TIGHTENING_RATIO`）是为了提高随后添加的过滤器链环节的精确度，并且减少误判的概率。这种做法基于以下考虑：
//
// 1. **过滤器链的性能保障**：随着数据量的增加，单一布隆过滤器由于其固定的大小和错误率，可能会遇到容量瓶颈，导致误判率上升。通过添加新的过滤器并降低其错误率，可以在整个过滤器链中平衡误判率，从而在容量增加的同时，保持或提高整体性能。
//
// 2. **误判率的累积影响**：布隆过滤器链是由多个布隆过滤器组成的，每个过滤器有自己的错误率。随着链的增长，如果每个过滤器保持相同的错误率，则整体的误判概率会逐步增加。通过降低新添加的过滤器的错误率，可以减少随着链扩展而导致的误判率增加的累积效应。
//
// 3. **扩容策略的权衡**：扩容时降低错误率是在空间效率和准确度之间的一种权衡。虽然降低错误率会导致单个过滤器需要更多的位来存储，增加了空间复杂度，但这可以有效减少整个过滤器链的总体误判率，特别是在数据规模不断增长的情况下。
//
// 4. **提高后续过滤器的效率**：随着数据的增长和过滤器链的扩展，新加入的数据项相对于整个数据集的比例逐渐减小。通过减少新过滤器的错误率，可以为新增的数据项提供更高的过滤精度，同时减少对已有数据项的影响。
//
// 综上所述，扩容时降低容错率是为了保持或提高整个布隆过滤器链的准确性和效率，尤其是在处理大规模数据集时，这种策略能够有效控制误判率的增长，保证过滤器链的长期性能和稳定性。
func NewGoBloomChain(initsize uint64, errorRate float64, options GoBloomOptions, growth uint8) *GoBloomChain {
	if initsize == 0 || errorRate == 0 || errorRate >= 1 {
		return nil
	}
	sb := &GoBloomChain{
		mux: sync.RWMutex{},
	}
	sb.growth = growth
	sb.options = options
	// tightening：该层错误率（第一层的错误率 = BloomFilter初始化时设置的错误率 * 0.5，第二层为第一层的0.5倍，以此类推，ratio与expansion无关;
	tightening := 0.5
	if options&BLOOM_OPT_NO_SCALING != 0 {
		tightening = 1
	}
	sb.AddLink(initsize, errorRate*tightening)
	return sb
}

func (bc *GoBloomChain) AddLink(size uint64, errorRate float64) {
	buildOption := &BloomBuildOptions{
		Entries: size,

		Errors:       errorRate,
		BloomOptions: bc.options,
		Growth:       bc.growth,
	}
	bc.filters = append(bc.filters, NewGoBloom(buildOption))
	bc.nfilters++
}

func (bc *GoBloomChain) Test(data []byte) bool {
	bc.mux.RLock()
	defer bc.mux.RUnlock()
	for i := int64(bc.nfilters - 1); i >= 0; i-- {
		if bc.filters[i].Test(data) {
			return true
		}
	}
	return false
}
func (bc *GoBloomChain) Add(data []byte) bool {
	if bc.Test(data) {
		return false
	}
	bc.mux.Lock()
	defer bc.mux.Unlock()
	cur := bc.filters[bc.nfilters-1]
	if cur.GetItems() >= cur.GetEntries() {
		// 如果不允许扩容，直接返回
		if bc.options&BLOOM_OPT_NO_SCALING != 0 {
			return false
		}
		errors := cur.GetErrorRate() * 0.5
		bc.AddLink(cur.GetEntries()*uint64(bc.growth), errors)
		cur = bc.filters[bc.nfilters-1]
	}
	rv := cur.Add(data)
	if rv {
		bc.capacity++
	}
	return rv
}

// func (bc *GoBloomChain) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error){

// }
// func (bc *GoBloomChain) Set(ctx context.Context, key interface{}, args ...interface{}) error
// func (bc *GoBloomChain) OnMessage(ctx context.Context, from interface{}, message redis.XMessage) error

/**
 * BF.LOADCHUNK <KEY> <ITER> <DATA>
 * Incrementally loads a bloom filter.
 */
//  static int BFLoadChunk_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     RedisModule_AutoMemory(ctx);

//     if (argc != 4) {
//         return RedisModule_WrongArity(ctx);
//     }

//     long long iter;
//     if (RedisModule_StringToLongLong(argv[2], &iter) != REDISMODULE_OK) {
//         return RedisModule_ReplyWithError(ctx, "ERR Second argument must be numeric");
//     }

//     size_t bufLen;
//     const char *buf = RedisModule_StringPtrLen(argv[3], &bufLen);

//     RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
//     SBChain *sb;
//     int status = bfGetChain(key, &sb);
//     if (status == SB_EMPTY && iter == 1) {
//         const char *errmsg;
//         SBChain *sb = SB_NewChainFromHeader(buf, bufLen, &errmsg);
//         if (!sb) {
//             return RedisModule_ReplyWithError(ctx, errmsg);
//         } else {
//             RedisModule_ModuleTypeSetValue(key, BFType, sb);
//             RedisModule_ReplicateVerbatim(ctx);
//             return RedisModule_ReplyWithSimpleString(ctx, "OK");
//         }
//     } else if (status != SB_OK) {
//         return RedisModule_ReplyWithError(ctx, statusStrerror(status));
//     }

//     assert(sb);

//     const char *errMsg;
//     if (SBChain_LoadEncodedChunk(sb, iter, buf, bufLen, &errMsg) != 0) {
//         return RedisModule_ReplyWithError(ctx, errMsg);
//     } else {
//         RedisModule_ReplicateVerbatim(ctx); // Should be replicated?
//         return RedisModule_ReplyWithSimpleString(ctx, "OK");
//     }
// }

// func (bc *GoBloomChain) LoadFromRedisDump(iter int, dump []byte) error {
// 	return bc.filters[0].LoadFromRedisDump(dump)
// }

func (bc *GoBloomChain) GetItems() uint64 {
	return bc.capacity
}

func (bc *GoBloomChain) GetNumberFilters() uint64 {
	return atomic.LoadUint64(&bc.nfilters)
}

func (bc *GoBloomChain) GetFilter(index int) GoBloomFilter {
	return bc.filters[index]
}
