package gobloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	golayeredcache "github.com/begonia-org/go-layered-cache"
)

type LocalBloomFilters struct {
	filters map[string]*GoBloomChain
	mux     sync.RWMutex
}

func (lbf *LocalBloomFilters) Get(ctx context.Context, key interface{}, args ...interface{}) ([]interface{}, error) {
	lbf.mux.RLock()
	defer lbf.mux.RUnlock()
	f, ok := lbf.filters[key.(string)]
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
		result[i] = f.Test(val)
	}
	return result, nil
}
func (lbf *LocalBloomFilters) Set(ctx context.Context, key interface{}, args ...interface{}) error {
	lbf.mux.RLock()
	defer lbf.mux.RUnlock()
	f, ok := lbf.filters[key.(string)]
	if !ok {
		return fmt.Errorf("not found such bloom filter %s", key)
	}
	for _, arg := range args {
		f.Add(arg.([]byte))
	}
	return nil
}
func (lbf *LocalBloomFilters) AddFilter(key string, filter *GoBloomChain) {
	lbf.mux.Lock()
	defer lbf.mux.Unlock()
	lbf.filters[key] = filter
}

// SBChain *SB_NewChainFromHeader(const char *buf, size_t bufLen, const char **errmsg) {
//     const dumpedChainHeader *header = (const void *)buf;
//     if (bufLen < sizeof(dumpedChainHeader)) {
//         *errmsg = "ERR received bad data"; // LCOV_EXCL_LINE
//         return NULL;                       // LCOV_EXCL_LINE
//     }

//     if (bufLen != sizeof(*header) + (sizeof(header->links[0]) * header->nfilters)) {
//         *errmsg = "ERR received bad data"; // LCOV_EXCL_LINE
//         return NULL;                       // LCOV_EXCL_LINE
//     }

//     SBChain *sb = RedisModule_Calloc(1, sizeof(*sb));
//     sb->filters = RedisModule_Calloc(header->nfilters, sizeof(*sb->filters));
//     sb->nfilters = header->nfilters;
//     sb->options = header->options;
//     sb->size = header->size;
//     sb->growth = header->growth;

//     for (size_t ii = 0; ii < header->nfilters; ++ii) {
//         SBLink *dstlink = sb->filters + ii;
//         const dumpedChainLink *srclink = header->links + ii;
// #define X(encfld, dstfld) dstfld = encfld;
//         X_ENCODED_LINK(X, srclink, dstlink)
// #undef X

//         if (bloom_validate_integrity(&dstlink->inner) != 0) {
//             SBChain_Free(sb);
//             *errmsg = "ERR received bad data";
//             return NULL;
//         }

//         dstlink->inner.bf = RedisModule_Calloc(1, dstlink->inner.bytes);
//         if (sb->options & BLOOM_OPT_FORCE64) {
//             dstlink->inner.force64 = 1;
//         }
//     }

//     if (SB_ValidateIntegrity(sb) != 0) {
//         SBChain_Free(sb);
//         *errmsg = "ERR received bad data";
//         return NULL;
//     }

//     return sb;
// }

func (lbf *LocalBloomFilters) loadHeader(key string, data []byte) error {
	// file, _ := os.Create("/data/work/begonia/go-layered-bloom/gobloom/testdata/header.bin")

	// defer file.Close() // 确保在函数返回前关闭文件

	// // 将数据写入文件
	// _, _ = file.Write(data)
	header := dumpedChainHeader{}
	headerSize := binary.Size(dumpedChainHeader{})
	if len(data) < headerSize {
		return fmt.Errorf("data too short to contain a dumpedChainHeader")
	}

	headerReader := bytes.NewReader(data[:headerSize])
	err := binary.Read(headerReader, binary.LittleEndian, &header)
	if err != nil {
		return err
	}

	linksData := data[headerSize:]
	linkSize := binary.Size(dumpedChainLink{})
	linkCount := int(header.Nfilters) // 假设Nfilters表示links数组的长度
	if len(linksData) < linkCount*linkSize {
		return fmt.Errorf("data too short to contain all dumpedChainLinks")
	}

	links := make([]dumpedChainLink, linkCount)
	for i := 0; i < linkCount; i++ {
		linkReader := bytes.NewReader(linksData[i*linkSize : (i+1)*linkSize])
		err = binary.Read(linkReader, binary.LittleEndian, &links[i])
		if err != nil {
			return err
		}
	}
	firstLink := links[0]
	// log.Printf("firstLink.Entries:%v,firstLink.Error:%f,header.Options:%d\n", firstLink.Entries, firstLink.Error, header.Options)
	bc := NewGoBloomChain(firstLink.Entries, firstLink.Error*2, GoBloomOptions(header.Options), uint8(header.Growth))
	for _, link := range links[1:] {
		bc.addLink(link.Entries, link.Error*2)
	}
	lbf.AddFilter(key, bc)
	return nil
}

// static SBLink *getLinkPos(const SBChain *sb, long long curIter, size_t *offset) {
//     if (curIter < 1) {
//         return NULL;
//     }

//     curIter--;
//     SBLink *link = NULL;

//     // Read iterator
//     size_t seekPos = 0;

//     for (size_t ii = 0; ii < sb->nfilters; ++ii) {
//         if (seekPos + sb->filters[ii].inner.bytes > curIter) {
//             link = sb->filters + ii;
//             break;
//         } else {
//             seekPos += sb->filters[ii].inner.bytes;
//         }
//     }
//     if (!link) {
//         return NULL;
//     }

//	    curIter -= seekPos;
//	    *offset = curIter;
//	    return link;
//	}
func (lbf *LocalBloomFilters) pos(key string, iter int64) (GoBloomFilter, uint64) {
	lbf.mux.RLock()
	defer lbf.mux.RUnlock()
	f, ok := lbf.filters[key]
	if !ok {
		return nil, 0
	}
	if iter < 1 {
		return nil, 0
	}
	curIter := int(iter - 1)
	seekPos := 0
	var link GoBloomFilter
	for i := 0; i < int(f.nfilters); i++ {
		if seekPos+int(f.filters[i].GetBytesNumber()) > curIter {
			link = f.filters[i]
		} else {
			seekPos += int(f.filters[i].GetBytesNumber())
		}
	}
	if link == nil {
		return nil, 0

	}
	curIter -= seekPos
	return link, uint64(curIter)
}

// int SBChain_LoadEncodedChunk(SBChain *sb, long long iter, const char *buf, size_t bufLen,
// 	const char **errmsg) {
// if (!buf || iter <= 0 || iter < bufLen) {
// *errmsg = "ERR received bad data";
// return -1;
// }
// // Load the chunk
// size_t offset;
// iter -= bufLen;

// SBLink *link = getLinkPos(sb, iter, &offset);
// if (!link) {
// *errmsg = "ERR invalid offset - no link found"; // LCOV_EXCL_LINE
// return -1;                                      // LCOV_EXCL_LINE
// }

// if (bufLen > link->inner.bytes - offset) {
// *errmsg = "ERR invalid chunk - Too big for current filter"; // LCOV_EXCL_LINE
// return -1;                                                  // LCOV_EXCL_LINE
// }

// // printf("Copying to %p. Offset=%lu, Len=%lu\n", link, offset, bufLen);
// memcpy(link->inner.bf + offset, buf, bufLen);
// return 0;
// }
func (lbf *LocalBloomFilters) loadDump(key string, iter uint64, data []byte) error {
	// file, _ := os.Create("/data/work/begonia/go-layered-bloom/gobloom/testdata/bloom.bin")

	// defer file.Close() // 确保在函数返回前关闭文件

	// // 将数据写入文件
	// _, _ = file.Write(data)
	bufLen := len(data)
	if bufLen == 0 || iter <= 0 || int64(iter) < int64(bufLen) {
		return fmt.Errorf("received bad data")

	}
	iter -= uint64(bufLen)
	link, offset := lbf.pos(key, int64(iter))
	if link == nil {
		return fmt.Errorf("invalid offset - no link found")
	}
	// for i:=0;i<bufLen;i++{
	// 	link.SetBit(offset+uint64(i),uint8(data[i]))
	// }
	return link.LoadBytes(offset, data)
}
func (lbf *LocalBloomFilters) Load(ctx context.Context, key interface{}, args ...interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("args is empty")
	}
	keyStr := key.(string)
	val, ok := args[0].(golayeredcache.RedisDump)
	if !ok {
		return fmt.Errorf("args[0] is not golayeredcache.RedisDump")
	}
	if val.Data == nil || len(val.Data) == 0 {
		return nil
	}
	if val.Iter == 1 {
		return lbf.loadHeader(keyStr, val.Data)
	}
	return lbf.loadDump(keyStr, val.Iter, val.Data)

}

func NewLocalBloomFilters(filters map[string]*GoBloomChain) *LocalBloomFilters {
	return &LocalBloomFilters{
		filters: filters,
	}
}
