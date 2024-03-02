package golayeredbloom

/*
#cgo CFLAGS: -I./RedisBloom/deps/murmur2 -I./RedisBloom/deps -I./RedisBloom/src -I./RedisBloom/deps/RedisModulesSDK/rmutil
#cgo LDFLAGS: -L./lib -lbloom_filter -lm -Wl,-rpath,'$ORIGIN/lib'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cuckoo.h"
#include "cuckoo.c"
#include "cf.h"
#include "sds.h"
#include "murmurhash2.h"

 size_t get_sdshdr8_len_from_string(const sds s) {
	 fprintf(stderr,"errrsds:%s\n", s);
    return sdslen(s);
}
*/
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

type RedCuckooFilter struct {
	filters *C.CuckooFilter
}

// StringToBytes converts string to byte slice without memory allocation.
func StringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))

	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = stringHeader.Data
	byteHeader.Len = stringHeader.Len
	byteHeader.Cap = stringHeader.Len

	return b
}
func NewCuckooFilter(capacity uint, bucketSize, maxIterations, expansion uint) *RedCuckooFilter {

	cf := (*C.CuckooFilter)(C.malloc(C.sizeof_CuckooFilter))
	if cf == nil {
		return nil // 如果内存分配失败，则返回nil
	}

	if C.CuckooFilter_Init(cf, C.ulong(capacity), C.ushort(bucketSize), C.ushort(maxIterations), C.ushort(expansion)) != 0 {
		C.free(unsafe.Pointer(cf))
		cf = nil
	}
	return &RedCuckooFilter{
		filters: cf,
	}
}
func (cf *RedCuckooFilter) Release() {
	if cf.filters != nil {
		C.CuckooFilter_Free(cf.filters)
		cf.filters = nil
	}
}
func (cf *RedCuckooFilter) Insert(data []byte, isNx bool) {
	// CuckooFilter_Insert(CuckooFilter *filter, CuckooHash hash)
	hash := cf.Hash(data)
	if isNx {
		cf.insertUnique(hash)
	} else {
		cf.insert(hash)
	}

}

// insertUnique
// CuckooInsertStatus CuckooFilter_InsertUnique(CuckooFilter *filter, CuckooHash hash)
func (cf *RedCuckooFilter) insertUnique(hash uint64) {
	C.CuckooFilter_InsertUnique(cf.filters, C.CuckooHash(hash))
}

// insert
// CuckooInsertStatus CuckooFilter_Insert(CuckooFilter *filter, CuckooHash hash);
func (cf *RedCuckooFilter) insert(hash uint64) {
	C.CuckooFilter_Insert(cf.filters, C.CuckooHash(hash))
}
func (c *RedCuckooFilter) Hash(value []byte) uint64 {
	return MurmurHash64ABloom(value, 0)
}

// int CuckooFilter_Delete(CuckooFilter *filter, CuckooHash hash);
func (cf *RedCuckooFilter) Delete(data []byte) {
	hash := cf.Hash(data)
	C.CuckooFilter_Delete(cf.filters, C.CuckooHash(hash))
}

// int CuckooFilter_Check(const CuckooFilter *filter, CuckooHash hash);
func (cf *RedCuckooFilter) Check(data []byte) bool {
	hash := cf.Hash(data)
	ret := C.CuckooFilter_Check(cf.filters, C.CuckooHash(hash))
	return C.int(ret) == C.int(1)

}

func (cf *RedCuckooFilter) LoadFrom(dump string, iter int64) bool {
	// hex_string = dump.encode("utf-8").hex()
	// cData := C.CString(escapeString(dump))
	cData := C.CBytes(StringToBytes(dump))
	fmt.Printf("cData:%v,len:%d,iter:%d\n", unsafe.Sizeof(cData), len(StringToBytes(dump)), iter)
	// val := C.CString(dump)

	defer C.free(unsafe.Pointer(cData))
	if iter == 1 {
		// CuckooFilter *CFHeader_Load(const CFHeader *header)
		if cf.filters != nil {
			cf.Release()
		}
		cf.filters = C.CFHeader_Load((*C.CFHeader)(unsafe.Pointer(cData)))
		if cf.filters == nil {
			return false

		}
		return true
	}

	// // sds := C.sdsnew(cData)
	// fmt.Println("dddsds:", sds)
	// defer C.sdsfree(sds)

	// length := C.get_sdshdr8_len_from_string(sds)
	fmt.Println("iter:", unsafe.Sizeof(dump))
	ret := uint(C.CF_LoadEncodedChunk(cf.filters, C.longlong(iter), (*C.char)(cData), C.size_t(unsafe.Sizeof(cData))))
	fmt.Println("ret:", ret)
	return ret == 1
	// CF_LoadEncodedChunk(const CuckooFilter *cf, long long pos, const char *data, size_t datalen)

}

// func (cf *CuckooFilter) Dump() string {
// 	// fillCFHeader(CFHeader *header, const CuckooFilter *cf)
// 	// CFHeader header;
// 	// fillCFHeader(&header, cf);
// 	header := C.CFHeader{}
// 	C.fillCFHeader(unsafe.Pointer(&header), cf.filters)

// }
