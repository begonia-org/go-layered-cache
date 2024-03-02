package golayeredbloom

/*
#cgo CFLAGS: -I./RedisBloom/deps/murmur2 -I./RedisBloom/deps -I./RedisBloom/src -I./RedisBloom/deps/RedisModulesSDK/rmutil
#cgo LDFLAGS: -L./lib -lbloom_filter -lm -Wl,-rpath,'$ORIGIN/lib'
#include "cuckoo.h"
#include "cf.h"
#include "sds.h"
#include "murmurhash2.h"
*/
import "C"
import (
	"encoding/hex"
	"fmt"
	"strings"
)

// static int CFLoadChunk_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     RedisModule_AutoMemory(ctx);

//     if (argc != 4) {
//         return RedisModule_WrongArity(ctx);
//     }

//     CuckooFilter *cf;
//     RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
//     int status = cfGetFilter(key, &cf);

//     // Pos, blob
//     long long pos;
//     if (RedisModule_StringToLongLong(argv[2], &pos) != REDISMODULE_OK || pos == 0) {
//         return RedisModule_ReplyWithError(ctx, "Invalid position");
//     }
//     size_t bloblen;
//     const char *blob = RedisModule_StringPtrLen(argv[3], &bloblen);

//     if (pos == 1) {
//         if (status != SB_EMPTY) {
//             return RedisModule_ReplyWithError(ctx, statusStrerror(status));
//         } else if (bloblen != sizeof(CFHeader)) {
//             return RedisModule_ReplyWithError(ctx, "Invalid header");
//         }

//         cf = CFHeader_Load((CFHeader *)blob);
//         if (cf == NULL) {
//             return RedisModule_ReplyWithError(ctx, "Couldn't create filter!");
//         }
//         RedisModule_ModuleTypeSetValue(key, CFType, cf);
//         RedisModule_ReplicateVerbatim(ctx);
//         return RedisModule_ReplyWithSimpleString(ctx, "OK");
//     }

//     if (status != SB_OK) {
//         return RedisModule_ReplyWithError(ctx, statusStrerror(status));
//     }

//	    if (CF_LoadEncodedChunk(cf, pos, blob, bloblen) != REDISMODULE_OK) {
//	        return RedisModule_ReplyWithError(ctx, "Couldn't load chunk!");
//	    }
//	    return RedisModule_ReplyWithSimpleString(ctx, "OK");
//	}
func hexStringToBytes(hexStr string) ([]byte, error) {
	// 去掉所有的"\\x"
	cleanStr := strings.Replace(hexStr, "\\x", "", -1)

	bytes, err := hex.DecodeString(cleanStr)
	if err != nil {
		panic(err)
	}
	return bytes, nil
}
func MurmurHash64ABloom(key []byte, seed uint64) uint64 {
	ckey := C.CBytes(key)
	defer C.free(ckey)

	// 调用C函数
	hash := C.MurmurHash64A_Bloom(ckey, C.int(len(key)), C.ulong(seed))
	return uint64(hash)
}
func LoadCuckooFilter() {
	ckey := C.CBytes([]byte("hello world"))
	defer C.free(ckey)

	// 调用C函数
	hash := C.MurmurHash64A_Bloom(ckey, C.int(len("hello world")), 0)
	fmt.Println(uint64(hash), ":", MurmurHash64ABloom([]byte("hello world"), 0))

	// rdb := redis.NewClient(&redis.Options{
	// 	Addr:     "localhost:6379",
	// 	Password: "",
	// 	DB:       0,
	// })
	// val, err := rdb.CFScanDump(context.Background(), "test", 1).Val().Data()
	// if err != nil {
	// 	panic(err)

	// }
	// cData := C.CString(val.Data)
	// defer C.free(unsafe.Pointer(cData)) // 转换完成后，确保释放C字符串占用的内存
	// blen := C.size_t(len(val.Data))
	// // static inline void sdssetlen(sds s, size_t newlen)
	// blob := C.sdssetlen(cData, blen)
	// //         cf = CFHeader_Load((CFHeader *)blob);
	// blobPtr := unsafe.Pointer(blob)

	// cf := C.CFHeader_Load((*C.CFHeader)(unsafe.Pointer(blobPtr)))
	// fmt.Println(cf.bucketSize)

	// valByte, err := hexStringToBytes(val.Data)
	// if err != nil {
	// 	panic(err)

	// }
	// var cfHeader C.CFHeader
	// err = json.Unmarshal(valByte, &cfHeader)
	// if err != nil {
	// 	panic(err)
	// }

	// var cf = C.CFHeader_Load(&cfHeader)
	// cValData := C.CString(val.Data)
	// defer C.free(unsafe.Pointer(cValData))
	// // int CF_LoadEncodedChunk(const CuckooFilter *cf, long long pos, const char *data, size_t datalen);
	// C.CF_LoadEncodedChunk(cf, 1, cValData, C.size_t(len(val.Data)))
	// fmt.Println(cf.bucketSize)

}
