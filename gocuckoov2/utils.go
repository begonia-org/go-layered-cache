package gocuckoo

import (
	"reflect"
	"unsafe"
)

// GetNextN2
// 这个函数的目的是找到大于或等于输入 n 的最小的2的幂次方数。
func GetNextN2(n uint64) uint64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

func IsPower2(num uint64) bool {
	return (num&(num-1)) == 0 && num != 0
}

func allocateCuckooBuckets(n, m int) []CuckooBucket {
	// 第一步：为[]CuckooBucket分配内存
	cuckooBuckets := make([]CuckooBucket, n)

	// 第二步：为每个CuckooBucket分配内存
	for i := range cuckooBuckets {
		cuckooBuckets[i] = make(CuckooBucket, m)
	}

	return cuckooBuckets
}

// StringToBytes converts string to byte slice without memory allocation.
func RedisStringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))

	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = stringHeader.Data
	byteHeader.Len = stringHeader.Len
	byteHeader.Cap = stringHeader.Len

	return b
}
