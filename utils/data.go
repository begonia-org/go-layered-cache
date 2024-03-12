package utils

import (
	"reflect"
	"unsafe"
)

func RedisStringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))

	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = stringHeader.Data
	byteHeader.Len = stringHeader.Len
	byteHeader.Cap = stringHeader.Len

	return b
}
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
