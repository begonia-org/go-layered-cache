package gocuckoo

import (
	"testing"
)

func BenchmarkCuckooInsert(b *testing.B) {
	// ...
	cf := New(1000, 2, 500, 1)
	for i := 0; i < b.N; i++ {
		err := cf.Insert([]byte("item1"))
		ok:=cf.Check([]byte("item1"))
		if err != nil||!ok {
			b.Error(err)
		}
	}
}
