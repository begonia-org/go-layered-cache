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
	bloomBuildOptions *BloomBuildOptions
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
	lbf.mux.Lock()
	defer lbf.mux.Unlock()
	f, ok := lbf.filters[key.(string)]
	if !ok {
		f=NewGoBloomChain(lbf.bloomBuildOptions.Entries,lbf.bloomBuildOptions.Errors,lbf.bloomBuildOptions.BloomOptions,lbf.bloomBuildOptions.Growth)
		if f == nil {
			return fmt.Errorf("create bloom filter failed")
		}
		lbf.filters[key.(string)] = f
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



func (lbf *LocalBloomFilters) loadHeader(key string, data []byte) error {

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


func (lbf *LocalBloomFilters) loadDump(key string, iter uint64, data []byte) error {

	bufLen := len(data)
	if bufLen == 0 || iter <= 0 || int64(iter) < int64(bufLen) {
		return fmt.Errorf("received bad data")

	}
	iter -= uint64(bufLen)
	link, offset := lbf.pos(key, int64(iter))
	if link == nil {
		return fmt.Errorf("invalid offset - no link found")
	}

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

func NewLocalBloomFilters(filters map[string]*GoBloomChain,buildOptions *BloomBuildOptions) *LocalBloomFilters {
	return &LocalBloomFilters{
		filters: filters,
		bloomBuildOptions:buildOptions,
	}
}
