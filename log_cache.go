package simpleraft

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

type LogCache struct {
	lock   sync.RWMutex
	config *Config
	logs   []*Log
}

func NewLogCache(config *Config) *LogCache {
	return &LogCache{
		config: config,
		logs:   make([]*Log, 0),
	}
}
func (cache *LogCache) PrintMetadata() {
	buffer := bytes.NewBuffer(nil)
	l := len(cache.logs)

	buffer.WriteString(fmt.Sprintf("len:%d\n", len(cache.logs)))
	buffer.WriteString(fmt.Sprintf("cap:%d\n", cap(cache.logs)))
	if l > 0 {
		buffer.WriteString(fmt.Sprintf("range:[%d:%d]\n", cache.logs[0].Index, cache.logs[l-1].Index))
	}
	fmt.Print(buffer.String())
}

var ErrExceedMaxLog = errors.New("exceed max logs in cache")

func (cache *LogCache) AddBatch(entries []*Log) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	if len(entries) > cache.config.MaxLogEntryInCache {
		return ErrExceedMaxLog
	}

	nsize := len(entries) + len(cache.logs)
	if nsize <= cap(cache.logs) {
		cache.logs = append(cache.logs, entries...)
		return nil
	}

	if nsize > cache.config.MaxLogEntryInCache {
		nsize = cache.config.MaxLogEntryInCache
	}

	buf := logPtrSlicePool.Get(uint32(nsize))
	//buf := make([]*Log, nsize)
	r := nsize - len(entries)

	del := len(cache.logs) - r
	copy(buf, cache.logs[del:])
	copy(buf[r:], entries)

	if len(cache.logs) > 0 {
		logPtrSlicePool.Put(cache.logs)
	}

	cache.logs = buf

	return nil
}

func (cache *LogCache) RemoveTo(index uint64) {
	if len(cache.logs) == 0 {
		return
	}
	firstIdx := cache.logs[0].Index
	i := index - firstIdx
	if int(i) >= len(cache.logs) {
		cache.logs = cache.logs[:0]
	}
	copy(cache.logs[0:], cache.logs[i:])
	cache.logs = cache.logs[:len(cache.logs)-int(i)]
}

func (cache *LogCache) GetRange(startIdx, endIdx uint64) []*Log {

	cache.lock.RLock()
	defer cache.lock.RUnlock()
	//fmt.Printf("Get from log cache range[%d:%d]\n", startIdx, endIdx)

	if len(cache.logs) == 0 {
		return nil
	}

	if endIdx < cache.logs[0].Index || startIdx > cache.logs[0].Index {
		return nil
	}
	firstPos := 0
	endPos := (endIdx - cache.logs[0].Index)
	if endPos >= uint64(len(cache.logs)) {
		endPos = uint64(len(cache.logs)) - 1
	}

	if startIdx > cache.logs[0].Index {
		firstPos = int(uint64(startIdx) - cache.logs[0].Index)
	}

	//buf := logslicePool.Get(uint32(endPos - uint64(firstPos) + 1))
	//fmt.Println("endPos:", endPos, "firstPos:", firstPos)

	buf := make([]*Log, endPos-uint64(firstPos)+1)
	copy(buf, cache.logs[firstPos:endPos+1])
	return buf
}

func (cache *LogCache) GetLastEntry() (index, term uint64) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if len(cache.logs) == 0 {
		return 0, 0
	}
	l := len(cache.logs)
	return cache.logs[l-1].Index, cache.logs[l-1].Term
}
