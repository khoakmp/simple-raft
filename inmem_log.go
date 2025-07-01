package simpleraft

import (
	"errors"
	"sync"
	"sync/atomic"
)

// only use for test purpose
type InmemLogStore struct {
	data      map[uint64]*Log
	lock      sync.RWMutex
	config    *Config
	lastIndex uint64
	lastTerm  uint64
}

/*
StoreLog(log *Log) error
GetRangeLogs(startIdx, endIdx uint64) ([]*Log, error)
StoreBatchLog(logs []*Log) error
*/

func (store *InmemLogStore) StoreLog(log *Log) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	store.data[log.Index] = log
	atomic.StoreUint64(&store.lastIndex, log.Index)
	atomic.StoreUint64(&store.lastTerm, log.Term)

	return nil
}

var ErrLogNotFound = errors.New("log not found")

func (store *InmemLogStore) GetRangeLogs(startIdx, endIdx uint64) ([]*Log, error) {
	store.lock.RLock()
	defer store.lock.RUnlock()

	logs := logPtrSlicePool.Get(uint32(endIdx - startIdx + 1))

	//logs := make([]*Log, 0, endIdx-startIdx+1)

	for i := startIdx; i <= endIdx; i++ {
		l, ok := store.data[i]
		if !ok {
			return nil, ErrLogNotFound
		}
		logs[i-startIdx] = l
	}
	return logs, nil
}

func (store *InmemLogStore) StoreBatchLog(logs []*Log) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	for _, log := range logs {
		store.data[log.Index] = log
	}
	return nil
}
func (store *InmemLogStore) GetLog(index uint64) (*Log, error) {
	store.lock.RLock()
	defer store.lock.RUnlock()
	l, ok := store.data[index]
	if ok {
		return l, nil
	}
	return nil, ErrLogNotFound
}

func (store *InmemLogStore) GetLastEntry() (lastLogIndex, lastLogTerm uint64) {
	store.lock.RLock()
	defer store.lock.RUnlock()
	return atomic.LoadUint64(&store.lastIndex), atomic.LoadUint64(&store.lastTerm)
}

func (store *InmemLogStore) SetRaft(r *Raft) {}
