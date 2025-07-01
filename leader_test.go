package simpleraft

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInflighQueue(t *testing.T) {
	q := newInflightQueue()
	n, idx := int32(3), uint64(0)
	batchSize, completeCnt := int32(5), int32(7)

	successCount, failCount := int32(0), int32(0)
	var term uint64 = 1
	var wg sync.WaitGroup
	fts := []*ApplyLogReqFuture{}

	getResultFn := func(logFt *ApplyLogReqFuture, wg *sync.WaitGroup) {
		defer wg.Done()
		err := logFt.Err()
		if err == nil {
			atomic.AddInt32(&successCount, 1)
			return
		}
		atomic.AddInt32(&failCount, 1)
	}

	for i := 0; i < int(n); i++ {
		batch := []*ApplyLogReqFuture{}
		for j := 0; j < int(batchSize); j++ {
			idx++

			logFt := makeApplyLogReqFuture(&Log{
				Type:  LogNoop,
				Index: idx,
				Term:  term,
				Data:  nil,
			})
			batch = append(batch, logFt)

			if idx > uint64(completeCnt) {
				fts = append(fts, logFt)
				continue
			}
			wg.Add(1)
			go getResultFn(logFt, &wg)
		}
		q.PushBack(batch)
	}
	q.Print()
	q.ReleaseUpto(uint64(completeCnt), nil)
	wg.Wait()

	assert.Equal(t, successCount, completeCnt)

	wg = sync.WaitGroup{}
	next := int32(5)
	var i int32
	for i = 0; i < next; i++ {
		logFt := fts[i]
		wg.Add(1)
		go getResultFn(logFt, &wg)
	}
	completeCnt += int32(next)
	q.ReleaseUpto(uint64(completeCnt), ErrNotLeader)

	wg.Wait()

	assert.Equal(t, successCount, int32(7))
	assert.Equal(t, failCount, next)

	q.Print()
}

func SampleClusterCommitState() *clusterCommitState {
	state := &clusterCommitState{
		matchIndexes: make(map[ServerID]uint64),
	}
	state.matchIndexes["server-1"] = 3
	state.matchIndexes["server-2"] = 3
	state.matchIndexes["server-3"] = 1

	return state
}
func TestPrint(t *testing.T) {
	f := follower{}
	f.Print()
	s := SampleClusterCommitState()
	s.Print()
}
