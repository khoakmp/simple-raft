package simpleraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

type RaftRole int

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader    = 2
)

type RaftBaseState struct {
	currentTerm      uint64
	newTerm          uint64
	lock             sync.RWMutex
	lastLogIndex     uint64
	lastLogTerm      uint64
	commitedIndex    uint64
	lastAppliedIndex uint64
	roleState        RaftRole
	startIndex       uint64
}

func (state *RaftBaseState) PrintBaseState(id ServerID) {
	buffer := bytes.NewBuffer(nil)

	buffer.WriteString(fmt.Sprintf("BaseState %s\n", id))

	//buffer.WriteString(fmt.Sprintf("CurrentTerm :%d\n", state.currentTerm))
	buffer.WriteString(fmt.Sprintf("LastLogIndex:%d\n", state.lastLogIndex))
	buffer.WriteString(fmt.Sprintf("LastLogTerm :%d\n", state.lastLogTerm))
	buffer.WriteString(fmt.Sprintf("CommitIndex :%d\n", state.commitedIndex))

	fmt.Print(buffer.String())
}

/* func (state *RaftBaseState) restoreBaseState(config *Config) {
	// TODO: may read it from disk
} */

func (state *RaftBaseState) changeRoleState(role RaftRole) {
	// only change state in main-loop goroutine
	state.roleState = role
}

func (state *RaftBaseState) getLastIndex() uint64 {
	return atomic.LoadUint64(&state.lastLogIndex)
}

func (state *RaftBaseState) setLastEntry(index, term uint64) {
	state.lock.Lock()
	atomic.StoreUint64(&state.lastLogIndex, index)
	atomic.StoreUint64(&state.lastLogTerm, term)
	state.lock.Unlock()
}
func (state *RaftBaseState) getLastEntry() (lastIndex, lastTerm uint64) {
	state.lock.RLock()
	lastIndex = atomic.LoadUint64(&state.lastLogIndex)
	lastTerm = atomic.LoadUint64(&state.lastLogTerm)
	state.lock.RUnlock()
	return
}

// only call in main-loop goroutine
func (state *RaftBaseState) setLastIndex(index uint64) {
	// use lock to ensure no other goroutine (replicate goroutine if in case leader state)
	// read when updating these values
	state.lock.Lock()
	// use atomic here because it can be read from replicate goroutines
	// without lock but just using operator
	atomic.StoreUint64(&state.lastLogIndex, index)
	atomic.StoreUint64(&state.lastLogTerm, state.currentTerm)

	state.lock.Unlock()
}

func (state *RaftBaseState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&state.commitedIndex)
}

func (state *RaftBaseState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&state.commitedIndex, index)
}

func (state *RaftBaseState) getLastAppliedIndex() uint64 {
	return atomic.LoadUint64(&state.lastAppliedIndex)
}

func (state *RaftBaseState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&state.currentTerm)
}

func (state *RaftBaseState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&state.currentTerm, term)
}

func (state *RaftBaseState) getNewTerm() uint64 {
	return atomic.LoadUint64(&state.newTerm)
}

func (state *RaftBaseState) setNewTerm(term uint64) {
	state.lock.Lock()
	defer state.lock.Unlock()
	nterm := atomic.LoadUint64(&state.newTerm)
	if term > nterm {
		atomic.StoreUint64(&state.newTerm, term)
	}
}
