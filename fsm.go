package simpleraft

import (
	"fmt"
	"sync"
)

type FSM interface {
	// push logs to one buffered-channel and handle asyn
	ApplyLogs(logs []*Log)
	SetPrintLock(lock *sync.Mutex)
}
type MockFSM struct {
	printLock *sync.Mutex
}

func (fsm *MockFSM) SetPrintLock(lock *sync.Mutex) {
	fsm.printLock = lock
}

// dung la keep the same thing
func (fsm *MockFSM) PrintSync(s string) {
	fsm.printLock.Lock()
	fmt.Println(s)
	fsm.printLock.Lock()
}
func (fsm *MockFSM) ApplyLogs(logs []*Log) {
	/* for _, log := range logs {
		//fmt.Printf("APPLIED log %d success\n", log.Index)
	} */
}
