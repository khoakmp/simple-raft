package simpleraft

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

type follower struct {
	nextIndex   uint64
	prevLogTerm uint64
	PeerID      ServerID
	PeerAddr    string

	// buffered-channel
	notifyNewLogChan chan struct{} // main-loop send new value of LastLogIndex to each follower

	stopChan chan struct{}
	term     uint64
	numRPC   uint32
}

func (f *follower) Print() {
	buffer := bytes.NewBuffer(createBorderLine(49))

	//printBorderLine(49)
	rightColSize := 32
	//buffer.WriteString(fmt.Sprintf("| Follower ID  |%s|\n", padding(string(f.PeerID), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| Follower ID  |%s|\n", padding(string(f.PeerID), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| Follower Addr|%s|\n", padding(f.PeerAddr, rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| NextIndex    |%s|\n", padding(fmt.Sprintf("%d", f.nextIndex), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| PrevLogIndex |%s|\n", padding(fmt.Sprintf("%d", f.nextIndex-1), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| PrevLogTerm  |%s|\n", padding(fmt.Sprintf("%d", f.prevLogTerm), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| Term         |%s|\n", padding(fmt.Sprintf("%d", f.term), rightColSize, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("| Total RPC    |%s|\n", padding(fmt.Sprintf("%d", f.numRPC), rightColSize, PaddingMid)))
	fmt.Print(buffer.String())
}
func (f *follower) notifyStop() {
	close(f.stopChan)
}

func (f *follower) notifyNewLog() {
	// not need to block
	select {
	case f.notifyNewLogChan <- struct{}{}:
	default:
	}

}

type clusterCommitState struct {
	lock                   sync.Mutex
	matchIndexes           map[ServerID]uint64
	notifyCommitChangeChan chan struct{} // bufferd channel -> to not block when sending on main routine
	currentIndex           uint64

	// leaderloop close closeChan on release phase
	//closeChan chan struct{}
}

// width = 50
func (cs *clusterCommitState) Print() {
	//printBorderLine(49)
	buffer := bytes.NewBuffer(createBorderLine(49))

	buffer.WriteString(fmt.Sprintf("|%s|\n", padding("Cluster Commit State", 47, PaddingMid)))
	buffer.WriteString(fmt.Sprintf("|%s|\n", padding(fmt.Sprintf("Commit Index : %d", cs.currentIndex), 47, 2)))
	buffer.Write(createBorderLine(49))

	for serverID, idx := range cs.matchIndexes {
		buffer.WriteString(fmt.Sprintf("|%s|%s|\n",
			padding(string(serverID), 23, PaddingMid),
			padding(fmt.Sprintf("%d", idx), 23, PaddingMid)))
	}
	fmt.Print(buffer.String())
}

func (cs *clusterCommitState) InitMatchIndexes(servers []Server) {
	for _, s := range servers {
		cs.matchIndexes[s.ID] = 0
	}
}

func (cs *clusterCommitState) Update(serverID ServerID, index uint64) {
	//fmt.Printf("UPDATE lastLogIndex of %s to %d\n", serverID, index)
	cs.lock.Lock()
	cs.matchIndexes[serverID] = index
	cs.calCommitIdx()
	cs.lock.Unlock()
}

func (cs *clusterCommitState) ChangeConfig(config ClusterConfig) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	inUsed := make(map[ServerID]bool)
	for _, server := range config.Servers {
		_, ok := cs.matchIndexes[server.ID]
		if !ok {
			cs.matchIndexes[server.ID] = 0
		}
		inUsed[server.ID] = true
	}

	for serverID := range cs.matchIndexes {
		if inUsed[serverID] {
			continue
		}
		delete(cs.matchIndexes, serverID)
	}
	cs.calCommitIdx()
}

func (cs *clusterCommitState) getCurrentCommitIndex() uint64 {
	return atomic.LoadUint64(&cs.currentIndex)
}

// only be called when keeping cs.lock already
func (cs *clusterCommitState) calCommitIdx() {
	indexes := make([]uint64, 0, len(cs.matchIndexes))

	for _, val := range cs.matchIndexes {
		indexes = append(indexes, val)
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})

	mid := len(indexes) / 2
	//fmt.Println("indexes:", indexes)
	if indexes[mid] != atomic.LoadUint64(&cs.currentIndex) {
		//cs.currentIndex = indexes[mid]
		atomic.StoreUint64(&cs.currentIndex, indexes[mid])

		//fmt.Println("change cs.currentIndex to:", indexes[mid])

		select {
		case cs.notifyCommitChangeChan <- struct{}{}:
		default:
		}
	}
}

type LeaderState struct {
	term          uint64
	followers     map[ServerID]*follower
	commitState   *clusterCommitState
	inflightQueue *InflightQueue
}

func newInflightQueue() *InflightQueue {
	head := new(inflightNode)
	return &InflightQueue{
		head: head,
		tail: head,
	}
}

func newClusterCommitState(currentCommitIdx uint64) *clusterCommitState {

	return &clusterCommitState{
		matchIndexes:           make(map[ServerID]uint64),
		notifyCommitChangeChan: make(chan struct{}, 1),
		currentIndex:           currentCommitIdx,
	}
}

func (leaderState *LeaderState) stopAllReplication() {
	for _, follower := range leaderState.followers {
		close(follower.stopChan)
	}
}

func (ls *LeaderState) putInflight(logFutures []*ApplyLogReqFuture) {
	ls.inflightQueue.PushBack(logFutures)
}

func (leaderState *LeaderState) releaseInflight(index uint64, err error) {
	leaderState.inflightQueue.ReleaseUpto(index, err)

}

func (leaderState *LeaderState) releaseAllInflight(err error) {
	l := len(leaderState.inflightQueue.tail.logFutures)
	var index uint64 = 0
	if l > 0 {
		index = leaderState.inflightQueue.tail.logFutures[l-1].log.Index
		leaderState.releaseInflight(index, err)
	}
}

func (ls *LeaderState) closeCommitChan() {
	//close(ls.commitState.closeChan)
}

type InflightQueue struct {
	pushCount     uint32
	releaseCount  uint32
	allocateCount uint32
	head          *inflightNode
	tail          *inflightNode
	lock          sync.RWMutex
}

// initial , head = tail and not nil
func NewInflightQueue() *InflightQueue {
	head := new(inflightNode)
	return &InflightQueue{
		head: head,
		tail: head,
	}
}

type inflightNode struct {
	logFutures []*ApplyLogReqFuture
	next       *inflightNode
}

func (q *InflightQueue) PushBack(logFutures []*ApplyLogReqFuture) {
	q.lock.Lock()
	defer q.lock.Unlock()

	atomic.AddUint32(&q.pushCount, 1)
	newNode := &inflightNode{
		logFutures: logFutures,
		next:       nil,
	}
	q.tail.next = newNode
	if q.head.next == nil {
		q.head.next = newNode
	}
	q.tail = newNode
}

func (q *InflightQueue) GetRangeUpto(index uint64) []*Log {
	q.lock.RLock()
	defer q.lock.RUnlock()

	logs := make([]*Log, 0)
	node := q.head.next

	for node != nil {
		l := len(node.logFutures)
		for i := 0; i < l; i++ {
			if node.logFutures[i].log.Index > index {
				return logs
			}
			logs = append(logs, node.logFutures[i].log)
		}
		node = node.next
	}
	return logs
}

func (q *InflightQueue) GetRangeLogs(startIndex, endIndex uint64) []*Log {
	q.lock.RLock()
	defer q.lock.RUnlock()

	logs := make([]*Log, 0)
	node := q.head.next

	for node != nil {
		l := len(node.logFutures)
		for i := 0; i < l; i++ {
			if node.logFutures[i].log.Index > endIndex {
				return logs
			}

			if node.logFutures[i].log.Index >= startIndex {
				logs = append(logs, node.logFutures[i].log)
			}
		}
		node = node.next
	}
	return logs
}
func (q *InflightQueue) ReleaseUpto(index uint64, err error) {
	//fmt.Println("RELEASE log futures inflight upto", index)
	q.lock.Lock()
	defer q.lock.Unlock()

	node := q.head.next
	atomic.AddUint32(&q.releaseCount, 1)
	for node != nil {
		l := len(node.logFutures)
		for i := 0; i < l; i++ {
			if node.logFutures[i].log.Index > index {
				/* node.logFutures = node.logFutures[i:]
				b := []*ApplyLogReqFuture{nil} */
				atomic.AddUint32(&q.allocateCount, 1)
				sz := len(node.logFutures[i:])
				buf := logfutPool.Get(uint32(sz))

				//buf := make([]*ApplyLogReqFuture, sz)
				copy(buf, node.logFutures[i:])
				logfutPool.Put(node.logFutures)

				node.logFutures = buf

				//node.logFutures = append(make([]*ApplyLogReqFuture, 0), node.logFutures[i:]...)
				return
			}

			node.logFutures[i].respErr(err)
			node.logFutures[i] = nil
		}
		logfutPool.Put(node.logFutures)

		node = node.next
		q.head.next = node
		//fmt.Println("assign q.head.next : ", node)
	}
	//fmt.Println("RELEASE all inflight future")
	q.tail = q.head
}

func (q *InflightQueue) Print() {
	node := q.head.next
	for {
		fmt.Printf("(")
		l := len(node.logFutures)
		for i := 0; i < l; i++ {
			if i < l-1 {
				fmt.Printf("%d,", node.logFutures[i].log.Index)
			} else {
				fmt.Printf("%d)", node.logFutures[i].log.Index)
			}
		}
		node = node.next
		if node == nil {
			break
		}
		fmt.Printf(",")
	}
	fmt.Println()
}
