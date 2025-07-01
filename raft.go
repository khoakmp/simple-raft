package simpleraft

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

type Raft struct {
	RaftBaseState
	config            *Config
	SelfID            ServerID
	SelfAddr          string
	clusterState      *ClusterConfigState // can be accessed in only main-loop
	initialCluster    ClusterConfig
	initFlag          bool
	leaderState       *LeaderState
	rpcChan           <-chan *RPC
	applyLogChan      chan *ApplyLogReqFuture
	logStore          LogStore
	fsm               FSM
	updateClusterChan chan *UpdateClusterFuture
	newTermChan       chan struct{}
	lastTouch         time.Time // use for follower state
	rpcTrans          RPCTransport
	wg                sync.WaitGroup
	exitFlag          bool
	lastVote          atomic.Pointer[VoteStored]
}

type VoteStored struct {
	Term        uint64   `yaml:"term"`
	CandidateID ServerID `yaml:"candidate_id"`
}

func sampleServers() []Server {
	return []Server{
		{ID: "server-1", Addr: "localhost:5051"},
		{ID: "server-2", Addr: "localhost:5052"},
		{ID: "server-3", Addr: "localhost:5053"},
	}
}
func CreateRaft() {
	config := Config{
		ServerID:     "server-1",
		ServerAddr:   "127.0.0.1:5051",
		LogBatchSize: 20,
	}

	logStore := InmemLogStore{
		data: make(map[uint64]*Log),
	}

	fsm := MockFSM{}
	transport := MockTransport{}
	raft := NewRaft(&config, &logStore, &fsm, &transport)

	servers := sampleServers()
	ft, _ := raft.InitCluster(servers)
	//assert.Equal(t, err, nil)
	ft.Err()

	//assert.Equal(t, err, nil)
	raft.PrintClusterState()
	fut := raft.ApplyCommand(Command{
		Data: []byte("this is command"),
	})
	err := fut.Err()
	fmt.Println("err:", err)
	raft.Wait()
}

func (r *Raft) PrintClusterState() {
	r.clusterState.Print()
}

func NewRaft(config *Config, logStore LogStore, fsm FSM, transport RPCTransport) *Raft {
	// TODO: consider update some channel to buffered channel
	raft := &Raft{
		config:   config,
		SelfID:   config.ServerID,
		SelfAddr: config.ServerAddr,
		RaftBaseState: RaftBaseState{
			currentTerm:      0,
			lock:             sync.RWMutex{},
			lastLogIndex:     0,
			lastLogTerm:      0,
			commitedIndex:    0,
			lastAppliedIndex: 0,
			roleState:        StateFollower,
			startIndex:       0,
		},
		clusterState:      new(ClusterConfigState),
		leaderState:       nil,
		rpcChan:           transport.RpcChan(),
		applyLogChan:      make(chan *ApplyLogReqFuture, config.LogBatchSize),
		logStore:          logStore,
		fsm:               fsm,
		newTermChan:       make(chan struct{}, 1),
		lastTouch:         time.Now(),
		updateClusterChan: make(chan *UpdateClusterFuture),
		rpcTrans:          transport,
		wg:                sync.WaitGroup{},
		initialCluster:    ClusterConfig{},
		initFlag:          false,
		exitFlag:          false,
	}

	//raft.restoreBaseState(config)
	raft.setupBaseState()
	raft.setupClusterConfig()
	raft.setupLastVote()

	clientListener := clientListener{}
	go clientListener.listenAndServe(raft)
	//raft.wgRun(raft.MainLoop)
	go transport.ListenAndRun()

	return raft
}

func (r *Raft) setupBaseState() {
	index, term := r.logStore.GetLastEntry()
	r.setLastEntry(index, term)

	r.setCurrentTerm(term)

	r.PrintBaseState(r.SelfID)
}

func (r *Raft) setupLastVote() {
	f, err := os.Open(r.config.LastVoteFilename)
	if err != nil {
		fmt.Println("failed to open last vote file:", err)
		return
	}
	decoder := yaml.NewDecoder(f)
	var lastVote VoteStored
	decoder.Decode(&lastVote)
	if r.getCurrentTerm() < lastVote.Term {
		r.setCurrentTerm(lastVote.Term)
	}
	fmt.Printf("%s currentTerm:%d\n", r.SelfID, r.getCurrentTerm())

	r.lastVote.Store(&lastVote)
}
func (r *Raft) setupClusterConfig() {
	f, err := os.Open(r.config.ClusterConfigFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(r.clusterState)
	if err != nil {
		panic(err)
	}
	r.clusterState.Print()
}

func (r *Raft) storeClusterConfig() {
	f, err := os.OpenFile(r.config.ClusterConfigFilename, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Failed to store cluster config state:", err)
		return
	}
	defer f.Close()
	encoder := yaml.NewEncoder(f)
	err = encoder.Encode(r.clusterState)
	if err != nil {
		fmt.Println("failed to encode cluster config state:", err)
		return
	}
}
func (r *Raft) Start() {
	r.wgRun(r.MainLoop)
}
func (r *Raft) Wait() {
	r.wg.Wait()
}
func (r *Raft) wgRun(f func()) {
	r.wg.Add(1)
	go func() {
		f()
		r.wg.Done()
	}()
}

func (r *Raft) handleRPC(rpc *RPC) {
	switch rpc.Type {
	case RpcAppendEntries:
		req := rpc.Request.(*AppendEntriesRequest)
		r.HandleAppendEntries(req, rpc)

	case RpcRequestVote:
		payload := rpc.Request.(*RequestVotePayload)
		r.handleRequestVote(payload, rpc)

	case RpcInstallSnapshot:

	}
}

func (r *Raft) MainLoop() {
	for !r.exitFlag {
		switch r.roleState {
		case StateLeader:
			r.startLeader()
		case StateFollower:
			r.runFollower()
		case StateCandidate:
			r.runCandidate()
		}
	}
}

var ErrNotLeader = errors.New("not leader")
var ErrNotInitialState = errors.New("not initial state")

func (r *Raft) runFollower() {
	fmt.Printf("[START-FOLLOWER] %s start follower state loop\n", r.SelfID)
	checkTicker := time.NewTicker(time.Second * 2)
	r.lastTouch = time.Now()

	for r.roleState == StateFollower {
		select {
		case <-checkTicker.C:
			if d := time.Since(r.lastTouch); d > time.Second*2 {
				fmt.Println("Check ticker tick, switch to candidate state, elasped time:", d)
				r.changeRoleState(StateCandidate)
			}
		case logFuture := <-r.applyLogChan:
			logFuture.respErr(ErrNotLeader)

		case future := <-r.updateClusterChan:
			// check whether it's inticluster dung do van de se la dang do dung?///
			if future.Type != CommandInitCluster {
				future.respErr(ErrNotLeader)
				continue
			}

			r.changeRoleState(StateLeader)
			r.setCurrentTerm(r.getCurrentTerm() + 1)
			r.initFlag = true
			r.initialCluster = ClusterConfig{
				Servers: future.Servers,
			}

			future.respErr(nil)

			//future.respErr(ErrNotInitialState)

		case rpc := <-r.rpcChan:
			//fmt.Printf("Recv rpc cid: %d\n", rpc.CorrelationID)
			r.handleRPC(rpc)
			//fmt.Println("Complete cid:", rpc.CorrelationID)
			//r.lastTouch = time.Now()
			//fmt.Println("Last touch:", r.lastTouch)
		}
	}
}
func randomTimeChan(minDuration, maxDuration int) <-chan time.Time {
	r := maxDuration - minDuration
	del := rand.Int() % int(r)

	return time.NewTimer(time.Duration(minDuration+del) * time.Millisecond).C
}

// when call this function, it ensures that other conditions like lastIndex upto date
func (r *Raft) checkAndVote(term uint64, candidateID ServerID) (voteGranted bool) {

	lastVote := r.lastVote.Load()
	if lastVote != nil {
		if lastVote.Term > term {
			return false
		}

		if lastVote.Term == term {
			return lastVote.CandidateID == candidateID
		}
	}
	newVote := &VoteStored{
		Term:        term,
		CandidateID: candidateID,
	}

	swapped := r.lastVote.CompareAndSwap(lastVote, newVote)
	// TODO: add to disk
	f, err := os.OpenFile(r.config.LastVoteFilename, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Failed to open vote file:", err)
		return swapped
	}
	defer f.Close()
	encoder := yaml.NewEncoder(f)
	err = encoder.Encode(newVote)

	if err != nil {
		fmt.Println("Failed to store last vote, err:", err)
	}
	return swapped
}

func (r *Raft) runCandidate() {
	quorum := (len(r.clusterState.Lastest.Servers) >> 1) + 1

START_ELECTION:
	currentTerm := r.getCurrentTerm() + 1
	r.setCurrentTerm(currentTerm)
	fmt.Printf("%s start election for term %d\n", r.SelfID, currentTerm)

	var voteRespChan chan RequestVoteResp = nil
	var electionTimeoutCh <-chan time.Time = nil

	if n := len(r.clusterState.Lastest.Servers); n > 1 {
		voteRespChan = make(chan RequestVoteResp, n)
		electionTimeoutCh = time.After(time.Second * 2)

		lastIdx, lastTerm := r.getLastEntry()
		term := r.getCurrentTerm()

		req := &RequestVotePayload{
			CandidateID:  r.SelfID,
			Term:         term,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}

		for _, s := range r.clusterState.Lastest.Servers {
			if s.ID == r.SelfID {
				go func() {
					<-randomTimeChan(100, 500)

					voteResp := RequestVoteResp{Term: currentTerm}
					voteResp.VoteGranted = r.checkAndVote(currentTerm, r.SelfID)
					if voteResp.VoteGranted {
						fmt.Printf("[VOTE] %s do self elect in term %d\n", r.SelfID, currentTerm)
					} else {
						fmt.Printf("[NON-VOTE] %s do not self elect in term %d\n", r.SelfID, currentTerm)
					}
					select {
					case voteRespChan <- voteResp:
					default:
					}
				}()
				continue
			}

			go func() {
				<-randomTimeChan(100, 500)
				resp, err := r.rpcTrans.RequestVote(s.Addr, req)
				if err != nil {
					fmt.Println("Failed to send request votes to peer", s.ID)
					//TODO: may retry ...
					return
				}

				if resp.VoteGranted {
					fmt.Printf("[VOTE] %s voted for %s in term %d\n", s.ID, r.SelfID, req.Term)
				} else {
					fmt.Printf("[NON-VOTE] %s did not vote for %s in term %d\n", s.ID, r.SelfID, req.Term)
				}
				select {
				case voteRespChan <- *resp:
				default:
				}
			}()
		}
	}

	fmt.Println("Start candidate phase,current term:", r.getCurrentTerm())
	var voteGrantedCnt int = 0

	for r.roleState == StateCandidate {
		select {
		case <-electionTimeoutCh:
			goto START_ELECTION
		case rpc := <-r.rpcChan:
			r.handleRPC(rpc)
			//r.lastTouch = time.Now()

		case fut := <-r.applyLogChan:
			fut.respErr(ErrNotLeader)
			continue

		case future := <-r.updateClusterChan:
			// check whether it's inticluster
			if future.Type != CommandInitCluster {
				future.respErr(ErrNotLeader)
				continue
			}
			// type: InitClusterAndIndicateLeader

			r.changeRoleState(StateLeader)

			r.setCurrentTerm(r.getCurrentTerm() + 1)
			r.initFlag = true

			r.initialCluster = ClusterConfig{
				Servers: future.Servers,
			}

			future.respErr(nil)

			//future.respErr(ErrNotInitialState)
		case voteResp := <-voteRespChan:
			//fmt.Println("VoteGranted:", voteResp.VoteGranted)
			if voteResp.Term > r.getCurrentTerm() {
				r.setCurrentTerm(voteResp.Term)
			}

			if voteResp.VoteGranted {
				voteGrantedCnt++
				if voteGrantedCnt >= quorum {
					fmt.Printf("[BECOME-LEADER] %s voteGranted count: %d, become leader in term %d\n", r.SelfID, voteGrantedCnt, currentTerm)
					r.changeRoleState(StateLeader)
					return
				}
			}

			if voteResp.Term > currentTerm {
				r.setCurrentTerm(voteResp.Term)
				r.changeRoleState(StateFollower)
				return
			}
		}
	}
}

func (r *Raft) startLeader() {
	// set up leader state first, before doing other thing :

	r.leaderState = &LeaderState{
		term:          r.getCurrentTerm(),
		followers:     make(map[ServerID]*follower, 0),
		commitState:   newClusterCommitState(r.commitedIndex),
		inflightQueue: newInflightQueue(),
	}
	// next
	if r.initFlag {
		r.clusterState.Lastest = r.initialCluster.Clone()
		r.clusterState.LastestIndex = r.getLastIndex() + 1
	}

	r.leaderState.commitState.InitMatchIndexes(r.clusterState.Lastest.Servers)

	r.setupReplication()

	// create first log and not specify Index + Term
	var firstLog *Log
	if r.initFlag {
		firstLog = &Log{
			Type: LogClusterConfig,
			Data: r.initialCluster.Encode(),
		}
		fmt.Println("create first log is LogClusterConfig")

		r.initFlag = false
	} else {
		firstLog = &Log{
			Type: LogNoop,
			Data: nil,
		}
	}

	logFut := makeApplyLogReqFuture(firstLog)
	r.processLogs([]*ApplyLogReqFuture{logFut})

	r.startIndex = r.lastLogIndex

	r.leaderLoop()
	// from here may have new term value
	r.releaseLeader()
}

var ErrStaleTerm = errors.New("stale term")

func (r *Raft) releaseLeader() {
	r.leaderState.stopAllReplication()
	r.leaderState.closeCommitChan()
	r.leaderState.releaseAllInflight(ErrStaleTerm)
	r.leaderState = nil
}

// only main-loop goroutine can change the follower state,
// stop follower and send new lastIndex to follower
func (r *Raft) setupReplication() {

	lastLogIndex, lastLogTerm := r.getLastEntry()
	currentTerm := r.getCurrentTerm()

	var inUsed map[ServerID]bool = make(map[ServerID]bool)
	for _, server := range r.clusterState.Lastest.Servers {
		if server.ID == r.SelfID {
			continue
		}
		_, ok := r.leaderState.followers[server.ID]
		inUsed[server.ID] = true
		if !ok {
			follower := &follower{
				nextIndex:        lastLogIndex + 1,
				notifyNewLogChan: make(chan struct{}, 1),
				stopChan:         make(chan struct{}),
				PeerID:           server.ID,
				PeerAddr:         server.Addr,
				term:             currentTerm,
				prevLogTerm:      lastLogTerm,
			}

			r.leaderState.followers[server.ID] = follower
			go r.replicateLoop(follower)
			continue
		}
		//fmt.Println("found old server ", server.ID)
		// TODO: check existed ServerID , but change ServerAddr
	}

	for serverID, follower := range r.leaderState.followers {
		if inUsed[serverID] {
			follower.Print()
			continue
		}
		// should close the stopchan
		follower.notifyStop()
		delete(r.leaderState.followers, serverID)
	}
}

var ErrLatestClusterConfigNotCommmited = errors.New("latest cluster config not commited yet")

func (r *Raft) leaderLoop() {
	fmt.Println("go in leader loop")
	notifyCommitChangeChan := r.leaderState.commitState.notifyCommitChangeChan
	var logFutures []*ApplyLogReqFuture = make([]*ApplyLogReqFuture, 0)
	for r.roleState == StateLeader {
		select {
		case future := <-r.updateClusterChan:

			if r.clusterState.LastestIndex > r.clusterState.CommitedIndex || r.startIndex == 0 {
				future.respErr(ErrLatestClusterConfigNotCommmited)
				continue
			}
			r.updateCluster(future.Type, future.Servers)
			future.respErr(nil)

		case rpc := <-r.rpcChan:
			r.handleRPC(rpc)

		case <-r.newTermChan:
			term := r.getNewTerm()

			if r.leaderState.term < term {
				r.setCurrentTerm(term)
				r.changeRoleState(StateFollower)
			}

		case <-notifyCommitChangeChan:
			index := r.leaderState.commitState.getCurrentCommitIndex()
			if index == r.commitedIndex {
				continue
			}

			//fmt.Printf("leader-loop receive new commit index %d\n", index)
			if index < r.commitedIndex {
				// some conflict may occur due to cluster-config change
				// so it descrease the commited index of the entire cluster
				// just update the new value of commited index, not apply logs
				r.updateCommit(index)
				continue
			}
			r.applyLogs(index)
			r.updateCommit(index)

		case logFuture := <-r.applyLogChan:
			//batch := []*ApplyLogReqFuture{logFuture}
			//timer := time.NewTimer(r.config.BatchTimeout)
			logFutures = append(logFutures, logFuture)

		BATCH:
			for {
				select {
				case logF := <-r.applyLogChan:
					//batch = append(batch, logF)
					logFutures = append(logFutures, logF)
					if len(logFutures) == r.config.LogBatchSize {
						break BATCH
					}
				default:
					break BATCH
				}
			}
			logBatch := logfutPool.Get(uint32(len(logFutures)))
			copy(logBatch, logFutures)
			logFutures = logFutures[:0]
			r.processLogs(logBatch)
			//r.processLogs(batch)
		}
	}
}

func (r *Raft) updateCommit(index uint64) {
	//fmt.Println("New CommitIndex: ", index)
	r.setCommitIndex(index)

	if r.clusterState.CommitedIndex != r.clusterState.LastestIndex && r.commitedIndex >= r.clusterState.LastestIndex {
		/* r.clusterState.Commited = r.clusterState.Lastest.Clone()
		r.clusterState.CommitedIndex = r.clusterState.LastestIndex */

		r.clusterState.commitLatestState()
		r.storeClusterConfig()
	}
}

// only call when state is Leader
func (r *Raft) applyLogs(index uint64) {
	r.applyToFSM(index)
	if r.leaderState != nil {
		r.leaderState.releaseInflight(index, nil)
	}
}

func (r *Raft) applyToFSM(index uint64) {
	lastAppliedIdx := r.getLastAppliedIndex()

	var entries []*Log
	if r.roleState == StateLeader {
		//fmt.Println("get entries from inflight queue")
		entries = r.leaderState.inflightQueue.GetRangeUpto(index)
	}
	if len(entries) == 0 {
		logs, err := r.logStore.GetRangeLogs(lastAppliedIdx, index)
		if err != nil {
			fmt.Println("Failed to get logs, caused by:", err)
			return
		}
		entries = logs
	}
	// TODO:
	if len(entries) == 0 {
		return
	}

	r.fsm.ApplyLogs(entries)
	r.lastAppliedIndex = entries[len(entries)-1].Index
}

func (r *Raft) updateCluster(cmdType int, servers []Server) {
	config := ClusterConfig{}
	fmt.Println("handle update cluster, servers: ", servers)
	switch cmdType {
	case CommandAddPeers:
		config.Servers = make([]Server, 0, len(r.clusterState.Lastest.Servers)+len(servers))
		if r.clusterState.Lastest.Servers != nil {
			config.Servers = append(config.Servers, r.clusterState.Lastest.Servers...)
		}
		config.Servers = append(config.Servers, servers...)

		// TODO: handle other case like RemovePeers
	case CommandInitCluster:
		config.Servers = servers
	}

	logFuture := makeApplyLogReqFuture(&Log{
		Type: LogClusterConfig,
		Data: config.Encode(),
	})

	r.processLogs([]*ApplyLogReqFuture{logFuture})
	// after processLogs return, r.lastLogIndex is increase
	// not set lastEntry here

	// when new latest config created, it firstly replicates to current follower nodes in cluster
	// and after that, it reset the replication config

	r.setLatestClusterConfig(r.getLastIndex(), config)
	r.clusterState.Print()
	if r.leaderState != nil {
		r.setupReplication()
	}
}

func (r *Raft) setLatestClusterConfig(index uint64, config ClusterConfig) {
	// only set new latest cluster state when the current latest cluster config is committed
	r.clusterState.LastestIndex = index
	r.clusterState.Lastest = config
	r.storeClusterConfig()
}

// store logs to logStore, keep futures in inflightQueue, and notify new Logs to follower
// each log created must go through this function
// only one point to change the lastLogIndex
func (r *Raft) processLogs(logFutures []*ApplyLogReqFuture) {
	lastIndex := r.getLastIndex()
	currentTerm := r.getCurrentTerm()
	// co the la ko nen use it dung?

	logs := logPtrSlicePool.Get(uint32(len(logFutures)))
	//logs := make([]*Log, len(logFutures))
	appMetrics.AddBatch(len(logFutures))

	//fmt.Println("PROCESS BATCH len:", len(logFutures), "from idx: ", lastIndex+1)
	//firstIdx := lastIndex + 1

	for i, logFuture := range logFutures {
		lastIndex++
		logFuture.log.Index = lastIndex
		logFuture.log.Term = currentTerm
		logs[i] = logFuture.log
	}
	// store logs before change the last log entry
	err := r.logStore.StoreBatchLog(logs)
	//r.logStore.GetRangeLogs(1, 1)

	logPtrSlicePool.Put(logs)

	if err != nil {
		for _, logFut := range logFutures {
			logFut.respErr(err)
		}
		// TODO: log error
		fmt.Println("failed to store log, caused by:", err)
		return
	}

	r.setLastIndex(lastIndex)

	if r.leaderState != nil {
		r.leaderState.commitState.Update(r.SelfID, lastIndex)
		//fmt.Printf("Leader prepare to put inflight for batch [%d:%d]\n", firstIdx, lastIndex)
		r.leaderState.putInflight(logFutures)
		for _, follower := range r.leaderState.followers {
			follower.notifyNewLog()
		}
	}
}

func (r *Raft) HandleAppendEntries(a *AppendEntriesRequest, rpc *RPC) {
	//fmt.Printf("[RECV-AppendEntries] %s handle append entries req, cid:%d\n", r.SelfID, rpc.CorrelationID)
	r.lastTouch = time.Now()

	currentTerm := r.getCurrentTerm()
	lastIndex, lastTerm := r.getLastEntry()
	resp := &AppendEntriesResponse{
		Success:      true,
		Term:         currentTerm,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		Stop:         false,
	}
	if a.Term < currentTerm {
		resp.Success = false
		rpc.Respond(resp)
		return
	}

	if a.Term >= currentTerm {
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
		r.changeRoleState(StateFollower)
		// TODO: set leader id + address
	}

	if a.PrevLogIndex == lastIndex {
		if a.PrevLogTerm != lastTerm {
			// in this case, split brain maybe occur
			fmt.Printf("[CONFLICT lastTerm]: %s lastTerm = %d a.PrevLogTerm=%d, lastIndex = %d\n", r.SelfID, lastTerm, a.PrevLogTerm, lastIndex)
			resp.Success = false
			resp.Stop = true
			rpc.Respond(resp)
			return
		}

	} else if a.PrevLogIndex > lastIndex {
		//fmt.Printf("[RECV-AppendEntries CONFLICT]%s recv AppendEntries req.PrevLogIndex > lastIndex: %d > %d \n", r.SelfID, a.PrevLogIndex, lastIndex)
		resp.Success = false
		rpc.Respond(resp)
		return
	} else {
		//fmt.Println("in branch ")
		// req.PrevLogIndex < lastIndex
		log, err := r.logStore.GetLog(a.PrevLogIndex)
		if err != nil {
			resp.Success = false
			resp.Stop = true
			rpc.Respond(resp)
			return
		}
		if log.Term != a.PrevLogTerm {
			resp.Success = false
			resp.Stop = true
			rpc.Respond(resp)
			return
		}
	}
	//fmt.Printf("%s rev total entries %d\n", r.SelfID, len(req.Entries))
	if l := len(a.Entries); l > 0 {
		for idx := 0; idx < l; idx++ {
			log := a.Entries[idx]
			if log.Index <= lastIndex {
				continue
			}

			logs := make([]*Log, 0, len(a.Entries)-idx)

			for _, e := range a.Entries[idx:] {
				//fmt.Printf("%s recv log idx: %d, type: %d\n", r.SelfID, l.Index, l.Type)

				if e.Type == LogClusterConfig {
					// Logcluster
					clusterCfg := new(ClusterConfig)
					clusterCfg.Decode(e.Data)
					fmt.Println("Detect cluster config log from leader")

					r.clusterState.setLatestConfig(e.Index, clusterCfg)
					r.storeClusterConfig()
				}
				logs = append(logs, e)
			}
			//fmt.Printf("%s store logs [%d:%d]\n", r.SelfID, logs[0].Index, logs[len(logs)-1].Index)
			if len(logs) > 0 {
				if logs[0].Index != lastIndex+1 {
					fmt.Printf("CONFLICT: append entries[%d:%d], but lastIndex:%d\n",
						logs[0].Index, logs[len(logs)-1].Index, lastIndex)
				}
			}
			r.logStore.StoreBatchLog(logs)

			resp.LastLogIndex = logs[len(logs)-1].Index
			resp.LastLogTerm = logs[len(logs)-1].Term
			lastIndex = resp.LastLogIndex
			lastTerm = resp.LastLogTerm
			break
			// TODO: call apply
		}
	}

	if resp.LastLogIndex > lastIndex {
		lastIndex = resp.LastLogIndex
		lastTerm = resp.LastLogTerm
	}

	commitIndex := a.LeaderCommitIndex
	if commitIndex > lastIndex {
		commitIndex = lastIndex
	}

	if commitIndex >= r.clusterState.LastestIndex {
		r.clusterState.commitLatestState()
		r.storeClusterConfig()
	}
	//fmt.Printf("%s set last index: %d\n", r.SelfID, lastIndex)

	r.setLastEntry(lastIndex, lastTerm)
	/* if r.getCommitIndex() != commitIndex {
		fmt.Printf("[UPDATE-COMMIT] %s set commitIndex %d\n", r.SelfID, commitIndex)
	} */

	r.setCommitIndex(commitIndex)
	rpc.Respond(resp)
}

func (r *Raft) handleRequestVote(payload *RequestVotePayload, rpc *RPC) {
	//fmt.Printf("%s handle request vote for candidate: %s, term:%d \n", r.SelfID, payload.CandidateID, payload.Term)

	currentTerm := r.getCurrentTerm()
	lastIndex, lastTerm := r.getLastEntry()
	var resp *RequestVoteResp = &RequestVoteResp{
		Term:        currentTerm,
		VoteGranted: false,
	}

	if payload.Term < currentTerm {
		rpc.Respond(resp)
		return
	}

	if payload.Term > currentTerm {
		resp.Term = payload.Term
		r.setCurrentTerm(payload.Term)
	}

	if payload.LastLogTerm < lastTerm {
		rpc.Respond(resp)
		return
	}

	if payload.LastLogTerm == lastTerm {
		if payload.LastLogIndex < lastIndex {
			rpc.Respond(resp)
			return
		}
		resp.VoteGranted = r.checkAndVote(payload.Term, payload.CandidateID)
		rpc.Respond(resp)
		return
	}
	// payload.LastLogTerm > lastTerm dung?
	resp.VoteGranted = r.checkAndVote(payload.Term, payload.CandidateID)
	rpc.Respond(resp)
}
