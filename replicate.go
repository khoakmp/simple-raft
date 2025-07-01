package simpleraft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// when starting one replication loop, assume that it's reached the last log index of leader
// and may resolve conflict later
func (r *Raft) replicateLoop(f *follower) {
RECONCILE:
	quitHeartbeatChan := make(chan struct{})
	go r.heartbeatLoop(f, quitHeartbeatChan)

	for {
		select {
		// TODO: handle hearbeat signal ??
		case <-f.stopChan:
			fmt.Println("stop replicating to peer:", f.PeerID)
			return

		case <-f.notifyNewLogChan:
			/* r.printLock.Lock()
			fmt.Printf("new last index: %d need to be replicated to peer %s\n", lastIndex, f.PeerID)
			r.printLock.Unlock() */
			lastIndex := r.getLastIndex()

			err := r.replicateTo(f, lastIndex)
			if err == nil {
				close(quitHeartbeatChan)
				goto PIPELINE
			}
			fmt.Println("Error when calling replicate to ", f.PeerID)
			// TODO: handle error
			// 1. network issue
			// 2. conflict state with follower and unresolvable ???
		}
	}

PIPELINE:
	fmt.Printf("Change to PIPELINE replication for %s\n", f.PeerID)
	p, err := r.rpcTrans.CreatePipeline(f.PeerAddr)

	if err != nil {
		fmt.Println("Failed to create pipeline, err:", err)
		// TODO: handle error here
	}
	r.pipelineReplicate(f, p)
	fmt.Println("OUT PIPELINE, jump into REPLICATE")
	// when go out of pipeline , some conflict, need to resolve by the replicate loop
	goto RECONCILE
}

var ErrFollowerConflict = errors.New("follower conflict")

// replicate until the follower reaches index or some unresolvable error occur
func (r *Raft) replicateTo(f *follower, index uint64) error {
	// if in main-loop, one append-entries call make the current term to be stale
	// so must use the term stored in follower
	/* if f.nextIndex > index {
		f.nextIndex = index
	} */

	fmt.Printf("Leader %s try to replicate to peer %s upto index %d\n", r.SelfID, f.PeerID, index)

	for {
		commitIndex := r.getCommitIndex()
		fmt.Printf("NextIndex of %s : %d\n", f.PeerID, f.nextIndex)

		entries, lastPosFromDisk, err := r.setupAppendEntries(f.nextIndex, index)

		// call it as -> resolve first -> dung do de ma no
		if err != nil {
			fmt.Println("Failed to set up entries to send", err)
			if err == ErrLogNotFound {

				return err
				// TODO: may send install snapshot instead
				// choose appropriate snapshot, and send directly from disk to network
				// not need buffering snaphsot in memory

				// if not error when sending snapshot -> continue else return
				// set up f.nextIndex
				//continue
			}
			return err
		}

		req := &AppendEntriesRequest{
			LeaderID:          r.SelfID,
			PrevLogIndex:      f.nextIndex - 1,
			Term:              f.term,
			PrevLogTerm:       f.prevLogTerm,
			LeaderCommitIndex: commitIndex,
		}

		fmt.Printf("Leader replicate to %s [%d:%d]\n", f.PeerID, entries[0].Index, entries[len(entries)-1].Index)
		req.Entries = entries

		resp, err := r.rpcTrans.AppendEntries(f.PeerAddr, req)
		r.releaseEntriesData(req.Entries, lastPosFromDisk)
		/* for i := lastPosFromDisk; i < len(req.Entries); i++ {
			bspool.Put(req.Entries[i].Data)
		} */

		//logPtrSlicePool.Put(req.Entries)

		if err != nil {
			// not able to make rpc call (due to something like network issue)
			fmt.Println("failed to call appendEntries, caused by:", err)
			// replication is not ready now , just return
			return err
		}

		fmt.Println("Recv response when call AppendEntries:")
		resp.Print()
		atomic.AddUint32(&f.numRPC, 1)
		if resp.Success {
			// follower apply sent entries, and it's last logIndex is the last entry's index
			// when resp.Success = true, it means that
			f.nextIndex = resp.LastLogIndex + 1
			f.prevLogTerm = resp.LastLogTerm

			r.leaderState.commitState.Update(f.PeerID, resp.LastLogIndex)
			// only resp.Success update commitState
			fmt.Printf("INFOR-REPL: replicate to peer %s up to date index %d\n", f.PeerID, resp.LastLogIndex)
			//fol.Print()

			if f.nextIndex == index+1 {
				return nil
			}

			// continue sending next logs until it reach index param
		} else {
			if resp.Term > f.term {
				fmt.Println("Detect stale term when replicating logs")
				//r.setCurrentTerm(resp.Term)
				r.setNewTerm(resp.Term)
				select {
				case r.newTermChan <- struct{}{}:
				default:
				}

				return ErrStaleTerm
			}

			f.nextIndex = resp.LastLogIndex + 1
			f.prevLogTerm = resp.LastLogTerm

			if resp.Stop {
				return ErrFollowerConflict
			}
		}
	}

}

var ErrLogIndexTooSmall = errors.New("log index too small")

func (r *Raft) releaseEntriesData(entries []*Log, toIndex int) {
	appMetrics.IncrPutLogDataBufferCnt(uint32(toIndex))
	/* for i := 0; i < toIndex; i++ {
		bspool.Put(entries[i].Data)
	} */
}

func (r *Raft) setupAppendEntries(startIdx, endIdx uint64) (logs []*Log, posFromDisk int, err error) {
	// TODO: get entries from logstore or return error
	if endIdx-startIdx+1 > uint64(r.config.MaxLogPerTrip) {
		endIdx = startIdx + uint64(r.config.MaxLogPerTrip) - 1
	}

	/* if r.leaderState != nil {
		logs = r.leaderState.inflightQueue.GetRangeLogs(startIdx, endIdx)
	}

	if len(logs) == 0 {
		logs, err := r.logStore.GetRangeLogs(startIdx, endIdx)
		return logs, 0, err
	}

	if logs[0].Index > startIdx {
		//fmt.Printf("Get range logs[%d:%d] from inflightQueue\n", logs[0].Index, logs[len(logs)-1].Index)
		diskLogs, err := r.logStore.GetRangeLogs(startIdx, logs[0].Index-1)
		if err != nil {
			return nil, 0, err
		}
		//fmt.Printf("Get range logs[%d:%d] from disk\n", diskLogs[0].Index, diskLogs[len(diskLogs)-1].Index)

		totalLogs := append(diskLogs, logs...)

		return totalLogs, len(diskLogs), nil
	} */
	logs, err = r.logStore.GetRangeLogs(startIdx, endIdx)

	return logs, len(logs), err
}

func (r *Raft) pipelineReplicate(f *follower, p Pipeline) (shouldStop bool, errRet error) {
	// errRet is network issue or peer closed

	var quitChan chan struct{} = make(chan struct{})

	go func() {
	LOOP:
		for {
			select {
			case rpcResp := <-p.ResponseChan():

				resp, ok := rpcResp.Response.(*AppendEntriesResponse)
				if !ok {
					fmt.Println("Failed to read from rpc response chan")
					break LOOP
				}
				//fmt.Println("Get resp with lastlogIndex:", resp.LastLogIndex)

				f.nextIndex = resp.LastLogIndex + 1
				f.prevLogTerm = resp.LastLogTerm
				tripStatus := StatusTripSuccess

				if !resp.Success {
					tripStatus = StatusTripFail
				}

				p.Metrics().completeTrip(rpcResp.CorrelationID, tripStatus)
				if resp.Success {
					//fmt.Println("Update lastlogIndex of ", f.PeerID, "to ", resp.LastLogIndex)
					r.leaderState.commitState.Update(f.PeerID, resp.LastLogIndex)

				} else {

					if resp.Term > f.term {
						r.setNewTerm(resp.Term)

						//r.setCurrentTerm(resp.Term)
						select {
						case r.newTermChan <- struct{}{}:
						default:
						}
					}

					p.Close()
					fmt.Printf("Failed in pipeline replication to %s, switch to resolve conflict mode\n", f.PeerID)
					break LOOP
				}
				//fmt.Printf("%s ready to read new response\n", f.PeerID)
			case <-f.stopChan:
				break LOOP
			}
		}
		close(quitChan)

	}()

	var nextIndex uint64 = f.nextIndex
	var prevLogTerm uint64 = f.prevLogTerm
	var currentTerm uint64 = f.term

	var lastIndex uint64 = r.getLastIndex()

	// TODO: remove ticker later
	//var ticker *time.Ticker = time.NewTicker(time.Millisecond * 200)
	for {
		select {

		/* case <-ticker.C:
		fmt.Printf("%s max delta send/recv: %d\n", f.PeerID, p.Metrics().getMaxDelta())
		*/
		case <-randomHeartBeat(100):
			commitIndex := r.getCommitIndex()

			req := &AppendEntriesRequest{
				LeaderID:          r.SelfID,
				PrevLogIndex:      nextIndex - 1,
				PrevLogTerm:       prevLogTerm,
				Term:              currentTerm,
				LeaderCommitIndex: commitIndex,
				Entries:           nil,
			}
			var posFromDisk int = 0
			if nextIndex <= lastIndex {
				entries, pos, err := r.setupAppendEntries(nextIndex, lastIndex)
				if err != nil {
					return false, err
				}
				posFromDisk = pos
				req.Entries = entries
				fmt.Printf("Set up entries in heartbeat request: [%d:%d]\n", nextIndex, lastIndex)
			}

			correlationID, err := p.AppendEntries(req)
			if len(req.Entries) > 0 {
				/* for i := posFromDisk; i < len(req.Entries); i++ {
					bspool.Put(req.Entries[i].Data)
				} */
				r.releaseEntriesData(req.Entries, posFromDisk)

				//logPtrSlicePool.Put(req.Entries)
			}

			if err != nil {
				// TODO: handle error dung?
				return false, err
			}

			//fmt.Printf("%s sent %d rpc\n", f.PeerID, sent)

			if l := len(req.Entries); l > 0 {

				//fmt.Printf("Send to %s entries[%d : %d]\n", f.PeerID, req.Entries[0].Index, req.Entries[l-1].Index)
				p.Metrics().putTrip(correlationID, req.Entries[0].Index, req.Entries[l-1].Index)
				//fmt.Printf("%s , nextIndex = %d\n", f.PeerID, nextIndex)
				nextIndex = req.Entries[len(req.Entries)-1].Index + 1

			}
		case <-quitChan:
			return false, nil
		case <-f.stopChan:
			// just return
			return true, nil

		/* case rpcResp := <-p.ResponseChan():
		// in pipeline repliate, response type is only AppendEntries Resp

		resp := rpcResp.Response.(*AppendEntriesResponse)

		p.Metrics().completeTrip(rpcResp.CorrelationID, StatusTripSuccess)

		if resp.Success {
			f.nextIndex = resp.LastLogIndex + 1

			f.prevLogTerm = resp.LastLogTerm
			r.leaderState.commitState.Update(f.PeerID, resp.LastLogIndex)
			continue
		}
		// TODO: may check conflict type dung?
		p.Close()

		if resp.Term > f.term {
			r.setCurrentTerm(resp.Term)
			select {
			case <-f.stopChan:
			case r.newTermChan <- resp.Term:
			}

			return true, ErrStaleTerm
		}

		f.nextIndex = resp.LastLogIndex + 1
		f.prevLogTerm = resp.LastLogTerm

		return false, nil */

		case <-f.notifyNewLogChan:
			lastIndex = r.getLastIndex()
			if lastIndex < nextIndex {
				//fmt.Printf("Follower %s is up to date, not need to replicate in this trigger\n", f.PeerID)
				continue
			}

			entries, posFromDisk, err := r.setupAppendEntries(nextIndex, lastIndex)
			if err != nil {
				// TODO: handle error
				return false, err
			}

			//fmt.Printf("Replicate entries[%d:%d] to %s\n", entries[0].Index, entries[len(entries)-1].Index, f.PeerID)
			commitIndex := r.getCommitIndex()
			req := &AppendEntriesRequest{
				LeaderID:          r.SelfID,
				PrevLogIndex:      nextIndex - 1,
				PrevLogTerm:       prevLogTerm,
				Term:              currentTerm,
				LeaderCommitIndex: commitIndex,
				Entries:           entries,
			}
			// when send AppendEntries request, ensure that len(entries) > 0
			correlationID, err := p.AppendEntries(req)

			// only put entry.Data back to bspool when entry is read from disk

			/* for i := posFromDisk; i < len(req.Entries); i++ {
				bspool.Put(req.Entries[i].Data)
			} */

			r.releaseEntriesData(req.Entries, posFromDisk)
			/* for i, entry := range req.Entries {
				bspool.Put(entry.Data)
			} */

			//logPtrSlicePool.Put(req.Entries)

			if err != nil {

				fmt.Println("Failed to append entries:", err)
				// only 2 cases cause error,
				// 1. Network Error
				// 2. EOF (peer closed connection explicitly or peer process crashed but the os is still working)
				// in 2 these case, read loop of connection also detect error => so it must stop readloop

				return false, err
			}

			//fmt.Printf("Send to %s trip_cid: %d, entries[%d : %d]\n", f.PeerID, correlationID, entries[0].Index, entries[len(entries)-1].Index)

			atomic.AddUint32(&f.numRPC, 1)

			//fmt.Printf("%s sent %d rpc\n", f.PeerID, sent)
			if l := len(req.Entries); l > 0 {
				nextIndex = req.Entries[l-1].Index + 1
				//lastIdx := r.getLastIndex()
				//fmt.Println("DELTA: ", lastIdx-nextIndex+1)

				p.Metrics().putTrip(correlationID, req.Entries[0].Index, req.Entries[l-1].Index)
				//fmt.Printf("%s , nextIndex = %d\n", f.PeerID, nextIndex)
			}

			if nextIndex <= lastIndex {
				select {
				case f.notifyNewLogChan <- struct{}{}:
				default:
				}
			}

		}
	}
}

func randomHeartBeat(minDuration int) <-chan time.Time {
	v := time.Duration(minDuration + rand.Int()%minDuration)
	return time.After(time.Millisecond * v)
}

func (r *Raft) heartbeatLoop(f *follower, quitChan chan struct{}) {
	fmt.Printf("[START-HEARTBEAT-LOOP] %s start heartbeat loop to %s\n", r.SelfID, f.PeerID)

	for {
		select {
		case <-f.stopChan:
			return

		case <-quitChan:
			fmt.Println("OUT heartbeat loop")
			return

		case <-randomHeartBeat(100):
			//fmt.Printf("[SEND-HEARTBEAT] %s send HEARTBEAT to %s\n", r.SelfID, f.PeerID)

			req := r.makeAppendEntriesRequest(f)
			_, err := r.rpcTrans.AppendEntries(f.PeerAddr, req)
			// TODO: handle error here
			if err != nil {
				fmt.Println("Failed to send heartbeat:", err)
			}
			/* if !resp.Success {
				f.notifyNewLog()
			} */
		}
	}
}

func (r *Raft) makeAppendEntriesRequest(f *follower) *AppendEntriesRequest {
	//commitIndex := r.getCommitIndex()
	req := &AppendEntriesRequest{
		LeaderID:          r.SelfID,
		PrevLogIndex:      f.nextIndex - 1,
		PrevLogTerm:       f.prevLogTerm,
		Term:              f.term,
		LeaderCommitIndex: r.getCommitIndex(),
		Entries:           nil,
	}
	return req
}

const (
	StatusTripWait    = 1
	StatusTripSuccess = 2
	StatusTripFail    = 3
	StatusTripError   = 4
)

type pipelineTrip struct {
	cid        TypeCorrelationID
	sendTime   time.Time
	startIndex uint64
	endIndex   uint64
	//respTime   time.Time
}

type pipelineMetrics struct {
	followerID  ServerID
	trips       map[TypeCorrelationID]pipelineTrip
	lock        sync.Mutex
	lastSendCID TypeCorrelationID
	lastRecvCID TypeCorrelationID
	maxDelta    uint32
}

func (metrics *pipelineMetrics) putTrip(cid TypeCorrelationID, startIndex, endIndex uint64) {
	metrics.lock.Lock()
	metrics.trips[cid] = pipelineTrip{
		cid:        cid,
		sendTime:   time.Now(),
		startIndex: startIndex,
		endIndex:   endIndex,
	}

	metrics.lastSendCID = cid
	delta := metrics.lastSendCID - metrics.lastRecvCID
	if delta > atomic.LoadUint32(&metrics.maxDelta) {
		atomic.StoreUint32(&metrics.maxDelta, delta)
	}

	metrics.lock.Unlock()
}

func (metrics *pipelineMetrics) getMaxDelta() uint32 {
	return atomic.LoadUint32(&metrics.maxDelta)
}
func (metrics *pipelineMetrics) completeTrip(cid TypeCorrelationID, status int) {
	metrics.lock.Lock()
	defer metrics.lock.Unlock()
	_, ok := metrics.trips[cid]
	if !ok {
		return
	}
	// number of trips = num_logs/max_log_per_trip

	delete(metrics.trips, cid)
	metrics.lastRecvCID = cid
	//fmt.Println("lastSendCID: ", metrics.lastSendCID, ", lastRecvCID:", metrics.lastRecvCID)
	//fmt.Printf("Trip %d [%d:%d] of %s duration:%s, status: %d\n", cid, trip.startIndex, trip.endIndex, metrics.followerID, time.Since(trip.sendTime), status)
}
