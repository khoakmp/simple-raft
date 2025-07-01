package simpleraft

import (
	"errors"
	"time"
)

type Command struct {
	Data            []byte
	CorrelationID   uint32
	applyLogFutChan chan *ApplyLogReqFuture
}

// TODO: remove errChan later
type ApplyLogReqFuture struct {
	log             *Log
	errChan         chan error
	err             error
	applyLogFutChan chan *ApplyLogReqFuture
	correlationID   uint32 // each request has one correlationID
}

func makeApplyLogReqFuture(log *Log) *ApplyLogReqFuture {
	return &ApplyLogReqFuture{
		log:     log,
		errChan: make(chan error, 1),
	}
}

var ErrReqTimeout = errors.New("request timeout")

func (ft *ApplyLogReqFuture) Err() error {

	select {
	case err := <-ft.errChan:
		return err
	case <-time.After(time.Second * 2):
		return ErrReqTimeout
	}
}

func (ft *ApplyLogReqFuture) respErr(err error) {
	if ft.correlationID == 0 {
		return
	}
	ft.err = err
	//fmt.Println("SET err response for apply logReqFuture with cid", ft.correlationID)
	if ft.applyLogFutChan != nil {
		ft.applyLogFutChan <- ft
	}

	if ft.errChan != nil {
		ft.errChan <- err
	}
}

func (r *Raft) ApplyCommand(cmd Command) *ApplyLogReqFuture {

	/* log := logpool.Get()
	log.Index = 0
	log.Term = 0
	log.Type = LogCommand
	log.Data = cmd.Data */

	logFut := &ApplyLogReqFuture{
		log: &Log{
			Type: LogCommand,
			Data: cmd.Data,
		},
		correlationID:   cmd.CorrelationID,
		applyLogFutChan: cmd.applyLogFutChan,
		//log:     log,
		//errChan: make(chan error, 1), // not blocking sender, if not have receiver
		errChan: nil,
	}

	//fmt.Println("Created logFuture cid:", logFut.correlationID)
	r.applyLogChan <- logFut
	return logFut
}

const (
	CommandAddPeers    = 0
	CommandRemovePeers = 1
	CommandInitCluster = 2
)

type UpdateClusterFuture struct {
	Type    int
	Servers []Server
	errChan chan error
}

func (ft *UpdateClusterFuture) respErr(err error) {
	ft.errChan <- err
}

func (ft *UpdateClusterFuture) Err() error {

	return <-ft.errChan
}

func (r *Raft) AddSinglePeer(serverID ServerID, serverAddr string) {
}

var ErrServerNotInCluster = errors.New("server is not in cluster")

// apply when current term is 0, indicate this node is leader
func (r *Raft) InitCluster(servers []Server) (*UpdateClusterFuture, error) {
	ok := false
	for _, s := range servers {
		if s.ID == r.SelfID {
			ok = true
			break
		}
	}
	if !ok {
		return nil, ErrServerNotInCluster
	}

	future := &UpdateClusterFuture{
		Type:    CommandInitCluster,
		errChan: make(chan error, 1),
		Servers: servers,
	}

	r.updateClusterChan <- future
	return future, nil
}
