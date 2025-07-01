package simpleraft

import "sync"

// adding RequestVote

type RPCTransport interface {
	AppendEntries(addr string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(addr string) // TODO:...
	RequestVote(addr string, payload *RequestVotePayload) (resp *RequestVoteResp, err error)

	RpcChan() <-chan *RPC
	CreatePipeline(addr string) (Pipeline, error)
	ListenAndRun() error
}

type Pipeline interface {
	ResponseChan() <-chan RPCResponse
	AppendEntries(req *AppendEntriesRequest) (correlationID TypeCorrelationID, err error)
	Metrics() *pipelineMetrics
	Close()
}

type MockTransport struct {
	printLock *sync.Mutex
	rpcChan   chan *RPC
}
type MockPipeline struct{}

func (p *MockPipeline) AppendEntries(req *AppendEntriesRequest) (TypeCorrelationID, error) {
	return 0, nil
}

func (p *MockPipeline) Metrics() *pipelineMetrics {
	return nil
}

func (t *MockTransport) RpcChan() <-chan *RPC {
	return t.rpcChan
}

func (t *MockTransport) SetPrintLock(lock *sync.Mutex) {
	t.printLock = lock
}

func (t *MockTransport) AppendEntries(addr string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp := AppendEntriesResponse{
		Success: true,
		Term:    req.Term,
		Stop:    false,
	}
	l := len(req.Entries)
	if l > 0 {
		entry := req.Entries[l-1]
		resp.LastLogIndex, resp.LastLogTerm = entry.Index, entry.Term
	}
	return &resp, nil
}

func (t *MockTransport) InstallSnapshot(addr string) {}
func (t *MockTransport) RequestVote(addr string, payload *RequestVotePayload) (resp *RequestVoteResp, err error) {
	return nil, ErrPeerNotReady
}
func (t *MockTransport) CreatePipeline(addr string) (Pipeline, error) {
	return &MockPipeline{}, nil
}
func (t *MockTransport) ListenAndRun() error {
	return nil
}
func (t *MockPipeline) ResponseChan() <-chan RPCResponse {
	return nil
}
func (t *MockPipeline) Close() {
}
