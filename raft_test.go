package simpleraft

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestCreateRaft(t *testing.T) {
	config := Config{
		ServerID:     "server-1",
		ServerAddr:   "127.0.0.1:5051",
		LogBatchSize: 20,
	}

	logStore := InmemLogStore{
		data: make(map[uint64]*Log),
	}

	fsm := MockFSM{}
	transport := NewNetTransport(&config)
	go samplePeerLoop("server-2", "127.0.0.1:5052")
	go samplePeerLoop("server-3", "127.0.0.1:5053")
	raft := NewRaft(&config, &logStore, &fsm, transport)
	raft.Start()
	servers := sampleServers()

	ft, _ := raft.InitCluster(servers)
	//assert.Equal(t, err, nil)
	err := ft.Err()
	fmt.Println(err)
	//assert.Equal(t, err, nil)
	raft.clusterState.Print()
	//raft.Wait()
	<-time.After(time.Second)
}

func TestCluster(t *testing.T) {

	cluster := ClusterConfigState{
		LastestIndex: 5,
		Lastest: ClusterConfig{
			Servers: []Server{
				{ID: "server-1", Addr: "localhost:5051"},
				{ID: "server-2", Addr: "localhost:5052"},
				{ID: "server-3", Addr: "localhost:5053"},
			},
		},
		CommitedIndex: 5,
		Commited: ClusterConfig{
			Servers: []Server{
				{ID: "server-1", Addr: "localhost:5051"},
				{ID: "server-2", Addr: "localhost:5052"},
				{ID: "server-4", Addr: "localhost:5054"},
			},
		},
	}
	cluster.Print()
}
func TestBytesBuffer(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString("khoa pham")
	buffer.Next(4)
	buffer.Truncate(3)
	fmt.Println("current buffer:", buffer.String())
	fmt.Println("len after truncate 1 byte:", buffer.Len())

	fmt.Println("cap:", buffer.Cap())
}

func TestLeader(t *testing.T) {
	config := Config{
		ServerID:     "server-1",
		ServerAddr:   "127.0.0.1:5051",
		LogBatchSize: 20,
	}

	//go samplePeerLoop("server-2", "127.0.0.1:5052")
	//go samplePeerLoop("server-3", "127.0.0.1:5053")

	logStore := InmemLogStore{
		data: make(map[uint64]*Log),
	}

	fsm := MockFSM{}
	transport := NewNetTransport(&config)

	raft := NewRaft(&config, &logStore, &fsm, transport)
	raft.clusterState.Print()
	raft.Start()

	// create the first log with the init cluster
	raft.InitCluster([]Server{
		{ID: "server-1", Addr: "127.0.0.1:5051"},
		{ID: "server-2", Addr: "127.0.0.1:5052"},
		{ID: "server-3", Addr: "127.0.0.1:5053"},
	})
	<-time.After(time.Second)
}

func TestEncode(t *testing.T) {
	entries := make([]*Log, 0, 500)
	for i := 0; i < 500; i++ {
		entries = append(entries, &Log{
			Type:  LogCommand,
			Index: 1,
			Term:  1,
			Data:  nil,
		})
	}
	req := &AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Term:              1,
		LeaderCommitIndex: 0,
		Entries:           entries,
	}
	// dung do van de WriteBinaryTo(w io.Writer) laok dung

	fmt.Println("len payload:", len(req.Encode()))
}

func TestEncodeBinary(t *testing.T) {
	entries := make([]*Log, 0, 500)
	for i := 0; i < 500; i++ {
		entries = append(entries, &Log{
			Type:  LogCommand,
			Index: uint64(i + 1),
			Term:  1,
			Data:  nil,
		})
	}
	req := &AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Term:              1,
		LeaderCommitIndex: 0,
		Entries:           entries,
	}
	buffer := bytes.NewBuffer(nil)
	n := req.WriteBinaryTo(buffer)
	a := new(AppendEntriesRequest)
	a.DecodeBinary(buffer.Bytes())
	fmt.Println("size of payload:", len(buffer.Bytes()))
	fmt.Println("n = ", n)

	resp := AppendEntriesResponse{
		Success:      true,
		Term:         2,
		LastLogIndex: 22,
		LastLogTerm:  2,
		Stop:         false,
	}

	buf := resp.EncodeBinary()
	fmt.Println("resp payload len:", len(buf))
	r2 := new(AppendEntriesResponse)
	r2.DecodeBinary(buf)
	fmt.Println(r2)
}

func TestSize(t *testing.T) {
	a := new(AppendEntriesRequest)
	a.Entries = make([]*Log, 0, 3)
	for i := 0; i < 3; i++ {
		a.Entries = append(a.Entries, &Log{
			Type:  LogClusterConfig,
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("asdsds"),
		})
	}
	fmt.Println(a.SizeBinary())
	a1 := new(AppendEntriesRequest)
	a1.DecodeBinary(a.EncodeBinary())
	for _, l := range a1.Entries {
		fmt.Print(l.ToString())
	}

	assert.Equal(t, uint32(len(a.EncodeBinary())), a.SizeBinary())
}

func TestBspool(t *testing.T) {
	buf := bspool.Get(24)
	fmt.Println("len :", len(buf), "cap:", cap(buf))
	ptr := &buf[0]
	bspool.Put(buf)
	b := bspool.Get(18)
	fmt.Println("len: ", len(b), "cap:", cap(b))
	assert.Equal(t, ptr, &b[0])
}
func TestEncodeBinary2(t *testing.T) {
	req := new(AppendEntriesRequest)
	req.Entries = make([]*Log, 0, 3)
	for i := 0; i < 3; i++ {
		req.Entries = append(req.Entries, &Log{
			Type:  LogCommand,
			Index: uint64(i + 2),
			Term:  1,
			Data:  []byte("hyes"),
		})
	}
	n := req.SizeBinary()
	fmt.Println("len = ", n)
	buf := bspool.Get(n)
	fmt.Println("pick out buf len:", len(buf), "cap:", cap(buf))
	req.WriteTo(buf)
	r := new(AppendEntriesRequest)
	r.DecodeBinary(buf)
	r.Print()
}
func TestLogStore(t *testing.T) {
	store := InmemLogStore{
		data: make(map[uint64]*Log),
	}

	start := time.Now()
	n := 1000000
	for i := 0; i < n; i++ {
		store.data[uint64(i+1)] = &Log{
			Type:  LogCommand,
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("hay ok done it now"),
		}
	}
	fmt.Println("total time:", time.Since(start))
	fmt.Println(unsafe.Sizeof(store.data))
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	runtime.GC()
	fmt.Println(memstats.HeapAlloc)
}

func TestHandleAppendEntries(t *testing.T) {
	config := Config{
		ServerID:     "server-2",
		ServerAddr:   "127.0.0.1:5052",
		LogBatchSize: 512,
	}

	logStore := InmemLogStore{
		data: make(map[uint64]*Log),
	}

	fsm := MockFSM{}
	transport := NewNetTransport(&config)
	raft := NewRaft(&config, &logStore, &fsm, transport)
	//raft.Start()
	clusterConfig := ClusterConfig{
		Servers: []Server{
			{ID: "server-1", Addr: "127.0.0.1:5051"},
			{ID: "server-2", Addr: "127.0.0.1:5052"},
			{ID: "server-3", Addr: "127.0.0.1:5053"},
		},
	}

	entries := []*Log{
		{
			Type:  LogClusterConfig,
			Index: 1,
			Term:  1,
			Data:  clusterConfig.Encode(),
		},
		{
			Type:  LogCommand,
			Index: 2,
			Term:  1,
			Data:  []byte("something"),
		},
	}

	req := &AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Term:              1,
		LeaderCommitIndex: 0,
		Entries:           entries,
	}

	respChan := make(chan RPCResponse, 1)

	rpc := &RPC{
		Type:          RpcAppendEntries,
		Request:       req,
		CorrelationID: 1,
		RespChan:      respChan,
		ExitChan:      nil,
	}

	raft.HandleAppendEntries(req, rpc)
	respRpc := <-rpc.RespChan
	resp := respRpc.Response.(*AppendEntriesResponse)
	resp.Print()
	raft.clusterState.Print()
	l, err := raft.logStore.GetLog(2)
	if err != nil {
		t.Fail()
		return
	}

	assert.Equal(t, l.Index, uint64(2))
}

func TestInitRaft(t *testing.T) {

	config := &Config{
		ServerID:           "server-1",
		ServerAddr:         "127.0.0.1:5051",
		LogBatchSize:       1000,
		ClusterPort:        5051,
		ClusterIPAddr:      "127.0.0.1",
		MaxLogPerTrip:      512,
		LogDir:             "server-2_test_logs",
		BatchTimeout:       time.Millisecond,
		MaxLogFileSize:     32 * 1024 * 1024,
		MaxLogEntryInCache: (1 << 16),
	}

	start := time.Now()
	logStore, err := CreateDiskLogStore(config)
	if err != nil {
		t.Error(err)
		return
	}

	fsm := MockFSM{}
	transport := NewNetTransport(config)
	r := NewRaft(config, logStore, &fsm, transport)
	fmt.Println("init time:", time.Since(start))
	r.PrintBaseState(r.SelfID)
}

func TestS(t *testing.T) {
	str := "khoapha"
	buf := []byte(str)
	b := []byte(str)
	println(&buf[0])
	println(&b[0])
}

func TestLeaderElection(t *testing.T) {

	config := &Config{
		ServerID:           "server-1",
		ServerAddr:         "127.0.0.1:5051",
		LogBatchSize:       1024,
		ClusterPort:        5051,
		ClusterIPAddr:      "127.0.0.1",
		MaxLogPerTrip:      512,
		LogDir:             "server-1_test_logs",
		BatchTimeout:       time.Millisecond,
		MaxLogFileSize:     32 * 1024 * 1024,
		MaxLogEntryInCache: (1 << 16),
		MinNumLogFile:      3,
		ClientPort:         7051,
		ClientIPAddr:       "127.0.0.1",
	}

	start := time.Now()
	logStore, err := CreateDiskLogStore(config)
	if err != nil {
		t.Error(err)
		return
	}

	fsm := MockFSM{}
	transport := NewNetTransport(config)
	r := NewRaft(config, logStore, &fsm, transport)
	fmt.Println("init time:", time.Since(start))
	r.PrintBaseState(r.SelfID)
	r.Start()

}
