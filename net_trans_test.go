package simpleraft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

/*
	 func samplePeerLoop(serverID, addr string) {
		<-time.After(time.Millisecond * 20)
		// auto recv conn -> create port -> and file descriptor dung?

		l, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer l.Close()

		var headBuffer [10]byte

		conn, err := l.Accept()

		fmt.Println("accepted new conn")
		if err != nil {
			fmt.Println(err)
			return
		}
		connWriter := bufio.NewWriterSize(conn, 1024)
		var lastIdx uint64
		var lastTerm uint64
		cnt := 0

		for {
			cnt++
			_, err := io.ReadFull(conn, headBuffer[:])
			if err == io.EOF {
				break
			}

			key, cid, sz := decodeHeader(headBuffer[:])

			//fmt.Println(serverID, " recv: [", "key:", key, " cid:", cid, " sz:", sz, "]")
			buf := make([]byte, sz)
			io.ReadFull(conn, buf)

			if key == RpcAppendEntries {
				payload := new(AppendEntriesRequest)
				payload.Decode(buf)
				//payload.Print()

				resp := AppendEntriesResponse{
					Success: true,
					Term:    payload.Term,
					Stop:    false,
				}
				l := len(payload.Entries)
				if l > 0 {
					resp.LastLogIndex = payload.Entries[l-1].Index
					resp.LastLogTerm = payload.Entries[l-1].Term

					lastIdx = payload.Entries[l-1].Index
					lastTerm = payload.Entries[l-1].Term
					//fmt.Printf("%s recv entries in [%d : %d]\n",serverID, payload.Entries[0].Index, payload.Entries[l-1].Index)
				} else {
					resp.LastLogIndex = lastIdx
					resp.LastLogTerm = lastTerm
				}
				respBuffer := resp.Encode()
				//fmt.Printf("%s send  resp LastLogIdx : %d\n", serverID, resp.LastLogIndex)
				binary.BigEndian.PutUint16(headBuffer[:2], RpcRespAppendEntries)
				binary.BigEndian.PutUint32(headBuffer[2:6], cid)
				binary.BigEndian.PutUint32(headBuffer[6:10], uint32(len(respBuffer)))
				connWriter.Write(headBuffer[:])
				connWriter.Write(respBuffer)
				connWriter.Flush()
			}

			//fmt.Println(serverID, "recv ", cnt, "rpc requests")
		}
		conn.Close()
	}
*/
func TestConnect(t *testing.T) {

	go samplePeerLoop("server-2", "127.0.0.1:5052")

	time.Sleep(time.Millisecond * 100)
	config := Config{
		ServerID:      "server-1",
		ServerAddr:    "127.0.0.1:5051",
		LogBatchSize:  200,
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
	}

	var trans RPCTransport = NewNetTransport(&config)
	req := &AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Term:              1,
		LeaderCommitIndex: 0,
		Entries:           nil,
	}
	var wg sync.WaitGroup

	f := func() {
		defer wg.Done()
		resp, err := trans.AppendEntries("127.0.0.1:5052", req)
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, resp.Success, true)
		assert.Equal(t, resp.Term, uint64(1))
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go f()
	}
	wg.Wait()
}

func TestTransportListen(t *testing.T) {
	config := Config{
		ServerID:      "server-2",
		ServerAddr:    "127.0.0.1:5052",
		LogBatchSize:  200,
		ClusterPort:   5052,
		ClusterIPAddr: "127.0.0.1",
	}

	trans := NewNetTransport(&config)
	rpcChan := trans.RpcChan()

	// both trans listener and first goroutine is follower
	go func() {
		var lastIdx uint64
		var lastTerm uint64

		for {
			rpc, ok := <-rpcChan
			if !ok {
				return
			}

			if rpc.Type == RpcAppendEntries {
				req := rpc.Request.(*AppendEntriesRequest)
				req.Print()
				l := len(req.Entries)
				resp := &AppendEntriesResponse{
					Success: true,
					Term:    req.Term,
				}

				if l > 0 {
					resp.LastLogIndex = req.Entries[l-1].Index
					resp.LastLogTerm = req.Entries[l-1].Term
					lastIdx = req.Entries[l-1].Index
					lastTerm = req.Entries[l-1].Term
				} else {
					resp.LastLogIndex = lastIdx
					resp.LastLogTerm = lastTerm
				}
				rpc.Respond(resp)

			}
		}
	}()
	go func() {
		<-time.After(time.Millisecond * 10)
		sampleLeader("127.0.0.1:5052")
	}()

	go trans.ListenAndRun()
	<-time.After(time.Second)
}

func sampleLeader(folAddr string) {
	conn, err := net.Dial("tcp", folAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
	var cnt uint32 = 0
	req := AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Term:              1,
		LeaderCommitIndex: 0,
		Entries:           nil,
	}
	payload := req.Encode()
	buffer := bytes.NewBuffer(nil)
	var hbuf [10]byte

	for i := 0; i < 3; i++ {
		buffer.Reset()
		cnt++
		binary.BigEndian.PutUint16(hbuf[:2], RpcAppendEntries)
		binary.BigEndian.PutUint32(hbuf[2:6], cnt)
		binary.BigEndian.PutUint32(hbuf[6:10], uint32(len(payload)))
		buffer.Write(hbuf[:])
		buffer.Write(payload)
		_, err := conn.Write(buffer.Bytes())

		if err != nil {
			fmt.Println(err)
			return
		}
		io.ReadFull(conn, hbuf[:])
		apiKey := binary.BigEndian.Uint16(hbuf[:2])
		cid := binary.BigEndian.Uint32(hbuf[2:6])
		sz := binary.BigEndian.Uint32(hbuf[6:10])
		fmt.Printf("Leader recv : [apikey: %d, cid: %d, sz: %d]\n", apiKey, cid, sz)
		buf := make([]byte, sz)
		io.ReadFull(conn, buf)
		resp := new(AppendEntriesResponse)
		resp.Decode(buf)
		fmt.Println("resp.Success:", resp.Success)

	}
	conn.Close()
}

func TestTrans(t *testing.T) {
	var start time.Time
	go func() {
		time.After(time.Millisecond * 20)
		conn, err := net.Dial("tcp", "127.0.0.1:5051")
		if err != nil {
			fmt.Println(err)
			return
		}
		start = time.Now()
		// time to write tam 100kb < 150 micro sec

		buf := make([]byte, 100000)
		for i := 0; i < 1; i++ {
			conn.Write(buf)
		}
		fmt.Println("write time:", time.Since(start))

	}()
	l, err := net.Listen("tcp", "127.0.0.1:5051")
	if err != nil {
		fmt.Println(err)
		return
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	connReader := bufio.NewReaderSize(conn, 100000)
	buf := make([]byte, 100000)

	for i := 0; i < 1; i++ {
		io.ReadFull(connReader, buf)
	}

	conn.Close()
	fmt.Println("total time:", time.Since(start))
}

func TestPipeline(t *testing.T) {
	config := Config{
		ServerID:      "server-1",
		ServerAddr:    "127.0.0.1:5051",
		LogBatchSize:  1000,
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
		BatchTimeout:  time.Millisecond * 5,
		MaxLogPerTrip: 500,
	}

	go samplePeerLoop("server-2", "127.0.0.1:5052")
	<-time.After(time.Millisecond * 50)
	trans := NewNetTransport(&config)
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
	start := time.Now()
	// tien hanh create pipeline dung?//
	p, err := trans.CreatePipeline("127.0.0.1:5052")

	if err != nil {
		t.Error(err)
		return
	}
	// send 400 hit dung

	n := 401

	cids := make([]uint32, 0, n)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			resp, ok := <-p.ResponseChan()
			if !ok && i < 3 {
				t.Fail()
				fmt.Println("not recv enough resp")
				return
			}
			assert.Equal(t, resp.CorrelationID, cids[i])
		}
	}()
	for i := 0; i < n; i++ {
		cid, err := p.AppendEntries(req)
		if err != nil {
			t.Error(err)
			return
		}
		cids = append(cids, cid)
	}
	wg.Wait()
	fmt.Println("total time:", time.Since(start))
}
func TestMakeRPC(t *testing.T) {
	config := Config{
		ServerID:      "server-1",
		ServerAddr:    "127.0.0.1:5051",
		LogBatchSize:  1000,
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
		BatchTimeout:  time.Millisecond * 5,
		MaxLogPerTrip: 500,
	}

	go samplePeerLoop("server-2", "127.0.0.1:5052")
	<-time.After(time.Millisecond * 50)
	trans := NewNetTransport(&config)
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
	_, err := trans.AppendEntries("127.0.0.1:5052", req)
	if err != nil {
		t.Error(err)
		return
	}

	start := time.Now()
	n := 400
	for i := 0; i < n; i++ {
		_, err := trans.AppendEntries("127.0.0.1:5052", req)
		if err != nil {
			t.Error(err)
			return
		}
	}

	fmt.Println("total time:", time.Since(start))
}
func TestRequetVote(t *testing.T) {
	requestVote := &RequestVotePayload{
		CandidateID:  "server-1",
		Term:         10,
		LastLogIndex: 12,
		LastLogTerm:  9,
	}

	sz := requestVote.SizeBinary()
	fmt.Println("size:", sz)
	buf := make([]byte, sz)
	requestVote.WriteTo(buf)
	fmt.Println("complete write")
	r := createRaftNode("server-0", "127.0.0.1:5050")
	r.Start()

	<-time.After(time.Millisecond * 50)
	cfg := Config{
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
	}
	trans := NewNetTransport(&cfg)
	resp, err := trans.RequestVote("127.0.0.1:5050", requestVote)
	if err != nil {
		t.Error(err)
		return
	}
	if resp != nil {
		fmt.Println("Resp.Term:", resp.Term)
		fmt.Println("Resp.VoteGranted:", resp.VoteGranted)
	}

}
