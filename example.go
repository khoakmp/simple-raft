package simpleraft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func samplePeerLoop(serverID, addr string) {
	<-time.After(time.Millisecond * 20)
	// auto recv conn -> create port -> and file descriptor dung?
	fmt.Println("peer follower ", serverID)
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
	connReader := bufio.NewReaderSize(conn, 32768)

	var lastIdx uint64
	var lastTerm uint64
	cnt := 0
	//buffer := bytes.NewBuffer(nil)

	for {
		cnt++
		_, err := io.ReadFull(connReader, headBuffer[:])
		if err == io.EOF {
			break
		}

		key, cid, sz := decodeHeader(headBuffer[:])

		//fmt.Println(serverID, " recv: [", "key:", key, " cid:", cid, " sz:", sz, "]")
		buf := bspool.Get(sz)
		io.ReadFull(connReader, buf)

		if key == RpcAppendEntries {
			payload := new(AppendEntriesRequest)
			payload.DecodeBinary(buf)
			//payload.Print()
			bspool.Put(buf)
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
				if l == 1 && payload.Entries[0].Type == LogClusterConfig {
					fmt.Println(serverID, "recv cluster config log: ", string(payload.Entries[0].Data))
				}
				/* fmt.Printf("%s recv entries in [%d : %d]\n",
				serverID, payload.Entries[0].Index, payload.Entries[l-1].Index) */
			} else {
				resp.LastLogIndex = lastIdx
				resp.LastLogTerm = lastTerm
			}

			respBuffer := resp.EncodeBinary()
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
func CheckReplicate() {
	config := Config{
		ServerID:      "server-1",
		ServerAddr:    "127.0.0.1:5051",
		LogBatchSize:  1000,
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
		BatchTimeout:  time.Millisecond * 5,
		MaxLogPerTrip: 400,
	}

	go samplePeerLoop("server-2", "127.0.0.1:5052")
	go samplePeerLoop("server-3", "127.0.0.1:5053")

	logStore := InmemLogStore{
		data:   make(map[uint64]*Log),
		config: &config,
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
	<-time.After(time.Millisecond * 25)
	// 1. khi ma send
	raft.PrintClusterState()

	/* sendCmdFn := func() {
		err := raft.ApplyCommand(Command{
			Data: []byte("sample thing"),
		}).Err()

		if err != nil && err != ErrReqTimeout {
			t.Error(err)
		}
	} */
	start := time.Now()
	futs := make([]*ApplyLogReqFuture, 0, 1000000)

	for i := 0; i < 1000000; i++ {
		futs = append(futs, raft.ApplyCommand(Command{
			Data: []byte("sample thing"),
		}))
	}

	//sendCmdFn()

	for _, fut := range futs {
		if err := fut.Err(); err != nil {
			fmt.Println(err)
			return
		}
	}

	/* futs = futs[:0]
	for i := 0; i < 2000; i++ {
		futs = append(futs, raft.ApplyCommand(Command{
			Data: []byte("sample thing"),
		}))
	}


	for _, fut := range futs {
		if err := fut.Err(); err != nil {
			t.Error(err)
			return
		}
	} */

	for _, f := range raft.leaderState.followers {
		f.Print()
	}

	fmt.Println("total time:", time.Since(start))
	//<-time.After(time.Second)
}

func RunRaftNode(serverID, serverAddr string) {
	//DeleteAllFilesOfDir(fmt.Sprintf("%s_test_logs", serverID))
	r := createAndRunRaftNode(serverID, serverAddr)
	r.wg.Wait()
}

func CreateRaftNode(serverID, serverAddr string) *Raft {
	return createRaftNode(serverID, serverAddr)
}

func createRaftNode(serverID, serverAddr string) *Raft {
	portStr := strings.Split(serverAddr, ":")[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	config := Config{
		ServerID:              ServerID(serverID),
		ServerAddr:            serverAddr,
		LogBatchSize:          2048,
		ClusterPort:           port,
		ClusterIPAddr:         "127.0.0.1",
		BatchTimeout:          time.Millisecond * 5,
		MaxLogPerTrip:         1024,
		LogDir:                fmt.Sprintf("%s_test_logs", serverID),
		MaxLogFileSize:        32 * 1024 * 1024,
		MaxLogEntryInCache:    (1 << 17),
		ClientPort:            port + 2000,
		ClientIPAddr:          "127.0.0.1",
		MinNumLogFile:         3,
		ClusterConfigFilename: fmt.Sprintf("%s_config/cluster.yaml", serverID),
		LastVoteFilename:      fmt.Sprintf("%s_config/vote.yaml", serverID),
	}

	logStore, err := CreateDiskLogStore(&config)
	if err != nil {
		panic(err)
	}

	logStore.logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	fsm := MockFSM{}
	transport := NewNetTransport(&config)

	raft := NewRaft(&config, logStore, &fsm, transport)

	return raft
}

func createAndRunRaftNode(serverID, serverAddr string) *Raft {
	raft := createRaftNode(serverID, serverAddr)
	raft.Start()
	return raft
}

func RunRaftLeader() {
	//logDir := "server-0_test_logs"
	//deleteAllFilesInDir(logDir)

	//logStore.logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	r1 := createAndRunRaftNode("server-1", "127.0.0.1:5051")

	//logStore.SetRaft(r1)
	//r2 := createRaft("server-2", "127.0.0.1:5052")
	//r3 := createRaft("server-3", "127.0.0.1:5053")

	/* r1.InitCluster([]Server{
		{ID: "server-1", Addr: "127.0.0.1:5051"},
		{ID: "server-2", Addr: "127.0.0.1:5052"},
		{ID: "server-3", Addr: "127.0.0.1:5053"},
	}) */

	go func() {
		var memstats runtime.MemStats
		for {
			runtime.ReadMemStats(&memstats)
			fmt.Println("Heap Alloc:", memstats.HeapAlloc, "NumGC:", memstats.NumGC)
			<-time.After(time.Millisecond * 500)
		}
	}()

	conn, err := net.Dial("tcp", "127.0.0.1:7051")
	if err != nil {
		fmt.Println("failed to connect to server: ", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	numReq := uint32(10000)
	bufReader := bufio.NewReaderSize(conn, 1024)
	start := time.Now()
	nt := uint32(100)

	go func() {
		defer wg.Done()
		for i := uint32(0); i < numReq*nt; i++ {
			var headbuf [10]byte
			_, err := io.ReadFull(bufReader, headbuf[:])
			if err != nil {
				fmt.Println("Failed to read header:", err)
				return
			}
			sz := binary.BigEndian.Uint32(headbuf[6:10])
			buf := make([]byte, sz)
			_, err = io.ReadFull(bufReader, buf)
			if err != nil {
				fmt.Println("Failed to read payload:", err)
				return
			}
		}

		fmt.Println("Total time for getting all response:", time.Since(start))
	}()

	bufWriter := bufio.NewWriterSize(conn, 4096)

	payloadBuf := []byte("kmp something to handle")
	for j := uint32(0); j < nt; j++ {
		for i := uint32(0); i < numReq; i++ {
			var headbuf [10]byte
			binary.BigEndian.PutUint16(headbuf[0:2], CommandType)
			binary.BigEndian.PutUint32(headbuf[2:6], j*(numReq)+i+1)
			binary.BigEndian.PutUint32(headbuf[6:10], uint32(len(payloadBuf)))
			bufWriter.Write(headbuf[:])
			bufWriter.Write(payloadBuf)
		}
		bufWriter.Flush()
	}

	//fmt.Println("Total time to commit:", time.Since(start))
	//<-time.After(time.Second)
	wg.Wait()
	r1.leaderState.commitState.Print()

	fmt.Println("log slice allocated count:", appMetrics.logSliceAllocateCnt)
	fmt.Println("log slice hit count:", appMetrics.logSliceHitCnt)
	fmt.Println("byte slice allocated count:", atomic.LoadUint32(&bspool.allocatedCount))
	fmt.Println("byte slice get count:", atomic.LoadUint32(&bspool.getCount))
	fmt.Println("byte slice put count:", atomic.LoadUint32(&bspool.putCount))

	fmt.Println("hit disk count:", atomic.LoadUint32(&appMetrics.getLogFromDiskCnt))
	fmt.Println("Number of time write to disk:", appMetrics.writeDiskCnt)
	fmt.Println("Total bytes write to disk:", appMetrics.totalBytesWriteDisk)
	fmt.Println("total time for write to disk:", appMetrics.totalTimeWrireDisk)
	fmt.Println("total logs load from disk:", appMetrics.totalLogLoadFromDisk)
	fmt.Println("flush to client count:", appMetrics.flushToClientCount)
	fmt.Println("batch logs count:", appMetrics.batchCount)

	fmt.Println("get log data buffer cnt:", appMetrics.getLogDataBufferCnt)
	fmt.Println("put log data buffer cnt:", appMetrics.putLogDatabufferCnt)

	for _, f := range r1.leaderState.followers {
		f.Print()
	}
	bspool.PrintCounters()
}
