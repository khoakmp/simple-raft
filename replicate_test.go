package simpleraft

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestReplicate(t *testing.T) {
	config := Config{
		ServerID:      "server-1",
		ServerAddr:    "127.0.0.1:5051",
		LogBatchSize:  2000,
		ClusterPort:   5051,
		ClusterIPAddr: "127.0.0.1",
		BatchTimeout:  time.Millisecond * 5,
		MaxLogPerTrip: 100,
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
	go func() {
		var memstats runtime.MemStats
		for {
			runtime.ReadMemStats(&memstats)
			fmt.Println("Heap Alloc:", memstats.HeapAlloc)
			<-time.After(time.Millisecond * 200)
		}
	}()
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

	numReq := 1000000
	data := "sample thing"

	futs := make([]*ApplyLogReqFuture, 0, numReq)

	for j := 0; j < 1; j++ {
		start := time.Now()
		for i := 0; i < numReq; i++ {
			buf := bspool.Get(uint32(len(data)))
			copy(buf, data)

			futs = append(futs, raft.ApplyCommand(Command{
				Data: buf,
			}))
		}

		for _, fut := range futs {
			if err := fut.Err(); err != nil {
				t.Error(err)
				return
			}
			bspool.Put(fut.log.Data)
		}

		for _, f := range raft.leaderState.followers {
			f.Print()
		}

		fmt.Println("total time:", time.Since(start))
		futs = futs[:0]
		<-time.After(time.Millisecond * 1000)
	}

	//<-time.After(time.Millisecond * 5000)
	runtime.GC()
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	fmt.Println("Heap Alloc:", memstats.HeapAlloc)

}

func createRaft(serverID, serverAddr string, logStore LogStore) *Raft {
	ps := strings.Split(serverAddr, ":")[1]
	port, err := strconv.Atoi(ps)

	if err != nil {
		fmt.Println(err)
		return nil
	}
	fmt.Println("port:", port)
	fmt.Println(port)

	config := Config{
		ServerID:      ServerID(serverID),
		ServerAddr:    serverAddr,
		LogBatchSize:  2048,
		ClusterPort:   port,
		ClusterIPAddr: "127.0.0.1",
		BatchTimeout:  time.Millisecond * 5,
		MaxLogPerTrip: 1024,
		ClientPort:    port + 2000,
		ClientIPAddr:  "127.0.0.1",
	}

	fsm := MockFSM{}
	transport := NewNetTransport(&config)

	raft := NewRaft(&config, logStore, &fsm, transport)
	raft.clusterState.Print()
	raft.Start()
	return raft
}

func TestReplicateReal(t *testing.T) {
	logDir := "server-0_test_logs"
	//deleteAllFilesInDir(logDir)

	logStore, _ := CreateDiskLogStore(&Config{
		MaxLogFileSize:     32 * 1024 * 1024,
		LogDir:             logDir,
		MaxLogEntryInCache: (1 << 21),
	})

	logStore.logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	r1 := createRaft("server-1", "127.0.0.1:5051", logStore)
	//r2 := createRaft("server-2", "127.0.0.1:5052")
	//r3 := createRaft("server-3", "127.0.0.1:5053")
	r1.InitCluster([]Server{
		{ID: "server-1", Addr: "127.0.0.1:5051"},
		{ID: "server-2", Addr: "127.0.0.1:5052"},
		{ID: "server-3", Addr: "127.0.0.1:5053"},
	})

	go func() {
		var memstats runtime.MemStats
		for {
			runtime.ReadMemStats(&memstats)
			fmt.Println("Heap Alloc:", memstats.HeapAlloc, "NumGC:", memstats.NumGC)
			<-time.After(time.Millisecond * 500)
		}
	}()

	numReq := 10000
	var futs []*ApplyLogReqFuture = make([]*ApplyLogReqFuture, 0, numReq)
	str := "set key sdsd value sdsjasdskmds_sssdadsdsa"
	start := time.Now()

	for j := 0; j < 100; j++ {
		//start := time.Now()
		//var futs []*ApplyLogReqFuture = make([]*ApplyLogReqFuture, 0, numReq)

		for i := 0; i < numReq; i++ {
			//buf := bspool.Get(uint32(len(str)))
			futs = append(futs, r1.ApplyCommand(Command{
				Data: []byte(str),
			}))
		}

		for _, fut := range futs {
			err := fut.Err()
			if err != nil {
				t.Error(err)
				return
			}

			//logpool.Put(fut.log)
			fut.log = nil
			//bspool.Put(fut.log.Data)
		}

		//fmt.Println("total time:", time.Since(start))
		//r2.clusterState.Print()
		//r3.clusterState.Print()
		//<-time.After(time.Millisecond * 10)
		/* var memstats runtime.MemStats

		//runtime.GC()
		runtime.ReadMemStats(&memstats)
		*/
		//r1.leaderState.commitState.Print()
		//fmt.Println("Number of time: write to disk:", len(appMetrics.storeTimes))

		/* for _, f := range r1.leaderState.followers {
			f.Print()
		} */
		futs = futs[:0]
	}
	fmt.Println("Total time to commit:", time.Since(start))
	<-time.After(time.Second)

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
	bspool.PrintCounters()
}
