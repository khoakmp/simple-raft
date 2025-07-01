package simpleraft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestClient(t *testing.T) {
	logDir := "server-1_test_logs"
	//deleteAllFilesInDir(logDir)

	logStore, _ := CreateDiskLogStore(&Config{
		MaxLogFileSize:     32 * 1024 * 1024,
		LogDir:             logDir,
		MaxLogEntryInCache: (1 << 21),
	})

	logStore.logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	r1 := createRaft("server-1", "127.0.0.1:5051", logStore)
	// createRaft create and run raft in another goroutine

	//logStore.SetRaft(r1)
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
			<-time.After(time.Millisecond * 200)
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
	for _, f := range r1.leaderState.followers {
		f.Print()
	}
	//bspool.PrintCounters()
}
