package simpleraft

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestLogSlicePool(t *testing.T) {
	n := 100000
	logs := make([]*Log, n)
	for i := 0; i < n; i++ {
		logs[i] = &Log{
			Type:  LogClusterConfig,
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("sss"),
		}
	}

	num := 1000
	idxes := make([]int32, num)
	for i := 0; i < num; i++ {
		var x int32
		for x == 0 {
			x = rand.Int31n(int32(1000))
		}
		idxes[i] = x
	}

	start := time.Now()
	for i := 0; i < num; i++ {
		buf := logPtrSlicePool.Get(uint32(idxes[i]))
		//buf := make([]*Log, idxes[i])
		copy(buf, logs[:idxes[i]])
		logPtrSlicePool.Put(buf)
	}
	fmt.Println("total time:", time.Since(start))
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	fmt.Println("heap alloc:", memstats.HeapAlloc)

}

func TestBsPool(t *testing.T) {
	//buf := make([]byte, 100000)

}

func TestUnsafePointer(t *testing.T) {
	/* var futs []*ApplyLogReqFuture
	x := (*[3]uintptr)(unsafe.Pointer(&futs))
	println(x[0], x[1], x[2])
	buf := make([]byte, 10)
	x = (*[3]uintptr)(unsafe.Pointer(&buf))
	println(x[0], x[1], x[2])
	buf = buf[1:]
	x = (*[3]uintptr)(unsafe.Pointer(&buf))
	println(x[0], x[1], x[2]) */

	buf := logfutPool.Get(256)
	x := &buf[0]
	println(x)
	logfutPool.Put(buf)
	logfutPool.Put(buf)
	b := logfutPool.Get(245)
	println(&b[0])

}

func TestLogPool(t *testing.T) {
	start := time.Now()
	//var buf []byte
	var log *Log

	for t := 0; t < 100; t++ {
		for i := 0; i < 10000; i++ {
			log = logpool.Get()
			log.Index = 1
			logpool.Put(log)
		}
	}

	fmt.Println("total time:", time.Since(start))
	var memstat runtime.MemStats
	runtime.ReadMemStats(&memstat)
	fmt.Println("heap alloc:", memstat.HeapAlloc)
	fmt.Println("numGC:", memstat.NumGC)
}
