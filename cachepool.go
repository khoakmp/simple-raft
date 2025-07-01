package simpleraft

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

type bsPool struct {
	pools       [24]sync.Pool
	counters    [24]uint32
	putCounters [24]uint32

	allocatedCount uint32
	getCount       uint32
	putCount       uint32
}

var bspool bsPool

func (p *bsPool) PrintCounters() {
	fmt.Println("Get counters:")
	for i := 0; i < 24; i++ {
		fmt.Println(i, ":", p.counters[i])
	}
	fmt.Println("Put counters:")
	for i := 0; i < 24; i++ {
		fmt.Println(i, ":", p.putCounters[i])
	}
}
func (p *bsPool) Get(sz uint32) []byte {
	atomic.AddUint32(&p.getCount, 1)

	if sz <= 0 {
		return nil
	}
	idx := bits.Len32(sz)
	if (1 << (idx - 1)) == sz {
		idx--
	}
	atomic.AddUint32(&p.counters[idx], 1)
	if idx >= 24 {

		return make([]byte, sz)
	}
	ptr := p.pools[idx].Get()

	if ptr == nil {
		atomic.AddUint32(&p.allocatedCount, 1)
		//fmt.Printf("create new byte slice with sz: %d, cap: %d\n", sz, (1 << idx))
		return make([]byte, sz, (1 << idx))
	}
	pt := ptr.(unsafe.Pointer)
	return unsafe.Slice((*byte)(pt), (1 << idx))[:sz]
}

func (p *bsPool) Put(buf []byte) {
	atomic.AddUint32(&p.putCount, 1)
	c := cap(buf)
	if c > (1 << 23) {
		return
	}
	idx := bits.Len32(uint32(c))
	if (1 << (idx - 1)) == c {
		idx--
	} else {
		return
	}

	atomic.AddUint32(&p.putCounters[idx], 1)

	ptr := unsafe.Pointer(&buf[0])
	p.pools[idx].Put(ptr)
}

type logPointerSlicePool struct {
	pools [20]sync.Pool
	//allocatedCount uint32
}

func (p *logPointerSlicePool) Get(sz uint32) []*Log {
	if (1 << 19) < sz {
		return make([]*Log, sz)
	}
	idx := bits.Len32(sz)
	if (1 << (idx - 1)) == sz {
		idx--
	}

	ptr := p.pools[idx].Get()
	if ptr == nil {
		atomic.AddUint32(&appMetrics.logSliceAllocateCnt, 1)
		return make([]*Log, sz, (1 << idx))
	}

	atomic.AddUint32(&appMetrics.logSliceHitCnt, 1)

	pt := ptr.(unsafe.Pointer)
	return unsafe.Slice((**Log)(pt), (1 << idx))[:sz]
}
func (p *logPointerSlicePool) Put(buf []*Log) {
	if cap(buf) > (1 << 19) {
		return
	}

	idx := bits.Len32(uint32(cap(buf))) - 1

	ptr := unsafe.Pointer(&buf[0])
	p.pools[idx].Put(ptr)
}

var logPtrSlicePool logPointerSlicePool

type pointerSlicePool struct {
	pools [20]sync.Pool
}

func (p *pointerSlicePool) Get(sz uint32) []unsafe.Pointer {
	idx := 0
	if (1 << idx) < sz {
		return make([]unsafe.Pointer, sz)
	}
	for (1 << idx) < sz {
		idx++
	}

	ptr := p.pools[idx].Get()
	if ptr == nil {
		return make([]unsafe.Pointer, sz, (1 << idx))
	}
	pt := ptr.(unsafe.Pointer)
	return unsafe.Slice((*unsafe.Pointer)(pt), (1 << idx))[:sz]
}

func (p *pointerSlicePool) Put(buf []unsafe.Pointer) {
	if cap(buf) > (1 << 19) {
		return
	}

	bits.Len32(uint32(len(buf)))

}

type ApplyLogFuturePool struct {
	pools [20]sync.Pool
}

func (p *ApplyLogFuturePool) Get(sz uint32) []*ApplyLogReqFuture {
	if sz > (1 << 19) {
		return make([]*ApplyLogReqFuture, sz)
	}
	idx := bits.Len32(sz)
	if (1 << (idx - 1)) == sz {
		idx--
	}
	ptr := p.pools[idx].Get()
	if ptr == nil {
		return make([]*ApplyLogReqFuture, sz, (1 << idx))
	}
	pt := ptr.(unsafe.Pointer)
	return unsafe.Slice((**ApplyLogReqFuture)(pt), (1 << idx))[:sz]
}
func (p *ApplyLogFuturePool) Put(buf []*ApplyLogReqFuture) {
	if cap(buf) > (1<<19) || cap(buf) == 0 {
		return
	}
	idx := bits.Len32(uint32(cap(buf))) - 1
	ptr := unsafe.Pointer(&buf[0])
	p.pools[idx].Put(ptr)
}

var logfutPool ApplyLogFuturePool

type LogPool struct {
	pool sync.Pool
}

func (p *LogPool) Get() *Log {
	ptr := p.pool.Get()
	if ptr == nil {
		return new(Log)
	}
	pt := ptr.(unsafe.Pointer)
	return (*Log)(pt)
}

func (p *LogPool) Put(l *Log) {
	ptr := unsafe.Pointer(l)
	p.pool.Put(ptr)
}

var logpool LogPool
