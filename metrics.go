package simpleraft

import (
	"sync/atomic"
	"time"
)

type AppMetrics struct {
	batchCount uint64
	batchSizes []int

	logSliceAllocateCnt uint32
	logSliceHitCnt      uint32

	getLogFromDiskCnt uint32
	//storeTimes          []time.Duration
	writeDiskCnt        uint64
	totalBytesWriteDisk uint64
	totalTimeWrireDisk  time.Duration

	totalTimeAddLogToCache time.Duration

	totalLogLoadFromDisk uint64
	flushToClientCount   uint32

	getLogDataBufferCnt uint32
	putLogDatabufferCnt uint32
}

var appMetrics AppMetrics

func init() {
	appMetrics.batchSizes = make([]int, 0)
}

func (m *AppMetrics) AddBatch(sz int) {
	m.batchCount++
	// m.batchSizes = append(m.batchSizes, sz)
}

func (m *AppMetrics) IncrBytesWriteDisk(n uint64) {
	m.totalBytesWriteDisk += n
}

func (m *AppMetrics) IncrWriteDiskTime(dur time.Duration) {
	m.totalTimeWrireDisk += dur
	m.writeDiskCnt++
}
func (m *AppMetrics) IncrAddCacheLogTime(dur time.Duration) {
	m.totalTimeAddLogToCache += dur
}

func (m *AppMetrics) IncrTotalLogGetFromDisk(del uint64) {
	atomic.AddUint64(&m.totalLogLoadFromDisk, del)
}

func (m *AppMetrics) IncrGetLogDataBufferCnt(d uint32) {
	atomic.AddUint32(&m.getLogDataBufferCnt, d)
}

func (m *AppMetrics) IncrPutLogDataBufferCnt(d uint32) {
	atomic.AddUint32(&m.putLogDatabufferCnt, d)
}
