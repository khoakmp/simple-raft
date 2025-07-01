package simpleraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func deleteAllFilesInDir(dir string) error {
	// Read all files and directories in the specified directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Iterate through entries and delete each one
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())

		// Remove file or directory
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", entryPath, err)
		}
	}

	return nil
}
func TestDiskLogStore(t *testing.T) {
	deleteAllFilesInDir("test_logs")
	store, err := CreateDiskLogStore(&Config{
		MaxLogFileSize:     64 * 1024 * 1024,
		LogDir:             "test_logs",
		MaxLogEntryInCache: (1 << 17),
	})

	if err != nil {
		t.Error(err)
		return
	}

	store.PrintMetadata()
	assert.Equal(t, store.metadata.CurrentFileNum, uint64(1))
	var logs []*Log = []*Log{
		{
			Type:  LogCommand,
			Index: 1,
			Term:  1,
			Data:  []byte("sss"),
		},
		{
			Type:  LogCommand,
			Index: 2,
			Term:  1,
			Data:  []byte("sss"),
		},
		{
			Type:  LogCommand,
			Index: 3,
			Term:  1,
			Data:  []byte("sss"),
		},
	}

	err = store.StoreBatchLog(logs)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println("Store sucess")
	finfo, _ := store.currentIndexFile.Stat()
	fmt.Println(finfo.Size())
	entries, err := store.getRangeFromFile(2, 3)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("len entries:", len(entries))
	for _, e := range entries {
		fmt.Println(e.ToString())
	}
}

func TestReadFileLog(t *testing.T) {
	start := time.Now()
	f, err := os.Open("server-2_test_logs/1")
	if err != nil {
		t.Error(err)
		return
	}

	buf, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println("Read All time:", time.Since(start))
	var offset uint32 = 0
	var logs []*Log = make([]*Log, 0)
	cnt := 0
	start = time.Now()

	for int(offset) < len(buf) {
		l := &Log{}
		// cai dong assert se take time
		l.Type = LogType(binary.LittleEndian.Uint16(buf[offset : offset+2]))
		l.Index = binary.LittleEndian.Uint64(buf[offset+2 : offset+10])
		l.Term = binary.LittleEndian.Uint64(buf[offset+10 : offset+18])
		var sz uint32 = binary.LittleEndian.Uint32(buf[offset+18 : offset+22])

		data := make([]byte, sz)
		copy(data, buf[offset+22:int(offset)+22+int(sz)])
		l.Data = data
		//fmt.Printf("log[%d].Data =%s\n", l.Index, string(l.Data))
		offset += 22 + sz
		logs = append(logs, l)
		cnt++
		//assert.Equal(t, l.Index, uint64(cnt))
		//assert.Equal(t, l.Term, uint64(1))
	}

	fmt.Println("Process buffer time:", time.Since(start))

	fmt.Println("len: ", len(logs))
	//assert.Equal(t, len(logs), 20001)
	fmt.Println("log[1].Data:", string(logs[0].Data))
	fmt.Println("log[2].Data:", string(logs[1].Data))
}
func TestWriteFileLog(t *testing.T) {
	//f, _ := os.OpenFile("logtest", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	var logs []*Log = []*Log{
		{
			Type:  LogCommand,
			Index: 1,
			Term:  1,
			Data:  []byte("sss"),
		},
		{
			Type:  LogCommand,
			Index: 2,
			Term:  1,
			Data:  []byte("sss"),
		},
		{
			Type:  LogCommand,
			Index: 3,
			Term:  1,
			Data:  []byte("sss"),
		},
	}
	// ca viec encode + decode ma no con co errod ung/
	store, err := CreateDiskLogStore(&Config{
		MaxLogFileSize:     64 * 1024 * 1024,
		LogDir:             "test_logs",
		MaxLogEntryInCache: (1 << 17),
	})

	if err != nil {
		t.Error(err)
		return
	}

	store.PrintMetadata()
	buf := store.encodeEntries(logs)
	/* 	_, err = f.Write(buf)
	   	if err != nil {
	   		t.Error(err)
	   		return
	   	} */
	entries, err := store.decodeBufferToEntries(buf, 3)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(string(entries[1].Data))
}
func TestLogCache(t *testing.T) {

	config := &Config{
		MaxLogFileSize:     32 * 1024 * 1024,
		LogDir:             "test_logs",
		MaxLogEntryInCache: (1 << 17),
	}

	cache := NewLogCache(config)
	n := 1000000
	logs := make([]*Log, n)
	for i := 0; i < n; i++ {
		logs[i] = &Log{
			Type:  LogCommand,
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("hey"),
		}
	}

	numBatch := 10
	for i := 0; i < numBatch; i++ {
		cache.AddBatch(logs[i*(int(n/numBatch)) : (i+1)*(int(n/numBatch))])
		fmt.Println("After add batch ", i+1)
		cache.PrintMetadata()
	}
	runtime.GC()

	entries := cache.GetRange(868931, 868943)

	fmt.Println("len entries:", len(entries))
	for _, e := range entries {
		fmt.Print(e.ToString())
	}

	var memstat runtime.MemStats
	runtime.ReadMemStats(&memstat)
	fmt.Println("HeapAlloc:", memstat.HeapAlloc)
	fmt.Println("NumGC:", memstat.NumGC)
	<-time.After(time.Second)
	runtime.ReadMemStats(&memstat)
	fmt.Println("HeapAlloc:", memstat.HeapAlloc)
	fmt.Println("NumGC:", memstat.NumGC)
}

func TestReadLogfile(t *testing.T) {
	filenum := 1
	dir := "server-2_test_logs/"
	logFileName := fmt.Sprintf("%s/%d", dir, filenum)
	indexFileName := logFileName + "_index"
	indexBuf, err := os.ReadFile(indexFileName)
	if err != nil {
		t.Error(err)
		return
	}
	start := time.Now()
	logsBuf, err := os.ReadFile(logFileName)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println("read full logfile time:", time.Since(start))
	var offset uint32 = 0
	var idx uint32 = 0
	var off uint32 = 0

	idx = binary.LittleEndian.Uint32(indexBuf[off : off+4])
	if idx != 0 {
		t.Error()
		return
	}

	for offset < uint32(len(logsBuf)) {
		_, sz, err := decodeLog(logsBuf[offset:])
		if err != nil {
			t.Error(err)
			return
		}
		offset += sz
		off += 4
		if off < uint32(len(indexBuf)) {
			idx = binary.LittleEndian.Uint32(indexBuf[off : off+4])
			if idx != offset {
				t.Error()
				return
			}
		}
	}
}

func decodeLog(buf []byte) (*Log, uint32, error) {
	if len(buf) < 22 {
		return nil, 0, ErrDecodeLogFailed
	}
	l := new(Log)
	l.Type = LogType(binary.LittleEndian.Uint16(buf[2:]))
	l.Index = binary.LittleEndian.Uint64(buf[2:10])
	l.Term = binary.LittleEndian.Uint64(buf[10:18])
	sz := binary.LittleEndian.Uint32(buf[18:22])
	data := make([]byte, sz)
	copy(data, buf[22:22+sz])
	l.Data = data
	return l, 22 + sz, nil
}

func TestTruncateFile(t *testing.T) {
	f, err := os.OpenFile("test", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	_, err = f.Write([]byte("khoapham"))
	if err != nil {
		t.Error(err)
		return
	}

	err = f.Truncate(5)
	if err != nil {
		fmt.Println("failed to truncate file")
		t.Error(err)
	}
}
func TestReadIndexFile(t *testing.T) {
	start := time.Now()

	indexFile, err := os.Open("test_logs/1_index")
	if err != nil {
		t.Error(err)
		return
	}
	stat, _ := indexFile.Stat()
	buf := make([]byte, stat.Size())

	fmt.Println("open time:", time.Since(start))
	start = time.Now()

	_, err = indexFile.ReadAt(buf[:4096], 0)
	if err != nil {
		t.Error(err)
		return
	}

	// cu read 1 cach don gian theo dang do se ok hon dung
	// van de dang la g
	fmt.Println("read all time:", time.Since(start))
	/*
		start = time.Now()
		offset := 0
		indexes := make([]uint32, 0)

		for offset < len(buf) {
			idx := binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
			indexes = append(indexes, idx)
		}
		fmt.Println("Process time:", time.Since(start))
		fmt.Println("len indexes:", len(indexes)) */
}

// file index: 2MB dung
func TestReadIndexFileByBlock(t *testing.T) {
	start := time.Now()
	indexFile, err := os.Open("test_logs/1_index")
	if err != nil {
		t.Error(err)
		return
	}
	defer indexFile.Close()
	fmt.Println("open time:", time.Since(start))
	stats, _ := indexFile.Stat()

	fmt.Println("file size:", stats.Size())

	numBlock := stats.Size() / 4096
	lastBlockSize := 4096
	if stats.Size()%4096 != 0 {
		lastBlockSize = int(stats.Size()) - int(4096*numBlock)
		numBlock++
	}
	fmt.Println("last block size:", lastBlockSize)
	buf := make([]byte, lastBlockSize)

	start = time.Now()
	_, err = indexFile.ReadAt(buf, (numBlock-1)*4096)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("read last block time:", time.Since(start))
}
func TestBSearchIndex(t *testing.T) {
	start := time.Now()
	indexFile, err := os.Open("test_logs/1_index")
	if err != nil {
		t.Error(err)
		return
	}
	defer indexFile.Close()
	fmt.Println("open time:", time.Since(start))
	stats, _ := indexFile.Stat()

	fmt.Println("file size:", stats.Size())

	numBlock := stats.Size() / 4096
	lastBlockSize := 4096
	if stats.Size()%4096 != 0 {
		lastBlockSize = int(stats.Size()) - int(4096*numBlock)
		numBlock++
	}
	fmt.Println("numblock:", numBlock)
	fmt.Println("last block size:", lastBlockSize)
	logFile, err := os.Open("test_logs/1")
	if err != nil {
		t.Error(err)
		return
	}

	defer logFile.Close()
	logFileStat, _ := logFile.Stat()
	logFileSize := logFileStat.Size()
	l, r := 0, int(numBlock-1)
	cntRead := 0
	blockBuffer := make([]byte, 4096)
	var logfileBlockBuffer []byte = make([]byte, 4096)

	handleBlock := func(buf []byte) (index uint64, success bool, err error) {
		r := (len(buf) >> 2) - 1

		l := 0
		for l <= r {
			idx := (l + r) >> 1
			offset := idx * 4
			cntRead++

			pos := binary.LittleEndian.Uint32(buf[offset : offset+4])
			//fmt.Println("check pos:", pos)
			//logFileBlock := pos / 4096
			buffer := logfileBlockBuffer
			if logFileSize-int64(pos) < 4096 {
				buffer = buffer[:logFileSize-int64(pos)]
			}

			_, errRead := logFile.ReadAt(buffer, int64(pos))
			if errRead != nil {
				err = errRead
				return
			}

			//buffer = buffer[:n]
			// pos + 22+ sz
			if len(buffer) < 22 {
				success = false
				fmt.Println("len buffer < 22")
				err = errors.New("conflict, index file contain more entry than log file")
				return
			}
			sz := binary.LittleEndian.Uint32(buffer[18:22])
			//fmt.Println("found sz:", sz)
			p := pos + 22 + sz
			if p == uint32(logFileSize) {
				index = binary.LittleEndian.Uint64(buffer[2:10])
				success = true
				return
			}

			if p > uint32(logFileSize) {
				fmt.Println("pos > logFileSize:", p, ">", logFileSize)
				err = errors.New("conflict, index file contain more entry than log file")
				return
			}
			l = idx + 1
		}
		return
	}
	lastIndex := uint64(0)
	start = time.Now()
	for l <= r {
		mid := (l + r) >> 1
		var buf []byte
		if mid == int(numBlock)-1 {
			buf = blockBuffer[:lastBlockSize]
		} else {
			buf = blockBuffer
		}
		_, err = indexFile.ReadAt(buf, int64(mid)*4096)
		if err != nil {
			t.Error(err)
			return
		}
		//fmt.Println("handle index block:", mid)
		r := len(buf) % 4
		buf = buf[:len(buf)-r]
		index, success, err := handleBlock(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if success {
			lastIndex = index
			break
		}
		l = mid + 1
	}

	fmt.Println("search time:", time.Since(start))
	fmt.Println("lastIndex:", lastIndex)
	fmt.Println("cnt read:", cntRead)
	var memstat runtime.MemStats
	runtime.ReadMemStats(&memstat)
	fmt.Println("heap alloc:", memstat.HeapAlloc)
}

func TestProcessLogAndIndexFile(t *testing.T) {
	store, _ := CreateDiskLogStore(&Config{
		LogDir:             "server-2_test_logs",
		MaxLogFileSize:     32 * 0124 * 1024,
		MaxLogEntryInCache: (1 << 17),
	})

	start := time.Now()
	startIdx, endIdx, err := store.processLogAndIndexFile("3_index")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("process time:", time.Since(start))
	fmt.Println("startIdx:", startIdx)
	fmt.Println("endIdx:", endIdx)
}

func TestReadAllFile(t *testing.T) {
	start := time.Now()
	buf, _ := os.ReadFile("server-1_test_logs/1")
	fmt.Println("len:", len(buf))
	fmt.Println("total time:", time.Since(start))
}

func TestSetupDiskStore(t *testing.T) {
	start := time.Now()
	store, err := CreateDiskLogStore(&Config{
		LogDir:             "server-3_test_logs",
		MaxLogFileSize:     32 * 0124 * 1024,
		MaxLogEntryInCache: (1 << 17),
	})
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("load time:", time.Since(start))
	store.PrintMetadata()

}
func TestCreateLogFile(t *testing.T) {
	deleteAllFilesInDir("server-1_test_logs")

	store, err := CreateDiskLogStore(&Config{
		LogDir:             "server-1_test_logs",
		MaxLogFileSize:     32 * 0124 * 1024,
		MaxLogEntryInCache: (1 << 17),
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = store.createNewLogAndIndexFile()
	if err != nil {
		t.Error(err)
	}
}

func TestPersistEntries(t *testing.T) {
	logdir := "server-x_test_logs"
	deleteAllFilesInDir(logdir)

	store, err := CreateDiskLogStore(&Config{
		LogDir:             logdir,
		MaxLogFileSize:     1024 * 1024,
		MaxLogEntryInCache: (1 << 15),
		MinNumLogFile:      3,
	})

	if err != nil {
		t.Error(err)
		return
	}
	err = store.createNewLogAndIndexFile()
	if err != nil {
		t.Error(err)
	}

	store.PrintMetadata()
	numLogs := 1024
	// 1024 * 200 dung?

	var logs []*Log = make([]*Log, numLogs)
	for i := 0; i < numLogs; i++ {
		logs[i] = &Log{
			Type:  LogCommand,
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("kmp"),
		}
	}
	start := time.Now()
	for i := 0; i < 200; i++ {
		err = store.StoreBatchLog(logs)
		if err != nil {
			t.Error(err)
		}
	}
	fmt.Println("total time:", time.Since(start))

	fmt.Println("total written bytes:", appMetrics.totalBytesWriteDisk)
	fmt.Println("total write disk time dur:", appMetrics.totalTimeWrireDisk)
	fmt.Println("total add log cache time:", appMetrics.totalTimeAddLogToCache)
}
