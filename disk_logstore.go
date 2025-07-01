package simpleraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

/*
StoreLog(log *Log) error
GetRangeLogs(startIdx, endIdx uint64) ([]*Log, error)
StoreBatchLog(logs []*Log) error
GetLog(index uint64) (*Log, error)
*/

type DiskLogStore struct {
	config               *Config
	metadata             LogStoreMetadata
	currentLogFile       *os.File // opening in append-only mode
	currentIndexFile     *os.File
	currentLogFileSize   uint32
	currentIndexFileSize uint32
	cacheLogs            *LogCache
	pauseFlag            uint32
	logger               zerolog.Logger
	lock                 sync.RWMutex
	//raft                 *Raft
}

type LogFileMetadata struct {
	FileName      string `json:"f_name"`
	FileNum       uint64 `json:"f_num"`
	StartLogIndex uint64 `json:"start_idx"`
	StartLogTerm  uint64 `json:"start_term"`
	EndLogIndex   uint64 `json:"end_idx"`
	EndLogTerm    uint64 `json:"end_term"`
}

func (m *LogFileMetadata) ToString() string {
	buffer := bytes.NewBuffer(nil)

	buffer.WriteString("---------------------------\n")
	buffer.WriteString(fmt.Sprintf("FileName       :%s\n", m.FileName))
	buffer.WriteString(fmt.Sprintf("FileNum        :%d\n", m.FileNum))
	/* buffer.WriteString(fmt.Sprintf("LastModifiedAt :%s\n", m.LastModifiedAt))
	buffer.WriteString(fmt.Sprintf("CreatedAt      :%s\n", m.CreatedAt)) */
	buffer.WriteString(fmt.Sprintf("StartIndex     :%d\n", m.StartLogIndex))
	buffer.WriteString(fmt.Sprintf("StartTerm      :%d\n", m.StartLogTerm))

	buffer.WriteString(fmt.Sprintf("EndIndex       :%d\n", m.EndLogIndex))
	buffer.WriteString(fmt.Sprintf("EndTerm        :%d\n", m.EndLogTerm))

	return buffer.String()
}

type LogStoreMetadata struct {
	CurrentFileNum uint64            `json:"counter"`
	Files          []LogFileMetadata `json:"files"`
}

func (store *DiskLogStore) PrintMetadata() {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(fmt.Sprintf("CurrentFileNum:%d\n", store.metadata.CurrentFileNum))
	for _, f := range store.metadata.Files {
		buffer.WriteString(f.ToString())
	}

	fmt.Print(buffer.String())

}
func (store *DiskLogStore) getFileNameFromNum(fileNum int) string {
	return fmt.Sprintf("%d", fileNum)
}

/*
	 func (store *DiskLogStore) initFileLogStore() error {

		logFileMeta := LogFileMetadata{
			FileName: store.getFileNameFromNum(1),
			FileNum:  1,

			StartIndex: 0,
			EndIndex:   0,
		}

		store.metadata = LogStoreMetadata{
			CurrentFileNum: 1,
			Files: []LogFileMetadata{
				logFileMeta,
			},
		}

		return nil
	}
*/
func getIndexFileName(logFileName string) string {
	return fmt.Sprintf("%s_index", logFileName)
}

func (store *DiskLogStore) openCurrentFiles() error {
	if store.metadata.CurrentFileNum == 0 {
		return nil
	}

	n := len(store.metadata.Files)
	fname := store.metadata.Files[n-1].FileName

	logFile, err := os.OpenFile(store.getAbsolutePath(fname), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	store.currentLogFile = logFile
	store.currentIndexFile, err = os.OpenFile(store.getAbsolutePath(getIndexFileName(fname)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	logStat, _ := logFile.Stat()
	indexStat, _ := store.currentIndexFile.Stat()

	store.currentLogFileSize = uint32(logStat.Size())
	store.currentIndexFileSize = uint32(indexStat.Size())

	if err != nil {
		return err
	}

	return nil
}

func (store *DiskLogStore) initLogCache() {
	// TODO: load in the current log file into cache
	store.cacheLogs = NewLogCache(store.config)
	l := len(store.metadata.Files)
	if l == 0 {
		return
	}
	// TODO: load entries to cache dung?

}

func CreateDiskLogStore(config *Config) (*DiskLogStore, error) {
	store := &DiskLogStore{
		config: config,
	}
	if config.MinNumLogFile < 3 {
		config.MinNumLogFile = 3
	}

	store.setupMetadata()
	store.openCurrentFiles()
	store.initLogCache()

	return store, nil
}

type LogMetadata struct {
	Index uint64
	Term  uint64
}

func (store *DiskLogStore) processLogAndIndexFile(indexFilename string) (startEntry, endEntry LogMetadata, err error) {
	logFileName := indexFilename[:len(indexFilename)-6]
	indexFile, err := os.OpenFile(store.getAbsolutePath(indexFilename), os.O_RDWR, 0644)
	if err != nil {
		store.logger.Err(err).Msg("failed to open index file")
		return
	}
	defer indexFile.Close()
	logFile, err := os.Open(store.getAbsolutePath(logFileName))
	if err != nil {
		store.logger.Err(err).Msg("failed to open log file")
		return
	}
	defer logFile.Close()

	var blockBuffer []byte = make([]byte, 4096)
	indexStat, _ := indexFile.Stat()
	indexFileSize := indexStat.Size() - (indexStat.Size() % 4)
	logStat, _ := logFile.Stat()
	logFileSize := logStat.Size()

	lastBlockSize := indexFileSize % 4096
	lastIndexBlock := indexFileSize / 4096
	if lastBlockSize == 0 {
		lastIndexBlock--
		lastBlockSize = 4096
	}

	indexBlock := lastIndexBlock

	var logFileBlockBuffer [22]byte

	var headLogBuf [18]byte
	logFile.ReadAt(headLogBuf[:], 0)

	startEntry.Index = binary.LittleEndian.Uint64(headLogBuf[2:10])
	startEntry.Term = binary.LittleEndian.Uint64(headLogBuf[10:18])
	fmt.Println("num block:", indexBlock)
	totalBlockRead := 0
	// handleBlock check the index buf , to determine the last log entry of logfile is indexed in buf
	// position: the position in index file

	handleBlock := func(buf []byte) (logMeta LogMetadata, position uint32, success bool, err error) {
		l := 0
		r := (len(buf) >> 2) - 1
		for l <= r {
			mid := (l + r) >> 1
			offset := mid << 2

			pos := binary.LittleEndian.Uint32(buf[offset : offset+4])
			buffer := logFileBlockBuffer[:]
			if logFileSize <= int64(pos) {
				r = mid - 1
				continue
			}

			if (logFileSize)-int64(pos) < 22 {
				r = mid - 1
				continue
			}
			/* if len(buffer) < 22 {
				r = mid - 1
				continue
			} */

			_, err = logFile.ReadAt(buffer, int64(pos))
			if err != nil {
				return
			}
			totalBlockRead++
			sz := binary.LittleEndian.Uint32(buffer[18:22])
			p := pos + 22 + sz

			if p == uint32(logFileSize) {
				success = true
				position = uint32(offset)
				logMeta.Index = binary.LittleEndian.Uint64(buffer[2:10])
				logMeta.Term = binary.LittleEndian.Uint64(buffer[10:18])

				return
			} else if p > uint32(logFileSize) {
				err = errors.New("unrecoverable conflict")
				return
			} else {
				// p < logFileSize dung?
				l = mid + 1
			}
		}
		return
	}
	totalBlockIndexRead := 0

	for indexBlock >= 0 {
		buffer := blockBuffer
		if indexBlock == lastIndexBlock {
			buffer = buffer[:lastBlockSize]
		}

		indexFile.ReadAt(blockBuffer, indexBlock*4096)
		totalBlockIndexRead++
		logMeta, position, success, errHandle := handleBlock(buffer)
		if errHandle != nil {
			err = errHandle
			return
		}

		if success {
			endEntry = logMeta

			if position+4+4096*uint32(indexBlock) < uint32(indexFileSize) {
				fmt.Println("the number of index records is larger than the number of actual log records")
				indexFile.Truncate(indexFileSize - int64(position+4))
			} else {
				fmt.Println("index and log ok")
			}
			break
		}
		indexBlock--
	}
	fmt.Println("Total blockIndex read:", totalBlockIndexRead)
	fmt.Println("total block log read:", totalBlockRead)
	return
}

func (store *DiskLogStore) setupMetadata() error {
	dirEntries, err := os.ReadDir(store.config.LogDir)
	if err != nil {
		store.logger.Err(err).Msg("failed to read log dir")
		return err
	}

	var fileMetas map[int]LogFileMetadata = make(map[int]LogFileMetadata)
	var filenums []int = make([]int, 0)
	for _, item := range dirEntries {
		if item.IsDir() {
			continue
		}

		name := item.Name()

		if len(name) > 6 && name[len(name)-6:] == "_index" {
			logFileName := name[:len(name)-6]
			filenum, _ := strconv.Atoi(logFileName)

			startEntry, endEntry, err := store.processLogAndIndexFile(name)
			if err != nil {
				fmt.Println("Failed to process ", name)
				return err
			}

			fileMetas[filenum] = LogFileMetadata{
				FileName:      logFileName,
				FileNum:       uint64(filenum),
				StartLogIndex: startEntry.Index,
				StartLogTerm:  startEntry.Term,
				EndLogIndex:   endEntry.Index,
				EndLogTerm:    endEntry.Term,
			}
			filenums = append(filenums, filenum)
		}
	}

	fmt.Println("init files,num file:", len(filenums))

	if len(filenums) == 0 {
		store.metadata.CurrentFileNum = 0
		store.metadata.Files = make([]LogFileMetadata, 0)
		return nil
	}

	sort.Ints(filenums)

	store.metadata.Files = make([]LogFileMetadata, 0, len(filenums))
	store.metadata.CurrentFileNum = uint64(filenums[len(filenums)-1])

	for _, num := range filenums {
		store.metadata.Files = append(store.metadata.Files, fileMetas[num])
	}

	return nil
}

// MaxEntryIn
func (store *DiskLogStore) GetLog(index uint64) (*Log, error) {
	logs, err := store.GetRangeLogs(index, index)
	if err != nil {
		return nil, err
	}
	return logs[0], nil
}

func (store *DiskLogStore) StoreLog(log *Log) error {
	return store.StoreBatchLog([]*Log{log})
}

func (store *DiskLogStore) GetRangeLogs(startIdx, endIdx uint64) ([]*Log, error) {
	logs := store.cacheLogs.GetRange(startIdx, endIdx)

	if len(logs) > 0 && logs[0].Index == startIdx && logs[len(logs)-1].Index == endIdx {

		return logs, nil
	}

	//fmt.Println("Get from cache: len:", len(logs))
	start, end := uint64(0), uint64(0)
	if len(logs) == 0 {
		start = startIdx
		end = endIdx
	} else if logs[0].Index > startIdx {
		start = startIdx
		end = logs[0].Index - 1
	}
	//fmt.Printf("Get range [%d:%d] from disk\n", start, end)
	entries, err := store.getRangeFromFile(start, end)
	if err != nil {

		return nil, err
	}

	if len(logs) > 0 {
		result := append(entries, logs...)
		return result, nil
	}
	return entries, nil
}

func (store *DiskLogStore) getLogFileOfEntry(index uint64) (fileNum int, err error) {
	store.lock.RLock()
	defer store.lock.RUnlock()
	for _, fileMeta := range store.metadata.Files {
		if fileMeta.StartLogIndex <= index && fileMeta.EndLogIndex >= index {
			return int(fileMeta.FileNum), nil
		}
	}
	return 0, ErrLogNotFound
}

func (store *DiskLogStore) getEntrySizeAtOffset(file *os.File, offset uint32) uint32 {
	// type 2bytes + Index + Term 16bytes + 4bytes data size + data
	var buf [4]byte

	file.ReadAt(buf[:], int64(offset)+18)
	return 22 + binary.LittleEndian.Uint32(buf[:])
}

func (store *DiskLogStore) loadEntriesFromFile(fileNum int, startIndex, endIndex uint64) ([]*Log, error) {
	//fmt.Printf("Loading entries[%d:%d] in file %d\n", startIndex, endIndex, fileNum)

	fileName := store.getFileNameFromNum(fileNum)
	startPosIdx, endPosIdx := -1, 0
	store.lock.RLock()
	for _, fileMeta := range store.metadata.Files {
		if fileMeta.FileNum != uint64(fileNum) {
			continue
		}
		if startIndex == 0 {
			startIndex = fileMeta.StartLogIndex
		}
		if endIndex == 0 {
			endIndex = fileMeta.EndLogIndex
		}

		startPosIdx = int(uint64(startIndex)-fileMeta.StartLogIndex) * 4
		endPosIdx = int(uint64(endIndex)-fileMeta.StartLogIndex) * 4
	}
	store.lock.RUnlock()

	if startPosIdx < 0 {
		return nil, ErrLogNotFound
	}
	//fmt.Printf("startPosIdx:%d, endPosIdx:%d\n", startPosIdx, endPosIdx)

	indexFileName := getIndexFileName(fileName)
	//os.ReadFile(indexFileName)
	indexFile, err := os.Open(store.getAbsolutePath(indexFileName)) // open index file in read-only mode dung?
	if err != nil {
		//fmt.Println("Failed to open index file ", fileNum)
		return nil, err
	}
	defer indexFile.Close()

	var posBuf [4]byte

	_, err = indexFile.ReadAt(posBuf[:], int64(startPosIdx))
	if err != nil {
		//fmt.Println("Failed to read index file at offset:", startPosIdx)
		return nil, err
	}
	var startOffset uint32 = binary.LittleEndian.Uint32(posBuf[:])

	_, err = indexFile.ReadAt(posBuf[:], int64(endPosIdx))
	if err != nil {
		//fmt.Println("Failed to read index file at offset:", endPosIdx)
		return nil, err
	}
	var endOffset uint32 = binary.LittleEndian.Uint32(posBuf[:])

	logFile, err := os.Open(store.getAbsolutePath(fileName))
	if err != nil {
		//fmt.Println("Failed to open log file, file:", fileNum)
		return nil, err
	}
	defer logFile.Close()
	//fmt.Printf("startOffset:%d, endOffset: %d\n", startOffset, endOffset)

	sz := store.getEntrySizeAtOffset(logFile, endOffset)

	buf := bspool.Get(sz + endOffset - startOffset)
	_, err = logFile.ReadAt(buf, int64(startOffset))
	if err != nil {
		//fmt.Println("Failed to read log file at offset ", startOffset, "len:", len(buf))
		return nil, err
	}

	logs, err := store.decodeBufferToEntries(buf, int(endIndex)-int(startIndex)+1)
	bspool.Put(buf)
	appMetrics.IncrTotalLogGetFromDisk(endIndex - startIndex + 1)
	return logs, err
}

func (store *DiskLogStore) decodeBufferToEntries(buf []byte, numEntries int) ([]*Log, error) {
	var offset uint32 = 0
	//var szBuf [4]byte

	var logs []*Log = make([]*Log, 0, numEntries)
	appMetrics.IncrGetLogDataBufferCnt(uint32(numEntries))

	for offset < uint32(len(buf)) {
		log := new(Log)
		log.Type = LogType(binary.LittleEndian.Uint16(buf[offset : offset+2]))
		log.Index = binary.LittleEndian.Uint64(buf[offset+2 : offset+10])
		log.Term = binary.LittleEndian.Uint64(buf[offset+10 : offset+18])
		sz := binary.LittleEndian.Uint32(buf[offset+18 : offset+22])

		data := bspool.Get(sz)

		copy(data, buf[offset+22:uint32(offset)+22+sz])
		log.Data = data
		//fmt.Printf("Get log[%d].Data : %s\n", log.Index, string(log.Data))
		offset += 22 + sz
		logs = append(logs, log)
	}

	return logs, nil
}
func (store *DiskLogStore) getRangeFromFile(startIdx, endIdx uint64) ([]*Log, error) {
	atomic.AddUint32(&appMetrics.getLogFromDiskCnt, 1)

	startFileNum, err := store.getLogFileOfEntry(startIdx)
	if err != nil {
		return nil, err
	}
	endFileNum, err := store.getLogFileOfEntry(endIdx)
	if err != nil {
		return nil, err
	}

	if startFileNum == endFileNum {
		//fmt.Println("startIdx and endIdx is same file:", startFileNum)
		return store.loadEntriesFromFile(startFileNum, startIdx, endIdx)
	}

	logs, err := store.loadEntriesFromFile(startFileNum, startIdx, 0)
	if err != nil {
		return nil, err
	}

	for filenum := startFileNum + 1; filenum < endFileNum; filenum++ {
		entries, err := store.loadEntriesFromFile(filenum, 0, 0)
		if err != nil {
			return nil, err
		}
		logs = append(logs, entries...)
	}
	entries, err := store.loadEntriesFromFile(endFileNum, 0, endIdx)
	if err != nil {
		return nil, err
	}

	logs = append(logs, entries...)
	return logs, nil
}

func (store *DiskLogStore) encodeEntries(logs []*Log) []byte {
	var sz uint32
	for _, entry := range logs {
		sz += 22 + uint32(len(entry.Data))
	}
	//fmt.Println("buffer size to encode all entries:", sz)

	buf := bspool.Get(sz)
	var offset uint32 = 0
	for _, entry := range logs {
		//fmt.Printf("offset of log[%d]:%d\n", entry.Index, offset)
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(entry.Type))
		binary.LittleEndian.PutUint64(buf[offset+2:offset+10], entry.Index)
		binary.LittleEndian.PutUint64(buf[offset+10:offset+18], entry.Term)
		sz := uint32(len(entry.Data))

		binary.LittleEndian.PutUint32(buf[offset+18:offset+22], sz)
		//fmt.Println("Copy entry.Data:", string(entry.Data), "to buffer")
		copy(buf[offset+22:offset+22+sz], entry.Data)

		offset += 22 + sz
	}

	return buf
}

func (store *DiskLogStore) persistEntries(logs []*Log) error {
	start := time.Now()
	logsBuf := store.encodeEntries(logs)
	//fmt.Println("prepare to persist logs")
	//fileMeta := &store.metadata.Files[len(store.metadata.Files)-1]
	offset := store.currentLogFileSize

	//indexesBuf := make([]byte, 4*len(logs))
	indexesBuf := bspool.Get(uint32(4 * len(logs)))
	off := 0

	for i := 0; i < len(logs); i++ {
		//fmt.Printf("Write index of log[%d] at offset:%d\n", logs[i].Index, offset)
		binary.LittleEndian.PutUint32(indexesBuf[off:off+4], uint32(offset))
		offset += logs[i].SizeBinary()
		off += 4
	}

	n, err := store.currentIndexFile.Write(indexesBuf)
	bspool.Put(indexesBuf)

	//fmt.Println("Write to index file", len(indexesBuf), " bytes")
	appMetrics.IncrBytesWriteDisk(uint64(len(indexesBuf)))

	if err != nil {
		store.logger.Error().Msg(fmt.Sprintf("Failed to write to indexfile, caused by %s", err))
		if n > 0 {
			truncateErr := store.currentIndexFile.Truncate(int64(store.currentIndexFileSize))
			store.logger.Error().Msg(fmt.Sprintf("Failed to truncate index file, caused by:%s", truncateErr))
		}
		//store.unsetCurrentFiles()
		return err
	}

	store.currentIndexFileSize += 4 * uint32(len(logs))

	_, err = store.currentLogFile.Write(logsBuf)
	bspool.Put(logsBuf)

	//fmt.Println("Write to log file", len(logsBuf), " bytes")
	appMetrics.IncrBytesWriteDisk(uint64(len(logsBuf)))

	if err != nil {
		// TODO: rollback writting to index file
		// logging to notify disk failed
		store.logger.Err(err).Msg("Failed to write log file")
		if n > 0 {
			if truncateErr := store.currentLogFile.Truncate(int64(store.currentLogFileSize)); truncateErr != nil {
				store.logger.Err(truncateErr).Msg("Failed to truncate log file")
			}
		}
		store.currentIndexFile.Truncate(int64(store.currentIndexFileSize))
		//store.unsetCurrentFiles()
		return err
	}

	//appMetrics.storeTimes = append(appMetrics.storeTimes, time.Since(start))
	//fmt.Println("Write Index and Log file time:", time.Since(start))
	appMetrics.IncrWriteDiskTime(time.Since(start))

	store.currentIndexFileSize += 4 * uint32(len(logs))
	store.currentLogFileSize += uint32(len(logsBuf))

	return nil
}
func (store *DiskLogStore) unsetCurrentFiles() {
	if store.currentIndexFile != nil {
		store.currentIndexFile.Close()
	}
	if store.currentLogFile != nil {
		store.currentLogFile.Close()
	}
	store.pauseFlag = 1 // just prevent to store new log, not block read log
	store.currentIndexFile = nil
	store.currentLogFile = nil
	store.currentLogFileSize = 0
	store.currentIndexFileSize = 0
}
func (store *DiskLogStore) calBatchCutingPoint(logs []*Log, limitSize uint32) int {
	var sz uint32 = 0
	for idx, l := range logs {
		sz += l.SizeBinary()
		if sz > limitSize {
			return idx - 1
		}
	}
	return len(logs) - 1
}
func (store *DiskLogStore) getAbsolutePath(filename string) string {
	return fmt.Sprintf("%s/%s", store.config.LogDir, filename)
}

func (store *DiskLogStore) checkRemoveLogFile() {
	if len(store.metadata.Files) < store.config.MinNumLogFile {
		return
	}
	// TODO: should check lastAppliedIndex instead of commitIndex
	//commitIndex := store.raft.getCommitIndex()
	firstFileMeta := &store.metadata.Files[0]
	/* if firstFileMeta.EndLogIndex > commitIndex {
		return
	}
	*/
	logFilename := firstFileMeta.FileName
	fmt.Println("Removing ", logFilename)
	start := time.Now()
	err := os.Remove(store.getAbsolutePath(logFilename))

	if err != nil {
		store.logger.Err(err).Msg("failed to remove log file")
	}

	err = os.Remove(store.getAbsolutePath(getIndexFileName(logFilename)))

	if err != nil {
		store.logger.Err(err).Msg("failed to remove log file")
	}
	fmt.Println("Remove time:", time.Since(start))
	var files []LogFileMetadata = make([]LogFileMetadata, 0, len(store.metadata.Files)-1)
	files = append(files, store.metadata.Files[1:]...)
	store.metadata.Files = files
}
func (store *DiskLogStore) createNewLogAndIndexFile() error {
	store.lock.Lock()
	defer store.lock.Unlock()

	store.checkRemoveLogFile()

	store.metadata.CurrentFileNum++

	logFileName := store.getFileNameFromNum(int(store.metadata.CurrentFileNum))

	logfile, err := os.OpenFile(store.getAbsolutePath(logFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		store.logger.Err(err).Msg("Failed to create new log file")
		return err
	}

	logIndexFile, err := os.OpenFile(store.getAbsolutePath(getIndexFileName(logFileName)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		os.Remove(logFileName)
		store.logger.Err(err).Msg("Failed to create new index file")
		return err
	}

	store.currentLogFile = logfile
	store.currentLogFileSize = 0

	store.currentIndexFile = logIndexFile
	store.currentIndexFileSize = 0
	store.pauseFlag = 0

	store.metadata.Files = append(store.metadata.Files, LogFileMetadata{
		FileName: logFileName,
		FileNum:  store.metadata.CurrentFileNum,

		StartLogIndex: 0,
		EndLogIndex:   0,
	})

	return nil
}

func (store *DiskLogStore) updateCurrentIndexMetadata(startEntry, endEntry LogMetadata) {
	store.lock.Lock()
	defer store.lock.Unlock()
	l := len(store.metadata.Files)
	metadata := &store.metadata.Files[l-1]
	metadata.EndLogIndex = endEntry.Index
	metadata.EndLogTerm = endEntry.Term

	if metadata.StartLogIndex == 0 {
		metadata.StartLogIndex = startEntry.Index
		metadata.StartLogTerm = startEntry.Term
	}

}

var ErrLogStorePausing error = errors.New("log store is pausing")

// only one goroutine call this function
func (store *DiskLogStore) StoreBatchLog(logs []*Log) error {
	if store.pauseFlag == 1 {
		return ErrLogStorePausing
	}

	if store.metadata.CurrentFileNum == 0 {
		store.createNewLogAndIndexFile()
	}
	avail := store.config.MaxLogFileSize - int(store.currentLogFileSize)
	idx := store.calBatchCutingPoint(logs, uint32(avail))

	if idx >= 0 {
		// each time persistEntries to current file failed, need change to state kia dung
		if err := store.persistEntries(logs[:idx+1]); err != nil {
			store.unsetCurrentFiles()
			return err
		}

		store.updateCurrentIndexMetadata(LogMetadata{
			Index: logs[0].Index,
			Term:  logs[0].Term,
		}, LogMetadata{
			Index: logs[idx].Index,
			Term:  logs[idx].Term,
		})
	}

	if idx < len(logs)-1 {
		remainLogs := logs[idx+1:]
		store.unsetCurrentFiles()
		if err := store.createNewLogAndIndexFile(); err != nil {
			store.unsetCurrentFiles()
			return err
		}
		if err := store.persistEntries(remainLogs); err != nil {
			store.unsetCurrentFiles()
			return err
		}

		store.updateCurrentIndexMetadata(LogMetadata{
			Index: remainLogs[0].Index,
			Term:  remainLogs[0].Term,
		}, LogMetadata{
			Index: remainLogs[len(remainLogs)-1].Index,
			Term:  remainLogs[len(remainLogs)-1].Term,
		})
	}

	// addBatch may not raise error
	/* start := time.Now()
	if err := store.cacheLogs.AddBatch(logs); err != nil {
		return err
	}
	appMetrics.IncrAddCacheLogTime(time.Since(start)) */
	return nil
}
func (store *DiskLogStore) GetLastEntry() (lastLogIndex, lastLogTerm uint64) {
	l := len(store.metadata.Files)
	if l == 0 {
		return 0, 0
	}
	return store.metadata.Files[l-1].EndLogIndex, store.metadata.Files[l-1].EndLogTerm
}
