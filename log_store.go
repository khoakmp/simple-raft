package simpleraft

type LogStore interface {
	StoreLog(log *Log) error
	GetRangeLogs(startIdx, endIdx uint64) ([]*Log, error)
	StoreBatchLog(logs []*Log) error
	GetLog(index uint64) (*Log, error)
	GetLastEntry() (lastLogIndex, lastLogTerm uint64)
}
