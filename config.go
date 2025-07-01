package simpleraft

import "time"

type Config struct {
	ServerID              ServerID
	ServerAddr            string
	LogBatchSize          int
	ClusterPort           int
	ClusterIPAddr         string
	BatchTimeout          time.Duration
	MaxLogPerTrip         int
	LogDir                string
	MaxLogFileSize        int // default: 64MB
	MaxLogEntryInCache    int // default: 100k
	MinNumLogFile         int // default: 3, at least 3
	ClientPort            int // default: 7000
	ClientIPAddr          string
	ClusterConfigFilename string
	LastVoteFilename      string
}
