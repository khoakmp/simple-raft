package simpleraft

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ServerID string

type Server struct {
	ID   ServerID `json:"server_id" yaml:"server_id"`
	Addr string   `json:"addr" yaml:"addr"`
}

type ClusterConfig struct {
	Servers []Server `json:"servers" yaml:"servers"`
}

func (cfg *ClusterConfig) ToBytes() []byte {
	buffer := bytes.NewBuffer(createBorderLine(49))
	for _, s := range cfg.Servers {
		buffer.WriteString(fmt.Sprintf("|%s|%s|\n",
			padding(string(s.ID), 23, PaddingMid), padding(s.Addr, 23, PaddingMid)))
	}
	return buffer.Bytes()
}

func (cfg *ClusterConfig) Print() {
	fmt.Print(cfg.ToBytes())
}

func (config *ClusterConfig) Clone() ClusterConfig {
	servers := make([]Server, 0, len(config.Servers))
	servers = append(servers, config.Servers...)
	return ClusterConfig{
		Servers: servers,
	}
}

func (config *ClusterConfig) Encode() []byte {
	buf, _ := json.Marshal(config)
	// TODO: change later

	return buf
}

func (config *ClusterConfig) Decode(buf []byte) {
	json.Unmarshal(buf, config)
}

type ClusterConfigState struct {
	LastestIndex  uint64        `yaml:"lastest_index"`
	Lastest       ClusterConfig `yaml:"lastest"`
	CommitedIndex uint64        `yaml:"commited_index"`
	Commited      ClusterConfig `yaml:"commited"`
}

// Print to debug
func (state *ClusterConfigState) Print() {
	//printBorderLine(49)
	buffer := bytes.NewBuffer(createBorderLine(49))
	buffer.WriteString(fmt.Sprintf("|%s|\n", padding("Cluster Configuration", 47, PaddingMid)))
	buffer.Write(createBorderLine(49))
	buffer.WriteString(fmt.Sprintf("|%s|\n",
		padding(fmt.Sprintf("LatestIndex : %d", state.LastestIndex), 47, PaddingMid)))
	buffer.Write(state.Lastest.ToBytes())
	buffer.WriteString(fmt.Sprintf("|%s|\n",
		padding(fmt.Sprintf("CommittedIndex : %d", state.CommitedIndex), 47, PaddingMid)))
	//state.commited.Print()
	buffer.Write(state.Commited.ToBytes())
	fmt.Print(buffer.String())
}

func (state *ClusterConfigState) commitLatestState() {
	state.Commited = state.Lastest.Clone()
	state.CommitedIndex = state.LastestIndex
}

func (state *ClusterConfigState) setLatestConfig(index uint64, config *ClusterConfig) {
	fmt.Println("Commit latest cluster config and set new latest cluster config")
	state.Commited = state.Lastest.Clone()
	state.CommitedIndex = state.LastestIndex
	state.Lastest = config.Clone()
	state.LastestIndex = index
}
