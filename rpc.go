package simpleraft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

type RPC struct {
	Type          uint16
	Request       interface{}
	CorrelationID uint32
	RespChan      chan RPCResponse
	ExitChan      chan struct{}
}

const (
	RpcIdentity        = 0
	RpcAppendEntries   = 1
	RpcRequestVote     = 2
	RpcInstallSnapshot = 3

	RpcRespIdentity               = 4
	RpcRespAppendEntries   uint16 = 5
	RpcRespRequestVote     uint16 = 6
	RpcRespInstallSnapshot uint16 = 7
)

type RPCPayload interface {
	SizeBinary() uint32
	EncodeBinary() []byte
	DecodeBinary(buf []byte)
	WriteBinaryTo(w io.Writer) uint32 // total written bytes
	WriteTo(buf []byte)
}

func IsRequestKey(apiKey int) bool {
	return apiKey < 4
}

type RPCResponse struct {
	Type          uint16
	Response      RPCPayload
	CorrelationID uint32
}

type AppendEntriesRequest struct {
	LeaderID          ServerID `json:"leader_id"`
	PrevLogIndex      uint64   `json:"prev_log_index"`
	PrevLogTerm       uint64   `json:"prev_log_term"`
	Term              uint64   `json:"termm"`
	LeaderCommitIndex uint64   `json:"leader_commited_index"`
	Entries           []*Log   `json:"entries"`
}

func (a *AppendEntriesRequest) WriteTo(buf []byte) {
	// len of buf is pre-calculate to fixed binary size of request
	binary.BigEndian.PutUint32(buf[:4], uint32(len(a.LeaderID)))
	copy(buf[4:len(a.LeaderID)+4], []byte(a.LeaderID))
	start := 4 + len(a.LeaderID)
	binary.BigEndian.PutUint64(buf[start:start+8], a.PrevLogIndex)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.PrevLogTerm)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.Term)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.LeaderCommitIndex)
	start += 8
	for _, e := range a.Entries {
		//binary.BigEndian.PutUint32(buf[start:start+4], e.SizeBinary())
		n := e.WriteSizeDataTo(buf[start:])
		start += int(n)
	}
}

func (a *AppendEntriesRequest) WriteToV2(buf []byte) {
	binary.BigEndian.PutUint32(buf[:4], uint32(len(a.LeaderID)))
	copy(buf[4:len(a.LeaderID)+4], []byte(a.LeaderID))
	start := 4 + len(a.LeaderID)
	binary.BigEndian.PutUint64(buf[start:start+8], a.PrevLogIndex)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.PrevLogTerm)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.Term)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], a.LeaderCommitIndex)
	start += 8
	for _, e := range a.Entries {
		n := e.WriteDataTo(buf[start:])
		start += int(n)
	}
}
func (a *AppendEntriesRequest) EncodeBinary() []byte {
	buffer := bytes.NewBuffer(nil)
	a.WriteBinaryTo(buffer)
	return buffer.Bytes()
}

func (a *AppendEntriesRequest) SizeBinary() uint32 {
	n := 36 + uint32(len(a.LeaderID)) + 4*uint32(len(a.Entries))
	for _, e := range a.Entries {
		n += e.SizeBinary()
	}
	return n
}
func (a *AppendEntriesRequest) SizeBinaryV2() uint32 {
	n := 36 + uint32(len(a.LeaderID))
	for _, e := range a.Entries {
		n += e.SizeBinary()
	}
	return n
}
func (a *AppendEntriesRequest) DecodeBinary(buf []byte) {

	leaderSize := binary.BigEndian.Uint32(buf[:4])
	leaderBuf := make([]byte, leaderSize)
	copy(leaderBuf, buf[4:leaderSize+4])
	start := leaderSize + 4
	a.LeaderID = ServerID(leaderBuf)
	a.PrevLogIndex = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.PrevLogTerm = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.Term = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.LeaderCommitIndex = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.Entries = make([]*Log, 0)

	for start < uint32(len(buf)) {
		sz := binary.BigEndian.Uint32(buf[start : start+4])
		//fmt.Println("log size: ", sz)
		start += 4
		l := new(Log)
		if err := l.DecodeBinary(buf[start : start+sz]); err != nil {
			fmt.Println("start =", start)
			panic(err)
		}
		a.Entries = append(a.Entries, l)
		start += sz
	}
}

func (a *AppendEntriesRequest) DecodeBinaryV2(buf []byte) {

	leaderSize := binary.BigEndian.Uint32(buf[:4])
	leaderBuf := make([]byte, leaderSize)
	copy(leaderBuf, buf[4:leaderSize+4])
	start := leaderSize + 4
	a.LeaderID = ServerID(leaderBuf)
	a.PrevLogIndex = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.PrevLogTerm = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.Term = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.LeaderCommitIndex = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	a.Entries = make([]*Log, 0)

	for start < uint32(len(buf)) {
		l := new(Log)
		sz, err := l.DecodeBinaryV2(buf[start:])
		if err != nil {
			panic(err)
		}
		start += sz
		a.Entries = append(a.Entries, l)
	}
}
func (a *AppendEntriesRequest) WriteBinaryTo(w io.Writer) (n uint32) {
	var szBuf [4]byte
	binary.BigEndian.PutUint32(szBuf[:], uint32(len(a.LeaderID)))
	w.Write(szBuf[:])
	w.Write([]byte(a.LeaderID))

	var buf [32]byte
	binary.BigEndian.PutUint64(buf[:8], a.PrevLogIndex)
	binary.BigEndian.PutUint64(buf[8:16], a.PrevLogTerm)
	binary.BigEndian.PutUint64(buf[16:24], a.Term)
	binary.BigEndian.PutUint64(buf[24:32], a.LeaderCommitIndex)
	//binary.BigEndian.PutUint32(buf[32:36], uint32(len(a.Entries)))
	// not need to write szie dung?
	w.Write(buf[:32])
	n = 36 + uint32(len(a.LeaderID))
	for _, entry := range a.Entries {
		n += entry.WriteSizeAndBinaryTo(w)
	}
	return
}

func (a *AppendEntriesRequest) Print() {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(fmt.Sprintf("LeaderID         :%s\n", a.LeaderID))
	buffer.WriteString(fmt.Sprintf("Term             :%d\n", a.Term))
	buffer.WriteString(fmt.Sprintf("PrevLogIndex     :%d\n", a.PrevLogIndex))
	buffer.WriteString(fmt.Sprintf("PrevLogTerm      :%d\n", a.PrevLogTerm))
	buffer.WriteString(fmt.Sprintf("LeaderCommitIndex:%d\n", a.LeaderCommitIndex))
	buffer.WriteString(fmt.Sprintf("Num Entries      :%d\n", len(a.Entries)))
	for _, l := range a.Entries {
		buffer.WriteString(l.ToSummaryString())
	}
	fmt.Print(buffer.String())
}

func (a *AppendEntriesRequest) Encode() []byte {
	// TODO: change to binary format, not text
	buf, _ := json.Marshal(a)
	return buf
}
func (a *AppendEntriesRequest) Decode(buf []byte) {
	// TODO: change to binary format, not text
	json.Unmarshal(buf, a)
}

type AppendEntriesResponse struct {
	Success      bool   `json:"success"`
	Term         uint64 `json:"term"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
	Stop         bool   `json:"stop"`
}

func (a *AppendEntriesResponse) Print() {
	fmt.Print(string(a.BytesToPrint()))
}

func (a *AppendEntriesResponse) BytesToPrint() []byte {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(fmt.Sprintf("Success     :%v\n", a.Success))
	buffer.WriteString(fmt.Sprintf("Term:       :%d\n", a.Term))
	buffer.WriteString(fmt.Sprintf("LastLogIndex:%d\n", a.LastLogIndex))
	buffer.WriteString(fmt.Sprintf("LastLogTerm :%d\n", a.LastLogTerm))
	buffer.WriteString(fmt.Sprintf("Stop        :%v\n", a.Stop))
	return buffer.Bytes()
}
func (a *AppendEntriesResponse) WriteTo(buf []byte) {
	if a.Success {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.BigEndian.PutUint64(buf[1:9], a.Term)
	binary.BigEndian.PutUint64(buf[9:17], a.LastLogIndex)
	binary.BigEndian.PutUint64(buf[17:25], a.LastLogTerm)
	if a.Stop {
		buf[25] = 1
	} else {
		buf[25] = 0
	}
}
func (a *AppendEntriesResponse) SizeBinary() uint32 {
	return 26
}
func (a *AppendEntriesResponse) WriteBinaryTo(w io.Writer) uint32 {
	var buf [26]byte
	buf[0] = 0
	if a.Success {
		buf[0] = 1
	}
	binary.BigEndian.PutUint64(buf[1:9], a.Term)
	binary.BigEndian.PutUint64(buf[9:17], a.LastLogIndex)
	binary.BigEndian.PutUint64(buf[17:25], a.LastLogTerm)
	buf[25] = 0
	if a.Stop {
		buf[25] = 1
	}
	w.Write(buf[:])
	return 0
}
func (a *AppendEntriesResponse) EncodeBinary() []byte {
	buffer := bytes.NewBuffer(nil)
	a.WriteBinaryTo(buffer)
	return buffer.Bytes()
}

func (a *AppendEntriesResponse) DecodeBinary(buf []byte) {
	a.Success = false
	if buf[0] == 1 {
		a.Success = true
	}
	a.Term = binary.BigEndian.Uint64(buf[1:9])
	a.LastLogIndex = binary.BigEndian.Uint64(buf[9:17])
	a.LastLogTerm = binary.BigEndian.Uint64(buf[17:25])
	a.Stop = false
	if buf[25] == 1 {
		a.Stop = true
	}
}
func (a *AppendEntriesResponse) Encode() []byte {
	buf, _ := json.Marshal(a)
	return buf
}

func (a *AppendEntriesResponse) Decode(buf []byte) {
	json.Unmarshal(buf, a)
}

func calResponseApiKey(rpcType uint16) uint16 {
	return rpcType + 4
}

func (rpc *RPC) Respond(resp RPCPayload) {
	select {
	case <-rpc.ExitChan:
		fmt.Println("Detect exitChan close, so not send resp to resp chan")
	case rpc.RespChan <- RPCResponse{
		Type:          calResponseApiKey(rpc.Type),
		Response:      resp,
		CorrelationID: rpc.CorrelationID,
	}:
	}
}

type RequestVotePayload struct {
	CandidateID  ServerID
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (p *RequestVotePayload) SizeBinary() uint32 {
	return 28 + uint32(len(p.CandidateID))
}

func (p *RequestVotePayload) EncodeBinary() []byte {
	buf := make([]byte, 28+len(p.CandidateID))
	p.WriteTo(buf)
	return buf
}
func (p *RequestVotePayload) DecodeBinary(buf []byte) {
	sz := binary.BigEndian.Uint32(buf[:4])
	idBuf := make([]byte, sz)
	copy(idBuf, buf[4:4+sz])
	start := 4 + sz
	p.CandidateID = ServerID(idBuf)
	p.Term = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	p.LastLogIndex = binary.BigEndian.Uint64(buf[start : start+8])
	start += 8
	p.LastLogTerm = binary.BigEndian.Uint64(buf[start : start+8])
}
func (p *RequestVotePayload) WriteTo(buf []byte) {
	binary.BigEndian.PutUint32(buf[:4], uint32(len(p.CandidateID)))
	start := 4
	copy(buf[start:start+len(p.CandidateID)], p.CandidateID)
	start += len(p.CandidateID)

	binary.BigEndian.PutUint64(buf[start:start+8], p.Term)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], p.LastLogIndex)
	start += 8
	binary.BigEndian.PutUint64(buf[start:start+8], p.LastLogTerm)
}

func (p *RequestVotePayload) WriteBinaryTo(w io.Writer) uint32 {
	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(p.CandidateID)))
	w.Write(sz[:])
	w.Write([]byte(p.CandidateID))
	var tailBuf [24]byte
	binary.BigEndian.PutUint64(tailBuf[:8], p.Term)
	binary.BigEndian.PutUint64(tailBuf[8:16], p.LastLogIndex)
	binary.BigEndian.PutUint64(tailBuf[16:24], p.LastLogTerm)
	w.Write(tailBuf[:])
	return 26 + uint32(len(p.CandidateID))
}

type RequestVoteResp struct {
	Term        uint64
	VoteGranted bool
}

func (r *RequestVoteResp) SizeBinary() uint32 {
	return 9
}

func (r *RequestVoteResp) EncodeBinary() []byte {
	buf := make([]byte, 9)
	r.WriteTo(buf)
	return buf
}
func (r *RequestVoteResp) WriteTo(buf []byte) {
	binary.BigEndian.PutUint64(buf[:8], r.Term)
	if r.VoteGranted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
}

func (r *RequestVoteResp) DecodeBinary(buf []byte) {
	r.Term = binary.BigEndian.Uint64(buf[:8])
	if buf[8] == 1 {
		r.VoteGranted = true
	} else {
		r.VoteGranted = false
	}
}

func (r *RequestVoteResp) WriteBinaryTo(w io.Writer) uint32 {
	var termBuf [9]byte
	binary.BigEndian.PutUint64(termBuf[:8], r.Term)
	if r.VoteGranted {
		termBuf[8] = 1
	} else {
		termBuf[8] = 0
	}
	w.Write(termBuf[:])
	return 9
}

/*
EncodeBinary() []byte
	DecodeBinary(buf []byte)
	WriteBinaryTo(w io.Writer) uint32 // total written bytes
	WriteTo(buf []byte)
*/
