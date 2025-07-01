package simpleraft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	net.Conn
	lock            sync.RWMutex
	reader          *bufio.Reader
	writer          *bufio.Writer
	writeLock       sync.Mutex
	applyLogFutChan chan *ApplyLogReqFuture // default size: 500

}

func newClient(conn net.Conn) *Client {
	return &Client{
		Conn:            conn,
		reader:          bufio.NewReader(conn),
		writer:          bufio.NewWriterSize(conn, 4096),
		lock:            sync.RWMutex{},
		applyLogFutChan: make(chan *ApplyLogReqFuture, 1024),
	}
}

type server struct{}

const (
	CommandType uint16 = 1
	QueryType   uint16 = 2
)
const (
	CommandRespType uint16 = 3
	QueryRespType   uint16 = 4
)

func (s server) serve(client *Client, raft *Raft) {
	quitChan := make(chan struct{})
	go s.writeLoop(client, quitChan)

	for {
		reqType, cid, payloadSize, err := s.readHeader(client)
		if err != nil {
			fmt.Println("Failed to read header, err:", err)
			break
		}
		appMetrics.IncrGetLogDataBufferCnt(1)
		payloadBuf := bspool.Get(payloadSize)
		_, err = io.ReadFull(client.reader, payloadBuf)
		if err != nil {
			fmt.Println("Failed to ready request payload, err:", err)
			break
		}

		if reqType == CommandType {
			//fmt.Println("apply command  cid:", cid)
			raft.ApplyCommand(Command{
				applyLogFutChan: client.applyLogFutChan,
				Data:            payloadBuf,
				CorrelationID:   cid,
			})
		}
	}
	close(quitChan)
}

func (s server) readHeader(client *Client) (reqType uint16, correlationID, payloadSize uint32, err error) {
	var headerBuf [10]byte
	_, err = io.ReadFull(client.reader, headerBuf[:])
	if err != nil {
		return
	}
	reqType = binary.BigEndian.Uint16(headerBuf[0:2])
	correlationID = binary.BigEndian.Uint32(headerBuf[2:6])
	payloadSize = binary.BigEndian.Uint32(headerBuf[6:10])
	return
}

func (s server) writeLoop(client *Client, quitChan chan struct{}) {
	flushTicker := time.NewTicker(time.Millisecond * 100)
	//var lastCID uint32 = 0
	//var lastFlushCID uint32 = 0

LOOP:
	for {
		select {
		case fut := <-client.applyLogFutChan:
			//bspool.Put(fut.log.Data)
			appMetrics.IncrPutLogDataBufferCnt(1)
			//lastCID = fut.correlationID
			s.writeApplyLogResp(client, fut.correlationID, fut.err)

		case <-flushTicker.C:
			atomic.AddUint32(&appMetrics.flushToClientCount, 1)
			//fmt.Println("Flush num resp:", lastCID-lastFlushCID)
			//lastFlushCID = lastCID

			client.writeLock.Lock()
			err := client.writer.Flush()
			if err != nil {
				client.writeLock.Unlock()
				break LOOP
			}
			client.writeLock.Unlock()
		case <-quitChan:
			break LOOP
		}
	}
	flushTicker.Stop()
}

type CommandResponse struct {
	Success bool  `json:"success"`
	Err     error `json:"error"`
}

func (s server) createCommandResp(err error) CommandResponse {
	resp := CommandResponse{}
	if err != nil {
		resp.Success = false
		resp.Err = err
	} else {
		resp.Success = true
	}
	return resp
}

func (s server) writeApplyLogResp(client *Client, correlationID uint32, err error) {
	var headerBuf [10]byte
	resp := s.createCommandResp(err)
	buf, _ := json.Marshal(resp)

	binary.BigEndian.PutUint16(headerBuf[:2], CommandRespType)
	binary.BigEndian.PutUint32(headerBuf[2:6], correlationID)
	binary.BigEndian.PutUint32(headerBuf[6:10], uint32(len(buf)))
	client.writeLock.Lock()
	client.writer.Write(headerBuf[:])
	client.writer.Write(buf)
	client.writeLock.Unlock()

}
