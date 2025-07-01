package simpleraft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type NetTransport struct {
	listener      net.Listener
	clusterPort   int
	clusterIPAddr string
	exitFlag      int32
	rpcChan       chan *RPC
	config        *Config
	poolConn      map[string]*PeerConn
	poolLock      sync.RWMutex
}

func (trans *NetTransport) RpcChan() <-chan *RPC {
	return trans.rpcChan
}

func NewNetTransport(config *Config) *NetTransport {
	return &NetTransport{
		clusterPort:   config.ClusterPort,
		clusterIPAddr: config.ClusterIPAddr,
		exitFlag:      0,
		rpcChan:       make(chan *RPC),
		config:        config,
		poolConn:      make(map[string]*PeerConn),
		poolLock:      sync.RWMutex{},
	}
}

type peerServer interface {
	serve(c *PeerConn, trans *NetTransport)
}

type serverV1 struct{}

func (c *PeerConn) exit() {
	close(c.exitChan)
	// no ko acquire duoc cai lock dung

	c.respsLock.Lock()
	fmt.Println("number of waiting resps:", len(c.waitingResps))
	for _, respChan := range c.waitingResps {
		close(respChan)
	}
	c.respsLock.Unlock()
	c.netConn.Close()
	// TODO: reset connReader and connWriter, put them to pool
	fmt.Println("After release all c.waitingResps")
}

func (s *serverV1) serve(c *PeerConn, trans *NetTransport) {
	//fmt.Println("start read loop")
	go c.processRespLoop()
	for {
		apiKey, correlationId, payloadSize, err := c.readHeader()
		if err != nil {
			// when read failed, close exitChan, and delete conn from pool
			fmt.Println("Failed to read in readloop, err:", err)
			c.exit()

			if c.deleteCallback != nil {
				//fmt.Println("DELETE connection in pool")
				c.deleteCallback()

			} else {
				fmt.Println("Not need to delete due to this connection is not in pool")
			}
			//fmt.Println("Out of Readloop")
			return
		}
		//payload := make([]byte, payloadSize)
		payload := bspool.Get(payloadSize)
		io.ReadFull(c.connReader, payload)

		if IsRequestKey(int(apiKey)) {
			var reqPayload RPCPayload

			switch apiKey {
			case RpcAppendEntries:
				reqPayload = new(AppendEntriesRequest)
			case RpcRequestVote:
				reqPayload = new(RequestVotePayload)
			}

			reqPayload.DecodeBinary(payload)
			bspool.Put(payload)

			var rpc *RPC = &RPC{
				Type:          apiKey,
				CorrelationID: correlationId,
				RespChan:      c.outRespChan,
				ExitChan:      c.exitChan,
				Request:       reqPayload,
			}
			trans.rpcChan <- rpc
			continue
		}
		//fmt.Println("len resp payload:", len(payload))

		var rpcPayload RPCPayload
		switch apiKey {
		case RpcRespAppendEntries:
			rpcPayload = new(AppendEntriesResponse)
		case RpcRespRequestVote:
			rpcPayload = new(RequestVoteResp)
			// TODO: handle other rpc response type
		}

		rpcPayload.DecodeBinary(payload)
		bspool.Put(payload)

		c.respsLock.RLock()
		respChan, ok := c.waitingResps[correlationId]
		if l := len(c.waitingResps[correlationId]); l > 2 {
			fmt.Println("len waiting resp:", l)
		}
		if ok {
			c.respsLock.RUnlock()
			//fmt.Println("FOUND resp channel for correlation id", correlationId)

			respChan <- RPCResponse{
				Type:          apiKey,
				Response:      rpcPayload,
				CorrelationID: correlationId,
			}
			c.respsLock.Lock()
			delete(c.waitingResps, correlationId)
			c.respsLock.Unlock()
			continue
		}
		c.respsLock.RUnlock()

		if cid := atomic.LoadUint32(&c.skipResponseToCID); cid != 0 && cid >= correlationId {
			continue
		}

		//fmt.Println("Put resp for ", correlationId, "to resp channel")
		c.inRespChan <- RPCResponse{
			Type:          apiKey,
			Response:      rpcPayload,
			CorrelationID: correlationId,
		}
	}
}

func (c *PeerConn) processRespLoop() {
	for {
		select {
		case <-c.exitChan:
			//fmt.Println("Stop Process the outRespChan, return ")
			return

		case rpcResp, ok := <-c.outRespChan:
			if !ok {
				return
			}

			//fmt.Println("RECV rpcResp: ")
			// TODO:

			c.writeLock.Lock()
			//binary.BigEndian.PutUint16(c.writeHeadBuf[:2], rpcResp.Type)
			//binary.BigEndian.PutUint32(c.writeHeadBuf[2:6], rpcResp.CorrelationID)
			l := rpcResp.Response.SizeBinary()
			//fmt.Println("Send response for rpc call, size:", l)

			buf := bspool.Get(10 + l)
			binary.BigEndian.PutUint16(buf[:2], rpcResp.Type)
			binary.BigEndian.PutUint32(buf[2:6], rpcResp.CorrelationID)
			binary.BigEndian.PutUint32(buf[6:10], l)

			//data := rpcResp.Response.EncodeBinary()

			//c.connWriter.Write(c.writeHeadBuf[:])
			rpcResp.Response.WriteTo(buf[10:])

			c.connWriter.Write(buf)
			bspool.Put(buf)
			err := c.connWriter.Flush()
			if err != nil {
				c.writeLock.Unlock()
				return
			}
			c.writeLock.Unlock()
		}

	}
}

var ErrConnExisted = errors.New("connection existed for the address")

func (trans *NetTransport) ListenAndRun() error {
	addr := fmt.Sprintf("%s:%d", trans.clusterIPAddr, trans.clusterPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	fmt.Println("listening at addr", addr)
	trans.listener = listener
	for {
		netConn, err := listener.Accept()
		if err != nil {
			fmt.Printf("failed to accept new conn, %s", err.Error())
			return err
		}
		// this type of connection is not added to pool, so do not set deletecallback
		conn := trans.newPeerConn(netConn, 4096, nil)

		var server peerServer = new(serverV1)
		go server.serve(conn, trans)
	}
}

type TypeCorrelationID = uint32
type PeerConn struct {
	PeerAddr     string
	netConn      net.Conn
	connReader   *bufio.Reader
	connWriter   *bufio.Writer
	readHeadBuf  [10]byte
	writeHeadBuf [10]byte
	writeLock    sync.Mutex
	reqCount     uint32

	outRespChan chan RPCResponse
	inRespChan  chan RPCResponse

	skipResponseToCID TypeCorrelationID

	respsLock      sync.RWMutex
	waitingResps   map[TypeCorrelationID]chan RPCResponse
	readyChan      chan struct{}
	exitChan       chan struct{}
	deleteCallback func()
}

func (c *PeerConn) init(netConn net.Conn, peerAddr string, deleteCallback func()) {
	c.PeerAddr = peerAddr
	c.netConn = netConn

	c.deleteCallback = deleteCallback

	c.exitChan = make(chan struct{})
	c.inRespChan = make(chan RPCResponse, 10)
	c.outRespChan = make(chan RPCResponse, 10)

	c.connReader = bufio.NewReader(netConn)
	c.connWriter = bufio.NewWriterSize(netConn, 4096)
	c.waitingResps = make(map[uint32]chan RPCResponse)
}

func (t *NetTransport) newPeerConn(netConn net.Conn, readSize int, deleteCallback func()) *PeerConn {

	return &PeerConn{
		netConn:           netConn,
		connReader:        bufio.NewReaderSize(netConn, readSize),
		connWriter:        bufio.NewWriterSize(netConn, 4096),
		readHeadBuf:       [10]byte{},
		writeHeadBuf:      [10]byte{},
		writeLock:         sync.Mutex{},
		reqCount:          0,
		outRespChan:       make(chan RPCResponse, 10),
		inRespChan:        make(chan RPCResponse, 10),
		respsLock:         sync.RWMutex{},
		waitingResps:      make(map[uint32]chan RPCResponse),
		exitChan:          make(chan struct{}),
		skipResponseToCID: 0,
		deleteCallback:    deleteCallback,
	}
}

func (c *PeerConn) readHeader() (apiKey uint16, correlationID uint32, payloadSize uint32, err error) {
	_, err = io.ReadFull(c.connReader, c.readHeadBuf[:])
	if err != nil {
		return
	}

	apiKey = binary.BigEndian.Uint16(c.readHeadBuf[:2])
	correlationID = binary.BigEndian.Uint32(c.readHeadBuf[2:6])
	payloadSize = binary.BigEndian.Uint32(c.readHeadBuf[6:10])
	return
}

func (c *PeerConn) makeRPC(rpcType uint16, payload RPCPayload, isPipeline bool) (<-chan RPCResponse, uint32, error) {
	// There are 2 goroutine that write to connection
	// [replicate goroutine,  handle out response goroutine]

	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	correlationID := atomic.AddUint32(&c.reqCount, 1)
	//buf := payload.Encode()
	//c.buffer.Reset()
	//buf := bspool.Get(payload.SizeBinary())
	sz := payload.SizeBinary()
	//fmt.Println("leader sends request with buf len:", sz)
	buf := make([]byte, sz+10)
	//buf := bspool.Get(sz + 10)

	binary.BigEndian.PutUint16(buf[:2], rpcType)
	binary.BigEndian.PutUint32(buf[2:6], correlationID)
	binary.BigEndian.PutUint32(buf[6:10], sz)

	/* _, err := c.connWriter.Write(c.writeHeadBuf[:])

	if err != nil {
		return nil, 0, err
	} */
	payload.WriteTo(buf[10:])

	_, err := c.connWriter.Write(buf)
	//bspool.Put(buf)
	if err != nil {
		return nil, 0, err
	}

	err = c.connWriter.Flush()

	if err != nil {
		fmt.Printf("Failed to flush data to %s in cid %d\n", c.PeerAddr, correlationID)
		return nil, 0, err
	}
	//fmt.Printf("Flushed data to %s in cid %d\n", c.PeerAddr, correlationID)
	//fmt.Println("Flush RPC request successfully")
	var respChan = c.inRespChan
	if !isPipeline {
		respChan = make(chan RPCResponse)
		c.respsLock.Lock()
		c.waitingResps[correlationID] = respChan
		c.respsLock.Unlock()
	}

	return respChan, correlationID, nil
}

var ErrPeerNotReady = errors.New("peer is not ready")

// bool = true when this conn is just created in this call
func (trans *NetTransport) getOrCreateConn(addr string) (*PeerConn, bool, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {

		return nil, false, err
	}
	trans.poolLock.RLock()
	conn, ok := trans.poolConn[addr]
	if ok {
		trans.poolLock.RUnlock()
		// when connection failed, delete it immediately
		//fmt.Printf("found one connection at addr %s created, wait until it ready\n", addr)
		<-conn.readyChan
		//fmt.Println("conn ready")
		return conn, true, nil
	}

	trans.poolLock.RUnlock()

	var connRet *PeerConn

	trans.poolLock.Lock()
	c, ok := trans.poolConn[addr]
	if ok {
		trans.poolLock.Unlock()
		connRet = c
		// before waiting conn ready, must relase pool lock first
		<-connRet.readyChan

	} else {
		connRet = new(PeerConn)
		connRet.readyChan = make(chan struct{})
		trans.poolConn[addr] = connRet
		trans.poolLock.Unlock()

		netConn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			close(connRet.readyChan)
			trans.poolLock.Lock()
			delete(trans.poolConn, addr)
			trans.poolLock.Unlock()
			return nil, false, err
		}

		fmt.Println("ESTABLISHED connection with peer addr: ", addr, "successfully")
		connRet.init(netConn, addr, func() {
			trans.poolLock.Lock()
			delete(trans.poolConn, addr)
			trans.poolLock.Unlock()
		})

		server := serverV1{}
		go server.serve(connRet, trans)

		close(connRet.readyChan)
	}

	//fmt.Println("GET out connection")
	return connRet, false, nil
}

func (trans *NetTransport) AppendEntries(addr string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	conn, _, err := trans.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}
	if len(conn.PeerAddr) == 0 {
		return nil, ErrPeerNotReady
	}
	// remote proc
	respChan, _, err := conn.makeRPC(RpcAppendEntries, req, false)
	if err != nil {
		return nil, err
	}

	rpcResp, ok := <-respChan

	if !ok {
		return nil, ErrPeerNotReady
	}
	resp := rpcResp.Response.(*AppendEntriesResponse)
	return resp, nil
}

type NetPipeline struct {
	conn     *PeerConn
	respChan <-chan RPCResponse
	metrics  pipelineMetrics
}

func (trans *NetTransport) CreatePipeline(addr string) (Pipeline, error) {
	conn, _, err := trans.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}

	return &NetPipeline{
		conn:     conn,
		respChan: conn.inRespChan,
		metrics: pipelineMetrics{
			trips: make(map[uint32]pipelineTrip),
		},
	}, nil
}
func (trans *NetTransport) InstallSnapshot(addr string) {}

func (p *NetPipeline) ResponseChan() <-chan RPCResponse {
	return p.respChan
}

func (p *NetPipeline) AppendEntries(req *AppendEntriesRequest) (uint32, error) {
	_, cid, err := p.conn.makeRPC(RpcAppendEntries, req, true)
	return cid, err
}

func (p *NetPipeline) Close() {
	atomic.StoreUint32(&p.conn.skipResponseToCID, p.Metrics().lastSendCID)
}

func (p *NetPipeline) Metrics() *pipelineMetrics {
	return &p.metrics
}

func (trans *NetTransport) RequestVote(addr string, payload *RequestVotePayload) (resp *RequestVoteResp, err error) {
	// using binary protocol
	conn, _, err := trans.getOrCreateConn(addr)
	if err != nil {
		return
	}
	respChan, _, err := conn.makeRPC(RpcRequestVote, payload, false)
	if err != nil {
		return
	}
	rpcResp, ok := <-respChan

	if !ok {
		return nil, ErrPeerNotReady
	}
	if rpcResp.Response == nil {
		return
	}
	resp = rpcResp.Response.(*RequestVoteResp)

	return
}
