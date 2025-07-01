package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	simpleraft "github.com/khoakmp/simple-raft"
	"gopkg.in/yaml.v3"
)

type Student struct {
	name string
	id   int
}

func main() {
	/* serverAddr := "127.0.0.1:5051"
	addr, _ := net.ResolveTCPAddr("tcp", serverAddr)

	go func() {
		<-time.After(time.Second)

		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			fmt.Printf("Failed to dial %s , err: %s\n", serverAddr, err.Error())
			return
		}
		defer conn.Close()
		rc, _ := conn.SyscallConn()
		var connFd uintptr = 0
		rc.Control(func(fd uintptr) {
			connFd = fd
		})

		conn.Write(nil)
		conn.Read(nil)
		fmt.Println("file descriptor of client conn:", connFd)

		// dung la ko chac lam do,hoi do dun gno dan
		flags, err := unix.FcntlInt(connFd, unix.F_GETFL, 0)
		if err != nil {
			fmt.Println("failed to get flags:", err)
			return
		}
		fmt.Println("is non-blocking:", flags&unix.O_NONBLOCK > 0)

	}()
	l, err := net.Listen("tcp", serverAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	var connFd uintptr = 0
	rc, _ := tcpConn.SyscallConn()
	rc.Control(func(fd uintptr) {
		connFd = fd
	})

	fmt.Println("file descriptor of client conn:", connFd)

	flags, err := unix.FcntlInt(connFd, unix.F_GETFL, 0)
	if err != nil {
		fmt.Println("failed to get flags:", err)
		return
	}
	fmt.Println("is non-blocking:", flags&unix.O_NONBLOCK > 0) */
	/* id, addr := os.Args[1], os.Args[2]

	simpleraft.RunRaftNode(id, addr) */
	//simpleraft.RunRaftLeader()
	//readVote()
	cmd := os.Args[1]
	if cmd == "s" {
		runCluster()
	} else {
		runClient()
	}
}
func runClient() {
	conn, err := net.Dial("tcp", "127.0.0.1:7052")
	if err != nil {
		fmt.Println("failed to connect to server: ", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	numReq := uint32(10000)
	bufReader := bufio.NewReaderSize(conn, 1024)
	start := time.Now()
	nt := uint32(100)

	go func() {
		defer wg.Done()
		for i := uint32(0); i < numReq*nt; i++ {
			var headbuf [10]byte
			_, err := io.ReadFull(bufReader, headbuf[:])
			if err != nil {
				fmt.Println("Failed to read header:", err)
				return
			}
			sz := binary.BigEndian.Uint32(headbuf[6:10])
			buf := make([]byte, sz)
			_, err = io.ReadFull(bufReader, buf)
			if err != nil {
				fmt.Println("Failed to read payload:", err)
				return
			}
		}

		fmt.Println("Total time for getting all response:", time.Since(start))
	}()

	bufWriter := bufio.NewWriterSize(conn, 4096)

	payloadBuf := []byte("kmp something to handle")
	for j := uint32(0); j < nt; j++ {
		for i := uint32(0); i < numReq; i++ {
			var headbuf [10]byte
			binary.BigEndian.PutUint16(headbuf[0:2], simpleraft.CommandType)
			binary.BigEndian.PutUint32(headbuf[2:6], j*(numReq)+i+1)
			binary.BigEndian.PutUint32(headbuf[6:10], uint32(len(payloadBuf)))
			bufWriter.Write(headbuf[:])
			bufWriter.Write(payloadBuf)
		}
		bufWriter.Flush()
	}

	//fmt.Println("Total time to commit:", time.Since(start))
	//<-time.After(time.Second)
	wg.Wait()
}
func runCluster() {
	numNode := 3
	var nodes []*simpleraft.Raft = make([]*simpleraft.Raft, numNode)
	// with install snapshot => not ez to do that dung ?
	// dung do,  nen co cai gi de ma
	servers := []simpleraft.Server{
		{ID: "server-1", Addr: "127.0.0.1:5051"},
		{ID: "server-2", Addr: "127.0.0.1:5052"},
		{ID: "server-3", Addr: "127.0.0.1:5053"},
	}

	/* for i := 0; i < numNode; i++ {
		simpleraft.DeleteAllFilesOfDir(fmt.Sprintf("%s_test_logs", servers[i].ID))
	} */

	for i := 0; i < numNode; i++ {
		nodes[i] = simpleraft.CreateRaftNode(string(servers[i].ID), servers[i].Addr)
		//nodes[i].Start()
	}
	<-time.After(time.Millisecond * 100)
	for i := 0; i < numNode; i++ {
		nodes[i].Start()
	}

	for i := 0; i < numNode; i++ {
		nodes[i].Wait()
	}
}

func readVote() {
	f, err := os.Open("./examples/vote.yaml")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	var lastVote simpleraft.VoteStored
	err = d.Decode(&lastVote)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lastVote)
}
