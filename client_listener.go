package simpleraft

import (
	"fmt"
	"net"
)

type clientListener struct{}

func (lis *clientListener) listenAndServe(raft *Raft) {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", raft.config.ClientIPAddr, raft.config.ClientPort))
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to listen for client, caused by:", err)
			break
		}
		client := newClient(conn)
		server := &server{}
		go server.serve(client, raft)
	}
}
