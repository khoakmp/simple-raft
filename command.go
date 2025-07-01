package simpleraft

import (
	"bufio"
	"os"
)

func (r *Raft) StartCommand() {
	reader := bufio.NewReader(os.Stdin)
	for {
		reader.ReadLine()
	}
}
