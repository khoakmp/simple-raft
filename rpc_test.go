package simpleraft

import "testing"

func TestEncodeAppendEntriesV2(t *testing.T) {
	enries := []*Log{
		{
			Type:  LogCommand,
			Index: 11,
			Term:  3,
			Data:  []byte("something"),
		},
		{
			Type:  LogCommand,
			Index: 12,
			Term:  3,
			Data:  []byte("other"),
		},
	}
	a := AppendEntriesRequest{
		LeaderID:          "server-1",
		PrevLogIndex:      10,
		PrevLogTerm:       3,
		Term:              3,
		LeaderCommitIndex: 8,
		Entries:           enries,
	}
	sz := a.SizeBinaryV2()
	buf := make([]byte, sz)
	a.WriteToV2(buf)
	a1 := new(AppendEntriesRequest)
	a1.DecodeBinaryV2(buf)
	a1.Print()
}

func TestAppendEntriesResponse(t *testing.T) {
	res := AppendEntriesResponse{
		Success:      true,
		Term:         1,
		LastLogIndex: 12,
		LastLogTerm:  1,
		Stop:         false,
	}
	buf := make([]byte, res.SizeBinary())
	res.WriteTo(buf)
	r1 := new(AppendEntriesResponse)
	r1.DecodeBinary(buf)
	r1.Print()
}
