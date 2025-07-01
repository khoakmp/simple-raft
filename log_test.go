package simpleraft

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestEncodeLog(t *testing.T) {
	l := Log{
		Type:  LogClusterConfig,
		Index: 1,
		Term:  1,
		Data:  []byte("hee"),
	}
	buf := l.EncodeBinary()
	l1 := new(Log)
	l1.DecodeBinary(buf)
	fmt.Println(string(l1.Data))
}

func TestDiskspeed(t *testing.T) {
	//os.OpenFile("log2.dat", os.O_RDWR|os.O_CREATE, os.ModePerm)

	f, err := os.OpenFile("log2.dat", os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 10)
	n := 1000000
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = 'a'
	}

	for i := 0; i < 1; i++ {
		start := time.Now()

		_, err = f.Write(buf)
		if err != nil {
			t.Error(err)
			return
		}

		fmt.Println("total time:", time.Since(start))
		//<-time.After(time.Millisecond * 20)
	}

}

func TestSlice(t *testing.T) {
	buf := make([]byte, 0, 10)
	println(buf)
	buf = append(buf, '1')
	buf = append(buf, '2')
	buf = buf[2:]
	println(buf)
}
