package simpleraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type LogType uint16

const (
	LogNoop          LogType = 0
	LogCommand       LogType = 1
	LogClusterConfig LogType = 2
)

type Log struct {
	Type  LogType `json:"type"`
	Index uint64  `json:"index"`
	Term  uint64  `json:"term"`
	Data  []byte  `json:"data"`
}

func (l *Log) WriteSizeDataTo(buf []byte) (n uint32) {
	logSize := l.SizeBinary()
	//fmt.Println("try to write log to buf with len:", len(buf), "and size+data of log: ", logSize+4)

	binary.BigEndian.PutUint32(buf[:4], logSize)
	binary.BigEndian.PutUint16(buf[4:6], uint16(l.Type))
	binary.BigEndian.PutUint64(buf[6:14], l.Index)
	binary.BigEndian.PutUint64(buf[14:22], l.Term)
	binary.BigEndian.PutUint32(buf[22:26], uint32(len(l.Data)))

	copy(buf[26:26+len(l.Data)], l.Data)
	return 4 + logSize
}
func (l *Log) WriteDataTo(buf []byte) uint32 {
	binary.BigEndian.PutUint16(buf[:2], uint16(l.Type))
	binary.BigEndian.PutUint64(buf[2:10], l.Index)
	binary.BigEndian.PutUint64(buf[10:18], l.Term)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(l.Data)))
	copy(buf[22:22+len(l.Data)], l.Data)
	return 22 + uint32(len(l.Data))
}
func (l *Log) ToSummaryString() string {
	return fmt.Sprintf("(Index: %d, Term: %d)\n", l.Index, l.Term)
}

func (l *Log) ToString() string {
	return fmt.Sprintf("(Type: %d,Index: %d, Term: %d, Data: %s)\n", l.Type, l.Index, l.Term, string(l.Data))
}

func (l *Log) SizeBinary() uint32 {
	return 22 + uint32(len(l.Data))
}

func (l *Log) EncodeBinary() []byte {
	buf := make([]byte, 22+len(l.Data))
	binary.BigEndian.PutUint16(buf[:2], uint16(l.Type))
	binary.BigEndian.PutUint64(buf[2:10], l.Index)
	binary.BigEndian.PutUint64(buf[10:18], l.Term)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(l.Data)))
	copy(buf[22:], l.Data)
	return buf
}

var ErrDecodeLogFailed = errors.New("decode log failed")

func (l *Log) DecodeBinary(buf []byte) error {
	if len(buf) < 22 {
		return ErrDecodeLogFailed
	}
	l.Type = LogType(binary.BigEndian.Uint16(buf[:2]))
	l.Index = binary.BigEndian.Uint64(buf[2:10])
	l.Term = binary.BigEndian.Uint64(buf[10:18])
	sz := binary.BigEndian.Uint32(buf[18:22])
	l.Data = make([]byte, sz)
	copy(l.Data, buf[22:])
	return nil
}

func (l *Log) DecodeBinaryV2(buf []byte) (uint32, error) {
	if len(buf) < 22 {
		return 0, ErrDecodeLogFailed
	}
	l.Type = LogType(binary.BigEndian.Uint16(buf[:2]))
	l.Index = binary.BigEndian.Uint64(buf[2:10])
	l.Term = binary.BigEndian.Uint64(buf[10:18])
	sz := binary.BigEndian.Uint32(buf[18:22])
	l.Data = make([]byte, sz)

	copy(l.Data, buf[22:])
	return 22 + sz, nil
}
func (l *Log) WriteBinaryTo(w io.Writer) (n uint32) {
	var buf [22]byte
	binary.BigEndian.PutUint16(buf[:2], uint16(l.Type))
	binary.BigEndian.PutUint64(buf[2:10], l.Index)
	binary.BigEndian.PutUint64(buf[10:18], l.Term)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(l.Data)))
	w.Write(buf[:])
	w.Write(l.Data)
	n = 22 + uint32(len(l.Data))
	return
}

func (l *Log) WriteSizeAndBinaryTo(w io.Writer) (totalWrite uint32) {
	var szBuf [4]byte
	binary.BigEndian.PutUint32(szBuf[:], 22+uint32(len(l.Data)))
	w.Write(szBuf[:])
	//l.WriteBinaryTo(w)
	totalWrite = 4 + l.WriteBinaryTo(w)
	return
}
