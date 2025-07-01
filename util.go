package simpleraft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	PaddingLeft  = 0
	PaddingRight = 1
	PaddingMid   = 2
)

func padding(s string, l int, padtype int) string {
	if len(s) >= l {
		return s
	}

	var result []byte = make([]byte, l)
	switch padtype {
	case PaddingRight:
		copy(result[:len(s)], []byte(s))
		for i := len(s); i < l; i++ {
			result[i] = ' '
		}
	case PaddingLeft:
		for i := 0; i < l-len(s); i++ {
			result[i] = ' '
		}
		copy(result[l-len(s):], []byte(s))
	case PaddingMid:
		left := (l - len(s)) >> 1
		for i := 0; i < left; i++ {
			result[i] = ' '
		}
		for i := left + len(s); i < l; i++ {
			result[i] = ' '
		}
		copy(result[left:left+len(s)], []byte(s))
	}

	return string(result)
}

func createBorderLine(l int) []byte {
	buf := make([]byte, l+1)
	for i := 0; i < l; i++ {
		buf[i] = '-'
	}
	buf[l] = '\n'
	return buf
}

func decodeHeader(buf []byte) (apiKey uint16, correlationID, payloadSize uint32) {
	apiKey = binary.BigEndian.Uint16(buf[:2])
	correlationID = binary.BigEndian.Uint32(buf[2:6])
	payloadSize = binary.BigEndian.Uint32(buf[6:10])
	return
}

func DeleteAllFilesOfDir(dir string) error {
	// Read all files and directories in the specified directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Iterate through entries and delete each one
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())

		// Remove file or directory
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", entryPath, err)
		}
	}

	return nil
}
