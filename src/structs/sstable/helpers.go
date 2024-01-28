package sstable

import (
	"bytes"
	"encoding/binary"
)

// this functions calculates lenght of bytes for forwarded content and returns it
func calculateOffset(content [][]byte, offset uint64) uint64 {
	for i := 0; i < len(content); i++ {
		offset += uint64(len(content[i]))
	}
	return uint64(offset)
}

// gets key - made to read key from records, but also from index block
func getKey(item []byte) (uint64, string) {
	var keySize uint64
	buffer := bytes.NewReader(item)
	binary.Read(buffer, binary.BigEndian, &keySize)
	keyBytes := make([]byte, keySize)
	buffer.Read(keyBytes)
	return keySize, string(keyBytes)
}

func uint64ToBytes(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}
