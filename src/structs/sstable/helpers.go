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

// reads block in summary/index
// returns key, offset in index/data, number of bytes from content that are read
func readBlock(content []byte) (string, uint64, int) {

	if len(content) == 0 {
		return "", 0, 0
	}
	var keySize uint64
	binary.Read(bytes.NewReader(content[:8]), binary.BigEndian, &keySize)

	keyBytes := make([]byte, keySize)
	binary.Read(bytes.NewReader(content[8:8+keySize]), binary.BigEndian, &keyBytes)
	var offset uint64

	binary.Read(bytes.NewReader(content[8+keySize:8+keySize+8]), binary.BigEndian, &offset)
	totalSize := 16 + int(keySize)

	return string(keyBytes), offset, totalSize
}
