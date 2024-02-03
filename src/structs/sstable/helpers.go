package sstable

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/utils"
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

// gets an ending offset for index, depending is sstable single file or not
func (sstable *SSTable) getEndingOffsetSummary(singleFile bool) int {
	var endingOffset int

	if singleFile {
		endingOffset = int(sstable.MerkleOffset)
	} else {
		offset, _ := utils.GetFileLength(sstable.Summary)
		endingOffset = int(offset)
	}
	return endingOffset
}

// gets an ending offset for index/data, depending is sstable single file or not
// offset1 and offset2 is different for index and data, so we need to pass it as a parameter
func getEndingOffset(singleFile bool, file *os.File, offset1, offset2, endingOffset int64) uint64 {
	if endingOffset != 0 {
		endingOffset += offset1
	} else {
		if singleFile {
			endingOffset = int64(offset2)
		} else {
			fileSize, _ := utils.GetFileLength(file)
			endingOffset = fileSize
		}
	}
	return uint64(endingOffset)
}

// returns key, offset in index/data and bytes read
func readBlock(file *os.File) (string, uint64, int, error) {
	var keySize uint64
	var offset uint64
	var key string
	var err error

	keySizeBuffer := make([]byte, 8)
	_, err = file.Read(keySizeBuffer)

	if err != nil {
		return "", 0, 0, err
	}
	keySize = binary.BigEndian.Uint64(keySizeBuffer)

	keyBytes := make([]byte, keySize)
	_, err = file.Read(keyBytes)

	if err != nil {
		return "", 0, 0, err
	}

	key = string(keyBytes)

	offsetBuffer := make([]byte, 8)
	_, err = file.Read(offsetBuffer)

	if err != nil {
		return "", 0, 0, err
	}

	offset = binary.BigEndian.Uint64(offsetBuffer)

	totalSize := 16 + int(keySize)
	return key, offset, totalSize, nil
}
