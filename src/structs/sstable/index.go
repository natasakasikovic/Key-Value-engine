package sstable

import (
	"bytes"
	"encoding/binary"
)

func (sstable *SSTable) searchIndex(data []byte, key string) (uint64, uint64) {
	var prev, next string
	var offset1, offset2 uint64
	var bytesRead int

	prev, offset1, bytesRead = readBlock(data)
	data = data[bytesRead:]
	for len(data) > 0 {
		next, offset2, bytesRead = readBlock(data)
		data = data[bytesRead:] // remove bytes that we have read
		if key >= prev && key < next {
			break
		}
		prev = next
		offset1 = offset2
	}
	if key > next { // if key is greater that last exisitng key in summary, then read index [offset1:]
		return offset1, 0
	}

	return offset1, offset2
}

func (sstable *SSTable) serializeIndexSummary(content [][]byte, n int) [][]byte {
	var retVal [][]byte
	var offset uint64 = 0
	for i := 0; i < len(content); i++ {
		if i%n == 0 {
			keySize, key := getKey(content[i])
			var buffer bytes.Buffer
			binary.Write(&buffer, binary.BigEndian, keySize)
			binary.Write(&buffer, binary.BigEndian, []byte(key))
			binary.Write(&buffer, binary.BigEndian, offset)
			retVal = append(retVal, buffer.Bytes())
		}
		offset += uint64(len(content[i]))

	}
	return retVal
}
