package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
)

// returns 2 offsets between which we should search index
// if offset2 == 0, then search until the end of index/summary
// USED ALSO FOR SEARCHING IN SUMMARY
func (sstable *SSTable) searchIndex(file *os.File, offset1 int, offset2 int, key string) (uint64, uint64, error) {
	var prev, next string
	var targetOffset1, targetOffset2 uint64
	var bytesRead int
	var err error

	file.Seek(int64(offset1), 0)
	prev, targetOffset1, bytesRead, err = readBlock(file) // read first block
	offset1 += bytesRead

	if err != nil {
		return 0, 0, err
	}

	for offset1 < offset2 {
		next, targetOffset2, bytesRead, err = readBlock(file)
		if err != nil {
			return 0, 0, err
		}
		if key >= prev && key < next { // if key is between prev and next, we found the right target offsets
			break
		}
		offset1 += bytesRead
		prev = next
		targetOffset1 = targetOffset2
	}

	if key >= next {
		return targetOffset1, 0, nil
	}

	return targetOffset1, targetOffset2, nil
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
