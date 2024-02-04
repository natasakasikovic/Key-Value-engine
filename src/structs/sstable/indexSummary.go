package sstable

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

// returns 2 offsets between which we should search index
// if offset2 == 0, then search until the end of index/summary
// USED ALSO FOR SEARCHING IN SUMMARY
func (sstable *SSTable) searchIndex(file *os.File, offset1 int, offset2 int, key string, compressionMap map[string]uint64) (uint64, uint64, error) {

	if sstable.CompressionOn {
		return sstable.searchIndexCompressed(file, offset1, offset2, key, compressionMap)
	}

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

func (sstable *SSTable) searchIndexCompressed(file *os.File, offset1 int, offset2 int, key string, compressionMap map[string]uint64) (uint64, uint64, error) {
	var prev, next uint64
	var targetOffset1, targetOffset2 uint64
	var bytesRead int
	var err error
	var prevString, nextString string

	file.Seek(int64(offset1), 0)

	// compressionMap isnt sorted by its numbers that represent keys, beacuse one compressionMap is for all sstables
	// for example in first sstable we have key example_key2 which get compressed key 0, but in second sstable we have key example_key2 which get compressed key 1
	// so conclusion is that we need to compare real keys
	prev, targetOffset1, bytesRead, err = readBlockCompressed(file) // read first block
	offset1 += bytesRead
	prevString = utils.GetKeyByValue(prev, compressionMap)

	if err != nil {
		return 0, 0, err
	}

	for offset1 < offset2 {
		next, targetOffset2, bytesRead, err = readBlockCompressed(file)
		if err != nil {
			return 0, 0, err
		}
		nextString = utils.GetKeyByValue(next, compressionMap)
		if key >= prevString && key < nextString { // if key is between prev and next, we found the right target offsets
			break
		}
		offset1 += bytesRead
		prev = next
		targetOffset1 = targetOffset2
	}

	if key >= nextString {
		return targetOffset1, 0, nil
	}

	return targetOffset1, targetOffset2, nil
}

func (sstable *SSTable) serializeIndexSummary(content [][]byte, n int, compressed bool) [][]byte {
	var retVal [][]byte
	var offset uint64 = 0
	for i := 0; i < len(content); i++ {
		if i%n == 0 {
			keySize, key := getKey(content[i], compressed)
			var buffer bytes.Buffer
			if !compressed {
				binary.Write(&buffer, binary.BigEndian, keySize) // write key size if not compressed
			}
			binary.Write(&buffer, binary.BigEndian, key)
			binary.Write(&buffer, binary.BigEndian, offset)
			retVal = append(retVal, buffer.Bytes())
		}
		offset += uint64(len(content[i]))

	}
	return retVal
}
