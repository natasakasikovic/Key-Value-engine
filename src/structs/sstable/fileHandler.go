package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
)

// returns pointer to SSTable if succesfuly created, otherwise returns an error
func LoadSStableSingle(p string) (*SSTable, error) {
	var sstable *SSTable = &SSTable{}
	var path string = fmt.Sprintf("%s/%s%s", p, FILE_NAME, "DataIndexSummary.db")

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	sstable.index = file
	sstable.data = file
	sstable.summary = file

	// set min key
	var minKeyLength uint64
	err = binary.Read(file, binary.BigEndian, &minKeyLength)
	if err != nil {
		return nil, err
	}
	minKeyBytes := make([]byte, minKeyLength)
	_, err = file.Read(minKeyBytes)
	if err != nil {
		return nil, err
	}
	sstable.minKey = string(minKeyBytes)

	// set max key
	var maxKeyLength uint64
	err = binary.Read(file, binary.BigEndian, &maxKeyLength)
	if err != nil {
		return nil, err
	}
	maxKeyBytes := make([]byte, maxKeyLength)
	_, err = file.Read(maxKeyBytes)
	if err != nil {
		return nil, err
	}
	sstable.maxKey = string(maxKeyBytes)

	// set offsets
	offsets := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		err = binary.Read(file, binary.BigEndian, &offsets[i])
		if err != nil {
			return nil, err
		}
	}
	sstable.bfOffset, sstable.dataOffset, sstable.indexOffset, sstable.summaryOffset, sstable.merkleOffset =
		int64(offsets[0]+8), int64(offsets[1]+8), int64(offsets[2]+8), int64(offsets[3]+8), int64(offsets[4]+8)

	return sstable, nil
}

// makes separate files: data, summary, index;
// param path: path to the folder where files will be saved;
// param records: array of pointers to records from memtable;
// param n: index degree;
// param m: summary degree;
// returns error: if it occured during actions connected to files;
func (sstable *SSTable) makeSeparateFiles(path string, records []*model.Record, n int, m int) error {

	data, _ := makeFile(path, "Data")
	summary, _ := makeFile(path, "Summary")
	index, _ := makeFile(path, "Index")
	filter, _ := makeFile(path, "Filter")
	merkle, _ := makeFile(path, "Merkle")
	if data != nil && index != nil && summary != nil {
		sstable.data = data
		sstable.index = index
		sstable.summary = summary

		minKeyBytes := []byte(sstable.minKey)
		maxKeyBytes := []byte(sstable.maxKey)

		var minKeyLength uint64 = uint64(len(minKeyBytes))
		var maxKeyLength uint64 = uint64(len(maxKeyBytes))

		minKeyInfoSerialized := append(uint64ToBytes(minKeyLength), minKeyBytes...)
		maxKeyInfoSerialized := append(uint64ToBytes(maxKeyLength), maxKeyBytes...)

		var contentSummary [][]byte = [][]byte{minKeyInfoSerialized, maxKeyInfoSerialized}

		var contentData [][]byte = sstable.serializeData(records)
		var contentIndex [][]byte = sstable.serializeIndexSummary(contentData, n)
		contentSummary = append(contentSummary, sstable.serializeIndexSummary(contentIndex, m)...)

		var contentBf [][]byte = [][]byte{sstable.bf.Serialize()}

		var contentMerkle [][]byte = [][]byte{sstable.merkle.Serialize()}

		sstable.writeToFile(data, contentData)
		sstable.writeToFile(index, contentIndex)
		sstable.writeToFile(summary, contentSummary)
		sstable.writeToFile(filter, contentBf)
		sstable.writeToFile(merkle, contentMerkle)
		return nil
	}
	return errors.New("error occured")
}

// function that calls every serialization
// saves header one after the other -> length of min key, so we know how we need to read to get min key
// same things is done for max key, then we saved offsets for data, index and summary
func (sstable *SSTable) writeToSingleFile(records []*model.Record, n int, m int) {
	var content [][]byte

	minKeyBytes := []byte(sstable.minKey)
	maxKeyBytes := []byte(sstable.maxKey)

	var minKeyLength uint64 = uint64(len(minKeyBytes))
	var maxKeyLength uint64 = uint64(len(maxKeyBytes))
	var bfOffset uint64 = 7*8 + minKeyLength + maxKeyLength
	var contentBf [][]byte = [][]byte{sstable.bf.Serialize()}
	var dataOffset uint64 = calculateOffset(contentBf, bfOffset)
	var contentData [][]byte = sstable.serializeData(records)
	var indexOffset uint64 = calculateOffset(contentData, dataOffset)
	var contentIndex [][]byte = sstable.serializeIndexSummary(contentData, n)
	var summaryOffset uint64 = calculateOffset(contentIndex, indexOffset)
	var contentSummary [][]byte = sstable.serializeIndexSummary(contentIndex, m)
	var merkleOffset uint64 = calculateOffset(contentSummary, summaryOffset)
	var contentMerkle [][]byte = [][]byte{sstable.merkle.Serialize()}

	content = append(content, uint64ToBytes(minKeyLength))
	content = append(content, minKeyBytes)
	content = append(content, uint64ToBytes(maxKeyLength))
	content = append(content, maxKeyBytes)
	content = append(content, uint64ToBytes(bfOffset))
	content = append(content, uint64ToBytes(dataOffset))
	content = append(content, uint64ToBytes(indexOffset))
	content = append(content, uint64ToBytes(summaryOffset))
	content = append(content, uint64ToBytes(merkleOffset))
	content = append(content, contentBf...)
	content = append(content, contentData...)
	content = append(content, contentIndex...)
	content = append(content, contentSummary...)
	content = append(content, contentMerkle...)

	sstable.writeToFile(sstable.data, content)

}

// params: n - index degree, m - summary degree
// makes sstable which is in single file, all *os.File in struct refer to same file
// this function also returns error if it occured during actions connected to files
func (sstable *SSTable) makeSingleFile(path string, records []*model.Record, n int, m int) error {
	file, err := makeFile(path, "DataIndexSummary")
	if err != nil {
		return err
	}
	sstable.index, sstable.data, sstable.summary = file, file, file
	sstable.writeToSingleFile(records, n, m)
	return nil
}

// helper - makes files necessary for one sstable
// used in function makeSeparateFiles and makeSingleFile
func makeFile(path string, s string) (*os.File, error) {
	file, err := os.OpenFile(fmt.Sprintf("%s/%s%s.db", path, FILE_NAME, s), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// writes serialized content to file
func (sstable *SSTable) writeToFile(file *os.File, arr [][]byte) {
	var content []byte
	for i := 0; i < len(arr); i++ {
		content = append(content, arr[i]...)
	}
	file.Write(content)
	defer file.Close()
}

// params: bool singleFile - if we load from single file first read first 8 bytes to check size of bf
// if it is not single file then read all bytes from file
func (sstable *SSTable) loadBF(separateFile bool, path string) error {
	var file *os.File
	var err error

	if separateFile {
		path = fmt.Sprintf("%s/%s/%s%s", PATH, path, FILE_NAME, "Filter.db")
		file, err = os.Open(path)
		if err != nil {
			return err
		}
	} else {
		file = sstable.data
	}

	var toRead []byte
	if separateFile {
		toRead, err = io.ReadAll(file)
	} else {
		toRead = make([]byte, int(sstable.dataOffset-sstable.bfOffset))
		file.Seek(sstable.bfOffset, 0)
		_, err = file.Read(toRead)
	}

	sstable.bf = bloomFilter.Deserialize(toRead)
	return nil
}
