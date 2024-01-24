package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/merkletree"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

const (
	DIR_NAME      = "sstable_"
	FILE_NAME     = "usertable-data-"
	PATH          = "../data/sstable"
	START_COUNTER = "0001"
)

type SSTable struct {
	data           *os.File
	index          *os.File
	summary        *os.File
	compresionInfo *os.File
	bf             *bloomFilter.BloomFilter
	merkle         *merkletree.MerkleTree
	minKey         string
	maxKey         string
}

// function that creates a new sstable
// returns pointer to sstable if it is successfully created, otherwise returns an error
func CreateSStable(records []*model.Record, singleFile, compressionOn bool, indexDegree, summaryDegree int) (*SSTable, error) {

	sstable := SSTable{}
	dirNames, err := utils.GetDirContent(PATH)

	if err != nil {
		return nil, err
	}

	var path string
	if len(dirNames) == 0 {
		path = fmt.Sprintf("%s/%s%s", PATH, DIR_NAME, START_COUNTER)
		dirNames = append(dirNames, fmt.Sprintf("%s%s", DIR_NAME, START_COUNTER))
	} else {
		dirNames, path, err = utils.GetNextContentName(dirNames, PATH, DIR_NAME)
		if err != nil {
			return nil, err
		}
	}

	err = os.Mkdir(path, os.ModeDir)
	if err != nil {
		return nil, err
	}

	if compressionOn {
		// TODO: implement compression
	}

	sstable.bf = bloomFilter.NewBf(len(records), 0.001)
	for _, record := range records {
		sstable.bf.Insert(record.Key)
	}
	sstable.minKey = records[0].Key
	sstable.maxKey = records[len(records)-1].Key
	sstable.merkle, _ = merkletree.NewTree(sstable.serializeData(records))

	if singleFile {
		err = sstable.makeSingleFile(path, records, indexDegree, summaryDegree)
	} else {
		err = sstable.makeSeparateFiles(path, records, indexDegree, summaryDegree)
	}

	if err != nil {
		return nil, err
	}

	return &sstable, nil
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

// function that calls every serialization
// saves header one after the other -> length of min key, so we know how we need to read to get min key
// same things is done for max key, then we saved offsets for data, index and summary
// returns content (that one needed for building merkle tree)
func (sstable *SSTable) writeToSingleFile(records []*model.Record, n int, m int) {
	var content [][]byte

	minKeyBytes := []byte(sstable.minKey)
	maxKeyBytes := []byte(sstable.maxKey)

	var minKeyLength uint64 = uint64(len(minKeyBytes))
	var maxKeyLength uint64 = uint64(len(maxKeyBytes))
	var bfOffset uint64 = 6*8 + minKeyLength + maxKeyLength
	var contentBf [][]byte = [][]byte{sstable.bf.Serialize()}
	var dataOffset uint64 = calculateOffset(contentBf, bfOffset)
	var contentData [][]byte = sstable.serializeData(records)
	var indexOffset uint64 = calculateOffset(contentData, dataOffset)
	var contentIndex [][]byte = sstable.serializeIndexSummary(contentData, n)
	var summaryOffset uint64 = calculateOffset(contentIndex, indexOffset)
	var contentSummary [][]byte = sstable.serializeIndexSummary(contentIndex, m)

	content = append(content, uint64ToBytes(minKeyLength))
	content = append(content, minKeyBytes)
	content = append(content, uint64ToBytes(maxKeyLength))
	content = append(content, maxKeyBytes)
	content = append(content, uint64ToBytes(bfOffset), uint64ToBytes(dataOffset), uint64ToBytes(indexOffset), uint64ToBytes(summaryOffset))
	content = append(content, contentBf...)
	content = append(content, contentData...)
	content = append(content, contentIndex...)
	content = append(content, contentSummary...)

	sstable.writeToFile(sstable.data, content)

}

// helper used in previous function
func uint64ToBytes(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}

// this functions calculates lenght of bytes for forwarded content and returns it
func calculateOffset(content [][]byte, offset uint64) uint64 {
	for i := 0; i < len(content); i++ {
		offset += uint64(len(content[i]))
	}
	return uint64(offset)
}

// (helper) serializes records - turns them to [][]byte
func (sstable *SSTable) serializeData(records []*model.Record) [][]byte {
	var content [][]byte
	for _, record := range records {
		content = append(content, record.ToBytes())
	}
	return content
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

func Search(key string) error {
	dirContent, err := utils.GetDirContent(PATH)
	if err != nil {
		return err
	}
	for i := 0; i < len(dirContent); i++ { // search through all sstables
		var dirName string = dirContent[i]
		content, err := utils.GetDirContent(fmt.Sprintf("%s/%s", PATH, dirName)) // get content of sstable, so we can check if sstable is in single file or in seperate files

		if err != nil {
			return err
		}

		if len(content) == 1 {
			ReadSStableSingleFile()
		} else {
			// ReadSSTableSeparateFiles()
		}
	}
	// TODO: check len of dir
	// Accoring to len of dir call different methods
	// In that methods read sstable and set it
	// then come back to this method and do search which is same for both
	return nil
}

func ReadSStableSingleFile() {
	//TODO: implement logic for reading sstable ehich is in single file
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
