package sstable

import (
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
	data, index, summary                                           *os.File
	bf                                                             *bloomFilter.BloomFilter
	merkle                                                         *merkletree.MerkleTree
	minKey, maxKey                                                 string
	dataOffset, indexOffset, summaryOffset, merkleOffset, bfOffset int64
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

// returns nil as first param if record is not found
// second param returns error if it occured during actions connected to files, otherwise returns nil
func Search(key string) (*model.Record, error) {

	dirContent, err := utils.GetDirContent(PATH) // dirContent - names of all sstables dirs
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(dirContent); i++ { // search through all sstables
		var dirName string = dirContent[i] // one sstable

		path := fmt.Sprintf("%s/%s", PATH, dirName)
		content, err := utils.GetDirContent(path) // get content of sstable, so we can check if sstable is in single file or in seperate files
		if err != nil {
			return nil, err
		}

		var sstable *SSTable

		if len(content) == 1 {
			sstable, err = LoadSStableSingle(path)
		} else {
			sstable, err = LoadSSTableSeparate(path)
		}
		if err != nil {
			return nil, err
		}

		sstable.loadBF(len(content) != 1, dirName)
		if !sstable.bf.Find(key) { // then record is not in this sstable, go to next
			continue
		}
		if key < sstable.minKey || key > sstable.maxKey {
			continue
		}

		var data []byte
		data, err = sstable.loadSummary(len(content) != 1)
		if err != nil {
			return nil, err
		}

		offset1, offset2 := sstable.searchSummary(data, key)

		data, err = sstable.loadIndex(len(content) != 1, int(offset1), int(offset2)) // TODO: myd to recieve uint64?
		if err != nil {
			return nil, err
		}

		offset1, offset2 = sstable.searchIndex(data, key)
		record := sstable.searchData(len(content) != 1, int(offset1), int(offset2), key)
		return record, nil
	}
	return nil, nil
}
