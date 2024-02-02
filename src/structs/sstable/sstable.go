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
	MinKey, MaxKey, Name                                           string
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
	sstable.Name = dirNames[len(dirNames)-1]

	if compressionOn {
		// TODO: implement compression
	}

	sstable.bf = bloomFilter.NewBf(len(records), 0.001)
	for _, record := range records {
		sstable.bf.Insert(record.Key)
	}
	sstable.MinKey = records[0].Key
	sstable.MaxKey = records[len(records)-1].Key
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

	for i := len(dirContent) - 1; i >= 0; i-- { // search through all sstables, started from newest one
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

		sstable.Name = dirName

		sstable.loadBF(len(content) != 1, dirName) // first ask bloomfilter
		if !sstable.bf.Find(key) {                 // then record is not in this sstable, go to next sstable
			continue
		}
		if key < sstable.MinKey || key > sstable.MaxKey { // if key is not in range of sstable, go to next sstable
			continue
		}

		var endingOffset int = sstable.getEndingOffsetSummary(len(content) == 1)

		// offset1 and offset2 are offsets between which we should search index
		offset1, offset2, err := sstable.searchIndex(sstable.summary, int(sstable.summaryOffset), endingOffset, key)

		if err != nil {
			return nil, err
		}

		if offset2 == 0 { // this means that we need to search until the end of index
			offset2 = uint64(sstable.summaryOffset) - uint64(sstable.indexOffset) // this is the size of index
		} else { // in other case we need to read next value
			sstable.index.Seek(int64(offset2+uint64(sstable.indexOffset)), 0)
			_, _, bytesRead, err := readBlock(sstable.index)
			if err != nil {
				return nil, err
			}
			offset2 += uint64(bytesRead) // we need to increase offset2, so we can read one more value while searching in index
		}

		offset1 += uint64(sstable.indexOffset) // if it is single file starting index offset is ok
		offset2 = getEndingOffset(len(content) == 1, sstable.index, sstable.indexOffset, sstable.summaryOffset, int64(offset2))

		// offset1 and offset2 are offsets between which we should search data
		offset1, offset2, err = sstable.searchIndex(sstable.index, int(offset1), int(offset2), key)

		if err != nil {
			return nil, err
		}

		offset1 += uint64(sstable.dataOffset) // if it is single file starting index offste is okay
		offset2 = getEndingOffset(len(content) == 1, sstable.data, sstable.dataOffset, sstable.indexOffset, int64(offset2))

		record, err := sstable.searchData(len(content) == 1, int(offset1), int(offset2), key)

		if err != nil {
			return nil, err
		}

		return record, nil
	}

	return nil, nil
}

// deletes sstable folder, returns error if it occured during deletion
// used for compactions
func (sstable *SSTable) Delete() error {

	sstable.data.Close()
	sstable.summary.Close()
	sstable.index.Close()
	dirContent, err := utils.GetDirContent(PATH) // dirContent - names of all sstables dirs
	if err != nil {
		return err
	}

	i := 0
	for i < len(dirContent) {
		if dirContent[i] == sstable.Name {
			break
		}
		i++
	}

	// remove content of sstable
	sstableFolder, err := utils.GetDirContent(fmt.Sprintf("%s/%s", PATH, sstable.Name)) // dirContent - names of all sstables dirs
	for j := 0; j < len(sstableFolder); j++ {
		err = os.Remove(fmt.Sprintf("%s/%s/%s", PATH, dirContent[i], sstableFolder[j]))
		if err != nil {
			return err
		}
	}
	// delete sstable folder
	toDelete := fmt.Sprintf("%s/%s", PATH, dirContent[i])
	err = os.Remove(toDelete)
	if err != nil {
		return err
	}
	// rename all folders after deleted one
	for j := i; j < len(dirContent)-1; j++ {
		new_name := fmt.Sprintf("%s/%s", PATH, dirContent[j])
		old_name := fmt.Sprintf("%s/%s", PATH, dirContent[j+1])
		err = os.Rename(old_name, new_name)
		if err != nil {
			return err
		}
	}
	return nil
}
