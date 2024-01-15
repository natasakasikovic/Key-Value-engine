package lsmtree

import (
	"github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
)

// Temporary memtable record struct containing a key
type tempRecord struct {
	model.MemtableRecord
	Key string
}

// Temporary SSTable interface
// TODO: change once SSTable is finished
type SSTableInterface struct {
}

func (s *SSTableInterface) ReadAtIndex(i int) (tempRecord, bool) {
	return tempRecord{}, false
}

func (s *SSTableInterface) Delete() {

}

func newSSTable(records []tempRecord) *SSTableInterface {
	return nil
}

//End of temporary SSTable interface

type LSMTree struct {
	sstableTree     [][]*SSTableInterface //Array of arrays of SSTable pointers
	maxDepth        uint32
	compressionOn   bool
	compressionType string
}

func NewLSMTree(conf config.Config) LSMTree {
	var tree LSMTree = LSMTree{}
	tree.sstableTree = make([][]*SSTableInterface, conf.LSMTreeMaxDepth)
	for i := 0; i < int(conf.LSMTreeMaxDepth); i++ {
		tree.sstableTree[i] = make([]*SSTableInterface, 0)
	}
	tree.maxDepth = conf.LSMTreeMaxDepth
	tree.compressionOn = conf.CompressionOn
	tree.compressionType = ""
	return tree
}

func mergeSSTables(sstableArray []*SSTableInterface) *SSTableInterface {
	var sstableCount int = len(sstableArray)
	var indexes []int = make([]int, sstableCount)

	var records []tempRecord = make([]tempRecord, sstableCount)

	var updateRecordIterators func() = func() {
		for i := 0; i < sstableCount; i++ {
			records[i], _ = sstableArray[i].ReadAtIndex(indexes[i])
		}
	}

	//Get the keys of records at the current indexes of the sstables
	var getKeys func() []string = func() []string {
		var keys []string = make([]string, sstableCount)
		for i := 0; i < sstableCount; i++ {
			keys[i] = records[i].Key
		}
		return keys
	}

	//Get the smallest key of the keys at the current iterators of the sstables
	var getMinKey func() string = func() string {
		var keys []string = getKeys()
		var minimum string = ""

		for i := 0; i < sstableCount; i++ {
			if keys[i] != "" && (keys[i] < minimum || minimum == "") {
				minimum = keys[i]
			}
		}

		return minimum
	}

	//Returns the number of records at the current iterators with the passed key
	var getKeyCount func(string) uint = func(key string) uint {
		var count uint = 0
		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key {
				count++
			}
		}
		return count
	}

	//Moves the indexes for the sstables containing duplicates of the minimum key forward by one
	var moveDuplicatesForward func(string) = func(key string) {
		//Index of the latest record with the key
		var latestIndex int = -1
		//Timestamp of the latest record with the key
		var latestTS uint64 = 0

		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key && (records[i].Timestamp > latestTS || latestIndex == -1) {
				latestIndex = i
				latestTS = records[i].Timestamp
			}
		}

		//Move all duplicates that arent the latest record with the key forward
		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key && i != latestIndex {
				indexes[i]++
			}
		}
	}

	//Returns the index of the sstable iterator currently containing the passed key
	//Assumes no other sstable iterator contains the key currently
	var getIndexOfIteratorWithKey func(string) int = func(key string) int {
		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key {
				return i
			}
		}
		return -1
	}

	var newRecords []tempRecord = make([]tempRecord, 0)

	for {
		updateRecordIterators()
		var minKey string = getMinKey()
		if minKey == "" {
			break
		}

		for getKeyCount(minKey) > 0 {
			moveDuplicatesForward(minKey)
			updateRecordIterators()
		}

		var minIndex int = getIndexOfIteratorWithKey(minKey)
		if records[minIndex].Tombstone == 0 {
			newRecords = append(newRecords, records[minIndex])
		}
		indexes[minIndex]++
	}

	return newSSTable(newRecords)
}

func (tree *LSMTree) sizeTieredCompaction(levelIndex uint32) {
	var toMerge []*SSTableInterface = make([]*SSTableInterface, 0)
	for i := 0; i < len(tree.sstableTree[levelIndex]); i++ {
		toMerge = append(toMerge, tree.sstableTree[levelIndex][i])
	}

	var merged *SSTableInterface = mergeSSTables(toMerge)
	//Delete old sstables
	for i := 0; i < len(tree.sstableTree[levelIndex]); i++ {
		tree.sstableTree[levelIndex][i].Delete()
	}
	//Make a new empty array
	clear(tree.sstableTree[levelIndex])
	tree.sstableTree[levelIndex] = tree.sstableTree[levelIndex][:0]

	tree.sstableTree[levelIndex] = append(tree.sstableTree[levelIndex], merged)
}

func (tree *LSMTree) compact(levelIndex uint32) {
	if tree.compressionType == "LEVELED" {

	} else {
		tree.sizeTieredCompaction(levelIndex)
	}
}

func uintPow(x, y uint32) uint32 {
	var i uint32
	var result uint32 = x
	for i = 1; i < y; i++ {
		result *= x
	}
	return result
}

func (tree *LSMTree) getCapacityOfLevel(levelIndex uint32) uint32 {
	//TODO: Update with config values
	if tree.compressionType == "leveled" {
		return uintPow(5, levelIndex+1)
	}
	return 5
}

func (tree *LSMTree) checkLevel(levelIndex uint32) {
	if levelIndex+1 >= tree.maxDepth {
		return
	}

	if len(tree.sstableTree[levelIndex]) >= int(tree.getCapacityOfLevel(levelIndex)) {
		tree.compact(levelIndex)
		tree.checkLevel(levelIndex + 1)
	}
}

func (tree *LSMTree) AddSSTable(sstable *SSTableInterface) {
	tree.sstableTree[0] = append(tree.sstableTree[0], sstable)
	tree.checkLevel(0)
}
