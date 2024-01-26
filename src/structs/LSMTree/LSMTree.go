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
	sstableArrays   [][]*SSTableInterface //Array of arrays of SSTable pointers
	maxDepth        uint32
	compressionOn   bool
	compressionType string
}

func NewLSMTree(conf config.Config) LSMTree {
	var tree LSMTree = LSMTree{}
	tree.sstableArrays = make([][]*SSTableInterface, conf.LSMTreeMaxDepth)
	for i := 0; i < int(conf.LSMTreeMaxDepth); i++ {
		tree.sstableArrays[i] = make([]*SSTableInterface, 0)
	}
	tree.maxDepth = conf.LSMTreeMaxDepth
	tree.compressionOn = conf.CompressionOn
	tree.compressionType = ""
	return tree
}

// Merges the passed sstables and splits them into new sstables
// Each new sstable contains at most maxSSTableElem elements
// Merges into a single sstable if maxSSTableElem is 0
func mergeSSTables(maxSSTableElem uint32, sstableArray []*SSTableInterface) []*SSTableInterface {
	var sstableCount int = len(sstableArray)
	var indexes []int = make([]int, sstableCount)

	var records []tempRecord = make([]tempRecord, sstableCount)

	const EMPTY_KEY string = ""

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
		var minimum string = EMPTY_KEY

		for i := 0; i < sstableCount; i++ {
			if keys[i] != EMPTY_KEY && (keys[i] < minimum || minimum == EMPTY_KEY) {
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
	var elementCounter uint32 = 0 //The number of records in the new records array
	var newSSTables []*SSTableInterface = make([]*SSTableInterface, 0)

	for {
		updateRecordIterators()
		var minKey string = getMinKey()
		if minKey == EMPTY_KEY {
			break
		}

		for getKeyCount(minKey) > 0 {
			moveDuplicatesForward(minKey)
			updateRecordIterators()
		}

		var minIndex int = getIndexOfIteratorWithKey(minKey)
		if records[minIndex].Tombstone == 0 {
			newRecords = append(newRecords, records[minIndex])
			elementCounter++
		}
		indexes[minIndex]++

		if elementCounter >= maxSSTableElem && maxSSTableElem > 0 {
			elementCounter = 0
			newSSTables = append(newSSTables, newSSTable(newRecords))
			newRecords = make([]tempRecord, 0)
		}
	}

	return newSSTables
}

func (tree *LSMTree) leveledCompaction(levelIndex uint32) {
	//All tables on the passed level need to be compacted with the tables on the next level
	var toMerge []*SSTableInterface = make([]*SSTableInterface, 0)
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		toMerge = append(toMerge, tree.sstableArrays[levelIndex][i])
	}
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		toMerge = append(toMerge, tree.sstableArrays[levelIndex+1][i])
	}

	//Merge them into new SSTables
	//TODO: MAGIC NUMBER
	const sstableElements uint32 = 32
	var merged []*SSTableInterface = mergeSSTables(sstableElements, toMerge)

	//Delete old sstables
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		tree.sstableArrays[levelIndex][i].Delete()
	}
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		tree.sstableArrays[levelIndex+1][i].Delete()
	}

	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]
	tree.sstableArrays[levelIndex+1] = merged
}

func (tree *LSMTree) sizeTieredCompaction(levelIndex uint32) {
	//Set all sstables on the passed level to be compacted
	var toMerge []*SSTableInterface = make([]*SSTableInterface, 0)
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		toMerge = append(toMerge, tree.sstableArrays[levelIndex][i])
	}

	//Merge all sstables into a single new sstable
	var merged []*SSTableInterface = mergeSSTables(0, toMerge)
	//Delete old sstables
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		tree.sstableArrays[levelIndex][i].Delete()
	}
	//Empty out the array for the compacted level
	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]

	tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], merged[0])
}

func (tree *LSMTree) compact(levelIndex uint32) {
	if tree.compressionType == "LEVELED" {
		tree.leveledCompaction(levelIndex)
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
	//TODO: MAGIC NUMBER
	return 5
}

func (tree *LSMTree) checkLevel(levelIndex uint32) {
	//No compaction on last level
	if levelIndex+1 >= tree.maxDepth {
		return
	}

	if len(tree.sstableArrays[levelIndex]) >= int(tree.getCapacityOfLevel(levelIndex)) {
		tree.compact(levelIndex)
		tree.checkLevel(levelIndex + 1)
	}
}

func (tree *LSMTree) AddSSTable(sstable *SSTableInterface) {
	tree.sstableArrays[0] = append(tree.sstableArrays[0], sstable)
	tree.checkLevel(0)
}
