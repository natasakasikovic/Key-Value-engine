package lsmtree

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

type LSMTree struct {
	sstableArrays  [][]*sstable.SSTable //Array of arrays of SSTable pointers
	maxDepth       uint32
	compactionType string
	config         *config.Config
}

func NewLSMTree(conf *config.Config) LSMTree {
	//TODO
	//Update when config gets updated with lsm stuff
	var tree LSMTree = LSMTree{}
	tree.config = conf
	tree.sstableArrays = make([][]*sstable.SSTable, conf.LSMTreeMaxDepth)
	for i := 0; i < int(conf.LSMTreeMaxDepth); i++ {
		tree.sstableArrays[i] = make([]*sstable.SSTable, 0)
	}
	tree.maxDepth = conf.LSMTreeMaxDepth
	tree.compactionType = ""
	return tree
}

func LoadLSMTreeFromFile(conf *config.Config, lsmFilename string) *LSMTree {
	var lsm LSMTree = NewLSMTree(conf)

	jsonData, err := os.ReadFile(lsmFilename)

	if err != nil {
		return nil
	}

	var sstableNames [][]string
	err = json.Unmarshal(jsonData, &sstableNames)
	if err != nil {
		return nil
	}

	for i := 0; i < int(lsm.maxDepth); i++ {
		for j := 0; j < len(sstableNames[i]); j++ {
			var dirName = sstableNames[i][j]
			var path = fmt.Sprintf("%s/%s", sstable.PATH, dirName)
			content, err := utils.GetDirContent(path) // get content of sstable, so we can check if sstable is a single file or in seperate files

			if err != nil {
				return nil
			}

			var table *sstable.SSTable

			if len(content) == 1 {
				table, err = sstable.LoadSStableSingle(path)
			} else {
				table, err = sstable.LoadSSTableSeparate(path)
			}

			if err != nil {
				return nil
			}

			lsm.sstableArrays[i] = append(lsm.sstableArrays[i], table)
		}
	}

	return &lsm
}

func (tree *LSMTree) SaveToFile(lsmFilename string) error {
	var filenames [][]string = make([][]string, tree.maxDepth)
	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			//TODO: UPDATE WHEN NAME BECOMES PUBLIC!!!
			//filenames[i] = append(filenames[i], tree.sstableArrays[i][j].Name)
		}
	}

	jsonData, err := json.Marshal(filenames)

	if err != nil {
		return err
	}

	f, err := os.Create(lsmFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(jsonData)

	if err != nil {
		return err
	}
	return nil
}

// Merges the passed sstables and splits them into new sstables
// Each new sstable contains at most maxSSTableElem elements
// Merges into a single sstable if maxSSTableElem is 0
func mergeSSTables(sstableArray []*sstable.SSTable) *sstable.SSTable {
	var sstableCount int = len(sstableArray)
	var indexes []int = make([]int, sstableCount)

	var records []model.Record = make([]model.Record, sstableCount)

	const EMPTY_KEY string = ""

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
		//Index of the most recent record with the key
		var latestIndex int = -1
		//Timestamp of the most recent record with the key
		var latestTS uint64 = 0

		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key && (records[i].Timestamp > latestTS || latestIndex == -1) {
				latestIndex = i
				latestTS = records[i].Timestamp
			}
		}

		//Move all duplicates that arent the latest record with the key forward
		//TODO: Just read the next record in the file
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

	var newRecords []*model.Record = make([]*model.Record, 0)

	for {
		var minKey string = getMinKey()
		if minKey == EMPTY_KEY {
			break
		}

		for getKeyCount(minKey) > 0 {
			moveDuplicatesForward(minKey)
		}

		var minIndex int = getIndexOfIteratorWithKey(minKey)
		if records[minIndex].Tombstone == 0 {
			var newRecord *model.Record = &model.Record{}
			(*newRecord) = records[minIndex]
			newRecords = append(newRecords, newRecord)
		}
		indexes[minIndex]++
	}
	//TODO
	//Complete when config gets updated
	//var newSSTable *sstable.SSTable = sstable.CreateSStable()
	//return newSSTable
	return nil
}

func (tree *LSMTree) leveledCompaction(levelIndex uint32) {
	//The first table on the passed level will be merged with the appropriate tables of the next level
	var upperTable *sstable.SSTable = tree.sstableArrays[levelIndex][0]
	//TODO: UPDATE WHEN KEY GETTERS ARE ADDED
	var minKey string = ""
	var maxKey string = ""

	//Index of the first table from the lower level that needs to be merged
	//Index of the last table from the lower level that needs to be merged
	var leftIndex int = -1
	var rightIndex int = -1

	//Find first table that needs to be merged
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		//TODO UPDATE WHEN SSTABLE KEYS BECOME PUBLIC!!!
		var tableMaxKey string = ""
		if minKey <= tableMaxKey {
			leftIndex = i
			break
		}
	}

	//Find last table that needs to be merged
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		//TODO UPDATE WHEN KEYS BECOME PUBLIC!!!
		var tableMinKey string = ""
		if maxKey >= tableMinKey {
			rightIndex = i
		}
	}

	if leftIndex == -1 || rightIndex == -1 || (rightIndex < leftIndex) {
		//If there is no overlap, just move the upper sstable to the lower level

		//Find the index of the first sstable with keys larger than the upper sstable
		var firstLargerIndex int = -1
		for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
			//TODO UPDATE WHEN KEYS BECOME PUBLIC!!!
			var tableMinKey string = ""
			if maxKey < tableMinKey {
				firstLargerIndex = i
				break
			}
		}

		if firstLargerIndex == -1 {
			//If all sstables have smaller keys than the upper table, append the upper table
			tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], upperTable)
		} else {
			//Otherwise, move the larger sstables to the right, and insert the upper table into the level
			//Expand the array
			tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], nil)
			//Shift elements to the right
			copy(tree.sstableArrays[levelIndex+1][firstLargerIndex+1:], tree.sstableArrays[levelIndex+1][firstLargerIndex:])
			tree.sstableArrays[levelIndex+1][firstLargerIndex] = upperTable
		}
	} else {
		//Add the first table from the passed level to be merged
		var toMerge []*sstable.SSTable = make([]*sstable.SSTable, 0)
		toMerge = append(toMerge, upperTable)
		//Add the tables from the lower level to be merged
		for i := leftIndex; i <= rightIndex; i++ {
			toMerge = append(toMerge, tree.sstableArrays[levelIndex+1][i])
		}

		var merged *sstable.SSTable = mergeSSTables(toMerge)

		//Delete sstables that were merged
		upperTable.Delete()
		for i := leftIndex; i <= rightIndex; i++ {
			tree.sstableArrays[levelIndex+1][i].Delete()
		}

		//Insert the merged sstable into the level and remove the deleted sstables from the level
		tree.sstableArrays[levelIndex+1][leftIndex] = merged
		copy(tree.sstableArrays[levelIndex+1][leftIndex+1:], tree.sstableArrays[levelIndex+1][rightIndex+1:])

		//How many sstables were removed from the lower level
		var tablesLost int = rightIndex - leftIndex
		//Change the extra tables to nil for the garbage collector
		var lowerLevelLen int = len(tree.sstableArrays[levelIndex+1])
		for i := 0; i < tablesLost; i++ {
			tree.sstableArrays[levelIndex+1][lowerLevelLen-1-i] = nil
		}
		tree.sstableArrays[levelIndex+1] = tree.sstableArrays[levelIndex+1][:lowerLevelLen-tablesLost]
	}

	//Remove the upper sstable from the upper level
	copy(tree.sstableArrays[levelIndex][0:], tree.sstableArrays[levelIndex][1:])
	levelLen := len(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex][levelLen-1] = nil
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:levelLen-1]
}

func (tree *LSMTree) sizeTieredCompaction(levelIndex uint32) {
	//Set all sstables on the passed level to be compacted
	var toMerge []*sstable.SSTable = make([]*sstable.SSTable, 0)
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		toMerge = append(toMerge, tree.sstableArrays[levelIndex][i])
	}

	//Merge all sstables into a single new sstable
	var merged *sstable.SSTable = mergeSSTables(toMerge)
	//Delete old sstables
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		tree.sstableArrays[levelIndex][i].Delete()
	}
	//Empty out the array for the compacted level
	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]

	tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], merged)
}

func (tree *LSMTree) compact(levelIndex uint32) {
	if tree.compactionType == "LEVELED" {
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
	if tree.compactionType == "leveled" {
		return uintPow(5, levelIndex+1)
	}
	//TODO: DUMB MAGIC NUMBER
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

func (tree *LSMTree) AddSSTable(sstable *sstable.SSTable) {
	tree.sstableArrays[0] = append(tree.sstableArrays[0], sstable)
	tree.checkLevel(0)
}
