package lsmtree

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/iterators"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

type LSMTree struct {
	sstableArrays  [][]*sstable.SSTable //Array of arrays of SSTable pointers
	maxDepth       uint32
	compactionType string
	firstLevelSize uint32
	growthFactor   uint32

	sstableIndexDegree   uint32
	sstableSummaryDegree uint32
	sstableInSameFile    bool
	sstableCompressionOn bool
}

func NewLSMTree(maxDepth uint32, compactionType string, firstLevelSize uint32, growthFactor uint32,
	sstableIndexDegree uint32, sstableSummaryDegree uint32, sstableInSameFile bool, sstableCompressionOn bool) *LSMTree {
	var tree *LSMTree = &LSMTree{
		maxDepth:             maxDepth,
		compactionType:       compactionType,
		firstLevelSize:       firstLevelSize,
		growthFactor:         growthFactor,
		sstableIndexDegree:   sstableIndexDegree,
		sstableSummaryDegree: sstableSummaryDegree,
		sstableInSameFile:    sstableInSameFile,
		sstableCompressionOn: sstableCompressionOn,
	}

	tree.sstableArrays = make([][]*sstable.SSTable, maxDepth)
	for i := 0; i < int(maxDepth); i++ {
		tree.sstableArrays[i] = make([]*sstable.SSTable, 0)
	}
	return tree
}

const LSM_PATH string = "../../../data/LSMTree.json"

// Returns a pointer to the lsm tree if loaded successfuly
// Otherwise, returns nil
func LoadLSMTreeFromFile(maxDepth uint32, compactionType string, firstLevelSize uint32, growthFactor uint32,
	sstableIndexDegree uint32, sstableSummaryDegree uint32, sstableInSameFile bool, sstableCompressionOn bool) *LSMTree {
	var lsm LSMTree = *NewLSMTree(maxDepth,
		compactionType,
		firstLevelSize,
		growthFactor,
		sstableIndexDegree,
		sstableSummaryDegree,
		sstableInSameFile,
		sstableCompressionOn)

	jsonData, err := os.ReadFile(LSM_PATH)

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

			table.Name = dirName
			lsm.sstableArrays[i] = append(lsm.sstableArrays[i], table)
		}
	}

	return &lsm
}

func (tree *LSMTree) SaveToFile() error {
	var filenames [][]string = make([][]string, tree.maxDepth)
	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			filenames[i] = append(filenames[i], tree.sstableArrays[i][j].Name)
		}
	}

	jsonData, err := json.Marshal(filenames)

	if err != nil {
		return err
	}

	f, err := os.Create(LSM_PATH)
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

func isSSTableInSingleFile(table *sstable.SSTable) (bool, error) {
	sstableFolder, err := utils.GetDirContent(fmt.Sprintf("%s/%s", sstable.PATH, table.Name)) // dirContent - names of all sstables dirs
	if err != nil {
		return false, err
	}

	if len(sstableFolder) > 1 {
		return false, nil
	} else {
		return true, nil
	}
}

// Updates the os.File pointers of the sstables after renaming
func (tree *LSMTree) reopenSSTables() error {
	var err error = nil
	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			var table *sstable.SSTable = tree.sstableArrays[i][j]
			var name string = table.Name
			var path string = fmt.Sprintf("%s/%s", sstable.PATH, table.Name)
			closeSSTable(table)

			var singleFile bool
			singleFile, err = isSSTableInSingleFile(table)
			if err != nil {
				return err
			}

			if !singleFile {
				tree.sstableArrays[i][j], err = sstable.LoadSSTableSeparate(path)
			} else {
				tree.sstableArrays[i][j], err = sstable.LoadSStableSingle(path)
			}
			tree.sstableArrays[i][j].Name = name

			if err != nil {
				return err
			}
		}
	}
	return err
}

// Merges the passed sstables and splits them into new sstables
func mergeSSTables(sstableArray []*sstable.SSTable, sstableIndexDegree uint32, sstableSummaryDegree uint32,
	sstableInSameFile bool, sstableCompressionOn bool) (*sstable.SSTable, error) {
	var sstableCount int = len(sstableArray)

	var records []*model.Record = make([]*model.Record, sstableCount)
	var fileIterators []*iterators.SSTableIterator = make([]*iterators.SSTableIterator, sstableCount)

	const EMPTY_KEY string = ""

	//Initialize file iterators
	for i := 0; i < sstableCount; i++ {
		var err error
		fileIterators[i], err = iterators.NewSSTableIterator(sstableArray[i])
		if err != nil {
			return nil, err
		}

		records[i], err = fileIterators[i].Next()
		if err != nil {
			return nil, err
		}
	}

	//Get the smallest key of the keys at the current iterators of the sstables
	var getMinKey func() string = func() string {
		var minimum string = EMPTY_KEY

		for i := 0; i < sstableCount; i++ {
			if records[i] != nil && (records[i].Key < minimum || minimum == EMPTY_KEY) {
				minimum = records[i].Key
			}
		}

		return minimum
	}

	//Returns the number of records at the current iterators with the passed key
	var getKeyCount func(string) uint = func(key string) uint {
		var count uint = 0
		for i := 0; i < sstableCount; i++ {
			if records[i] != nil && records[i].Key == key {
				count++
			}
		}
		return count
	}

	var readNextRecord func(int) error = func(index int) error {
		var err error
		records[index], err = fileIterators[index].Next()
		if err != nil {
			return err
		}
		return nil
	}

	//Moves the indexes for the sstables containing duplicates of the minimum key forward by one
	var moveDuplicatesForward func(string) error = func(key string) error {
		//Index of the most recent record with the key
		var latestIndex int = -1
		//Timestamp of the most recent record with the key
		var latestTS uint64 = 0

		for i := 0; i < sstableCount; i++ {
			if records[i] != nil && records[i].Key == key && (records[i].Timestamp > latestTS || latestIndex == -1) {
				latestIndex = i
				latestTS = records[i].Timestamp
			}
		}

		//Move all duplicates that arent the latest record with the key forward
		for i := 0; i < sstableCount; i++ {
			if records[i] != nil && records[i].Key == key && i != latestIndex {
				err := readNextRecord(i)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	//Returns the index of the sstable iterator currently containing the passed key
	//Assumes no other sstable iterator contains the key currently
	//Only call after calling moveDuplicatesForward
	var getIndexOfIteratorWithKey func(string) int = func(key string) int {
		for i := 0; i < sstableCount; i++ {
			if records[i] != nil && records[i].Key == key {
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

		for getKeyCount(minKey) > 1 {
			err := moveDuplicatesForward(minKey)
			if err != nil {
				return nil, err
			}
		}

		var minIndex int = getIndexOfIteratorWithKey(minKey)
		if records[minIndex].Tombstone == 0 {
			newRecords = append(newRecords, records[minIndex])
		}
		err := readNextRecord(minIndex)
		if err != nil {
			return nil, err
		}
	}
	newSSTable, err := sstable.CreateSStable(newRecords, sstableInSameFile, sstableCompressionOn, int(sstableIndexDegree), int(sstableSummaryDegree))

	if err != nil {
		return nil, err
	}

	return newSSTable, nil
}

func closeSSTable(table *sstable.SSTable) {
	table.Data.Close()
	table.Index.Close()
	table.Summary.Close()
}

// Deletes the passed sstable from the disk and updates the name attribute of other sstables
func (tree *LSMTree) deleteTable(table *sstable.SSTable) error {
	//Key - old name, value - new name
	//Will be used to update the name attribute of other sstables
	var nameMapping map[string]string = make(map[string]string)
	var deletedName = table.Name

	dirContent, err := utils.GetDirContent(sstable.PATH) // dirContent - names of all sstables dirs
	if err != nil {
		return err
	}

	i := 0
	for i < len(dirContent) {
		if dirContent[i] == deletedName {
			break
		}
		i++
	}

	for j := i; j < len(dirContent)-1; j++ {
		nameMapping[dirContent[j+1]] = dirContent[j]
	}

	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			closeSSTable(tree.sstableArrays[i][j])
			newName, exists := nameMapping[tree.sstableArrays[i][j].Name]
			if exists {
				tree.sstableArrays[i][j].Name = newName
			}
		}
	}

	table.Data.Close()
	table.Index.Close()
	table.Summary.Close()
	table.Delete()
	return nil
}

func (tree *LSMTree) leveledCompaction(levelIndex uint32) error {
	//The first table on the passed level will be merged with the appropriate tables of the next level
	var upperTable *sstable.SSTable = tree.sstableArrays[levelIndex][0]
	var minKey string = upperTable.MinKey
	var maxKey string = upperTable.MaxKey

	//Index of the first table from the lower level that needs to be merged
	//Index of the last table from the lower level that needs to be merged
	var leftIndex int = len(tree.sstableArrays[levelIndex+1])
	var rightIndex int = -1

	//Find first table that needs to be merged
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		var tableMaxKey string = tree.sstableArrays[levelIndex+1][i].MaxKey
		if minKey <= tableMaxKey {
			leftIndex = i
			break
		}
	}

	//Find last table that needs to be merged
	for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
		var tableMinKey string = tree.sstableArrays[levelIndex+1][i].MinKey
		if maxKey >= tableMinKey {
			rightIndex = i
		}
	}

	if leftIndex == len(tree.sstableArrays[levelIndex+1]) || rightIndex == -1 || (rightIndex < leftIndex) {
		//If there is no overlap, just move the upper sstable to the lower level

		//Find the index of the first sstable with keys larger than the upper sstable
		var firstLargerIndex int = -1
		for i := 0; i < len(tree.sstableArrays[levelIndex+1]); i++ {
			var tableMinKey string = tree.sstableArrays[levelIndex+1][i].MinKey
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

		merged, err := mergeSSTables(toMerge, tree.sstableIndexDegree, tree.sstableSummaryDegree,
			tree.sstableInSameFile, tree.sstableCompressionOn)

		if err != nil {
			return err
		}

		//Delete sstables that were merged
		tree.deleteTable(upperTable)
		for i := leftIndex; i <= rightIndex; i++ {
			tree.deleteTable(tree.sstableArrays[levelIndex+1][i])
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

		tree.reopenSSTables()
	}

	//Remove the upper sstable from the upper level
	copy(tree.sstableArrays[levelIndex][0:], tree.sstableArrays[levelIndex][1:])
	levelLen := len(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex][levelLen-1] = nil
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:levelLen-1]

	return nil
}

func (tree *LSMTree) sizeTieredCompaction(levelIndex uint32) error {
	//Set all sstables on the passed level to be compacted
	var toMerge []*sstable.SSTable = make([]*sstable.SSTable, 0)
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		toMerge = append(toMerge, tree.sstableArrays[levelIndex][i])
	}

	//Merge all sstables into a single new sstable
	merged, err := mergeSSTables(toMerge, tree.sstableIndexDegree, tree.sstableSummaryDegree,
		tree.sstableInSameFile, tree.sstableCompressionOn)

	if err != nil {
		return err
	}
	tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], merged)

	//Delete old sstables
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		tree.deleteTable(tree.sstableArrays[levelIndex][i])
	}
	//Empty out the array for the compacted level
	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]

	tree.reopenSSTables()
	return nil
}

func (tree *LSMTree) compact(levelIndex uint32) {
	if tree.compactionType == "leveled" {
		tree.leveledCompaction(levelIndex)
	} else {
		tree.sizeTieredCompaction(levelIndex)
	}
}

func uintPow(x, y uint32) uint32 {
	var i uint32
	var result uint32 = 1
	for i = 0; i < y; i++ {
		result *= x
	}
	return result
}

func (tree *LSMTree) getCapacityOfLevel(levelIndex uint32) uint32 {
	if tree.compactionType == "leveled" {
		if levelIndex == 0 {
			return 1
		} else {
			return tree.firstLevelSize * uintPow(tree.growthFactor, levelIndex-1)
		}
	} else {
		return tree.firstLevelSize
	}
}

func (tree *LSMTree) checkLevel(levelIndex uint32) {
	//No compaction on last level
	if levelIndex+1 >= tree.maxDepth {
		return
	}

	if len(tree.sstableArrays[levelIndex]) > int(tree.getCapacityOfLevel(levelIndex)) {
		tree.compact(levelIndex)
		tree.checkLevel(levelIndex + 1)
	}
}

func (tree *LSMTree) AddSSTable(sstable *sstable.SSTable) {
	tree.sstableArrays[0] = append(tree.sstableArrays[0], sstable)
	tree.checkLevel(0)
}
