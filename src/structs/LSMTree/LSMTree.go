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
	compressionMap       map[string]uint64
}

func isSSTableInSingleFile(tableName string) (bool, error) {
	//Check if the sstable folder exists
	_, err := os.ReadDir(fmt.Sprintf("%s/%s", sstable.PATH, tableName))

	if os.IsNotExist(err) {
		return false, err
	}

	//sstableFolder - Array of the names of all files in the sstable folder
	sstableFolder, err := utils.GetDirContent(fmt.Sprintf("%s/%s", sstable.PATH, tableName))
	if err != nil {
		return false, err
	}

	if len(sstableFolder) > 1 {
		return false, nil
	} else {
		return true, nil
	}
}

// Loads all sstables from the disk into an array
// Returns an error on fail
// The sstables are closed on load
func loadAllSStables() ([]*sstable.SSTable, error) {
	//Array of the names of all sstables
	sstableNames, err := utils.GetDirContent(sstable.PATH)
	if err != nil {
		return make([]*sstable.SSTable, 0), err
	}

	var tables []*sstable.SSTable = make([]*sstable.SSTable, 0)
	for i := 0; i < len(sstableNames); i++ {
		var table *sstable.SSTable
		isSingleFile, err := isSSTableInSingleFile(sstableNames[i])
		if err != nil {
			return make([]*sstable.SSTable, 0), err
		}

		var path string = fmt.Sprintf("%s/%s", sstable.PATH, sstableNames[i])

		if isSingleFile {
			table, err = sstable.LoadSStableSingle(path)
		} else {
			table, err = sstable.LoadSSTableSeparate(path)
		}
		if err != nil {
			return make([]*sstable.SSTable, 0), err
		}

		table.Name = sstableNames[i]
		table.Data.Close()
		table.Index.Close()
		table.Summary.Close()
		tables = append(tables, table)
	}

	return tables, nil
}

func makeEmptyLSMTree(maxDepth uint32, compactionType string, firstLevelSize uint32, growthFactor uint32,
	sstableIndexDegree uint32, sstableSummaryDegree uint32, sstableInSameFile bool, sstableCompressionOn bool,
	compressionMap map[string]uint64) *LSMTree {
	var tree *LSMTree = &LSMTree{
		maxDepth:             maxDepth,
		compactionType:       compactionType,
		firstLevelSize:       firstLevelSize,
		growthFactor:         growthFactor,
		sstableIndexDegree:   sstableIndexDegree,
		sstableSummaryDegree: sstableSummaryDegree,
		sstableInSameFile:    sstableInSameFile,
		sstableCompressionOn: sstableCompressionOn,
		compressionMap:       compressionMap,
	}

	tree.sstableArrays = make([][]*sstable.SSTable, maxDepth)
	for i := 0; i < int(maxDepth); i++ {
		tree.sstableArrays[i] = make([]*sstable.SSTable, 0)
	}

	return tree
}

// Creates a new LSM Tree and loads existing sstables into it
func NewLSMTree(maxDepth uint32, compactionType string, firstLevelSize uint32, growthFactor uint32,
	sstableIndexDegree uint32, sstableSummaryDegree uint32, sstableInSameFile bool, sstableCompressionOn bool,
	compressionMap map[string]uint64) (*LSMTree, error) {
	var tree *LSMTree = makeEmptyLSMTree(
		maxDepth,
		compactionType,
		firstLevelSize,
		growthFactor,
		sstableIndexDegree,
		sstableSummaryDegree,
		sstableInSameFile,
		sstableCompressionOn,
		compressionMap)

	tables, err := loadAllSStables()
	if err != nil {
		return nil, err
	}
	tree.sstableArrays[0] = tables
	tree.checkLevel(0)

	tree.SaveToFile()
	return tree, nil
}

const LSM_PATH string = "../data/LSMTree.json"

// Returns a pointer to the lsm tree if loaded successfuly
// Otherwise, returns nil
func LoadLSMTreeFromFile(maxDepth uint32, compactionType string, firstLevelSize uint32, growthFactor uint32,
	sstableIndexDegree uint32, sstableSummaryDegree uint32, sstableInSameFile bool, sstableCompressionOn bool,
	compressionMap map[string]uint64) (*LSMTree, error) {
	lsm := makeEmptyLSMTree(maxDepth,
		compactionType,
		firstLevelSize,
		growthFactor,
		sstableIndexDegree,
		sstableSummaryDegree,
		sstableInSameFile,
		sstableCompressionOn,
		compressionMap)

	jsonData, err := os.ReadFile(LSM_PATH)

	if err != nil {
		return nil, err
	}

	var sstableNames [][]string
	err = json.Unmarshal(jsonData, &sstableNames)
	if err != nil {
		return nil, err
	}

	//Get the sstable folder
	//We will use it to check how many sstables exist
	sstableFolder, err := utils.GetDirContent(sstable.PATH)
	if err != nil {
		return nil, err
	}

	//Count how many sstables have been loaded
	sstableCounter := 0
	for i := 0; i < int(lsm.maxDepth); i++ {
		for j := 0; j < len(sstableNames[i]); j++ {
			var dirName = sstableNames[i][j]
			var path = fmt.Sprintf("%s/%s", sstable.PATH, dirName)

			//Check if the sstable folder exists
			_, err := os.ReadDir(fmt.Sprintf("%s/%s", sstable.PATH, dirName))

			if os.IsNotExist(err) {
				return nil, err
			}

			content, err := utils.GetDirContent(path) // get content of sstable, so we can check if sstable is a single file or in seperate files

			if err != nil {
				return nil, err
			}

			var table *sstable.SSTable

			if len(content) == 1 {
				table, err = sstable.LoadSStableSingle(path)
			} else {
				table, err = sstable.LoadSSTableSeparate(path)
			}

			if err != nil {
				return nil, err
			}

			table.Name = dirName
			closeSSTable(table)
			lsm.sstableArrays[i] = append(lsm.sstableArrays[i], table)
			sstableCounter += 1
		}
	}

	//If we didn't load all the sstables, FAIL
	if len(sstableFolder) != sstableCounter {
		return nil, err
	}

	//Check if all levels are sorted if the compaction type is leveled
	if lsm.compactionType == "leveled" {
		for i := 0; i < int(lsm.maxDepth); i++ {
			for j := 0; j < len(lsm.sstableArrays[i])-1; j++ {
				//If the left table contains a key larger than in the right table, the lsm is not leveled
				if lsm.sstableArrays[i][j].MaxKey >= lsm.sstableArrays[i][j+1].MinKey {
					return nil, err
				}
			}
		}
	}

	return lsm, nil
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
			singleFile, err = isSSTableInSingleFile(table.Name)
			if err != nil {
				return err
			}

			if !singleFile {
				tree.sstableArrays[i][j], err = sstable.LoadSSTableSeparate(path)
			} else {
				tree.sstableArrays[i][j], err = sstable.LoadSStableSingle(path)
			}
			tree.sstableArrays[i][j].Name = name
			closeSSTable(tree.sstableArrays[i][j])

			if err != nil {
				return err
			}
		}
	}
	return err
}

// Merges the passed sstables into a new sstable
func mergeSSTables(sstableArray []*sstable.SSTable, sstableIndexDegree uint32, sstableSummaryDegree uint32,
	sstableInSameFile bool, sstableCompressionOn bool, compressionMap map[string]uint64) (*sstable.SSTable, error) {
	var sstableCount int = len(sstableArray)
	var fileIterators []iterators.Iterator = make([]iterators.Iterator, sstableCount)

	//Initialize file iterators
	for i := 0; i < sstableCount; i++ {
		var err error
		fileIterators[i], err = iterators.NewSSTableIterator(sstableArray[i], sstableCompressionOn, compressionMap)
		if err != nil {
			return nil, err
		}
	}

	iterGroup, err := iterators.NewIteratorGroup(fileIterators)
	defer iterGroup.Stop()
	if err != nil {
		return nil, err
	}

	//Array containing records going into the new sstable
	var newRecords []*model.Record = make([]*model.Record, 0)

	for {
		record_p, err := iterGroup.Next()
		if err != nil {
			return nil, err
		}

		//If all records have been read
		if record_p == nil {
			break
		}

		//If the record isn't nil, add it to the sstable
		newRecords = append(newRecords, record_p)
	}

	//The new sstable
	newSSTable, err := sstable.CreateSStable(newRecords, sstableInSameFile, sstableCompressionOn, int(sstableIndexDegree), int(sstableSummaryDegree), compressionMap)

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

func (tree *LSMTree) closeAllTables() {
	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			closeSSTable(tree.sstableArrays[i][j])
		}
	}
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
			newName, exists := nameMapping[tree.sstableArrays[i][j].Name]
			if exists {
				tree.sstableArrays[i][j].Name = newName
			}
		}
	}

	tree.closeAllTables()
	err = table.Delete()
	if err != nil {
		return err
	}
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
			tree.sstableInSameFile, tree.sstableCompressionOn, tree.compressionMap)

		//Insert the merged sstable into the level
		tree.closeAllTables()
		tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], nil)
		copy(tree.sstableArrays[levelIndex+1][leftIndex+1:], tree.sstableArrays[levelIndex+1][leftIndex:])
		tree.sstableArrays[levelIndex+1][leftIndex] = merged
		leftIndex += 1
		rightIndex += 1

		if err != nil {
			return err
		}

		//Delete sstables that were merged
		err = tree.deleteTable(upperTable)
		if err != nil {
			return err
		}

		for i := leftIndex; i <= rightIndex; i++ {
			err = tree.deleteTable(tree.sstableArrays[levelIndex+1][i])
			if err != nil {
				return err
			}
		}

		// Remove the deleted sstables from the level
		copy(tree.sstableArrays[levelIndex+1][leftIndex:], tree.sstableArrays[levelIndex+1][rightIndex+1:])

		//How many sstables were removed from the lower level
		var tablesLost int = rightIndex - leftIndex + 1
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
	tree.reopenSSTables()

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
		tree.sstableInSameFile, tree.sstableCompressionOn, tree.compressionMap)

	if err != nil {
		return err
	}
	tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], merged)

	//Delete old sstables
	for i := 0; i < len(toMerge); i++ {
		err = tree.deleteTable(toMerge[i])
		if err != nil {
			return err
		}
	}
	//Empty out the array for the compacted level
	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]

	tree.reopenSSTables()
	return nil
}

func (tree *LSMTree) compact(levelIndex uint32) error {
	var err error
	if tree.compactionType == "leveled" {
		err = tree.leveledCompaction(levelIndex)
	} else {
		err = tree.sizeTieredCompaction(levelIndex)
	}
	tree.closeAllTables()
	return err
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

func (tree *LSMTree) checkLevel(levelIndex uint32) error {
	//No compaction on last level
	if levelIndex+1 >= tree.maxDepth {
		return nil
	}

	if len(tree.sstableArrays[levelIndex]) > int(tree.getCapacityOfLevel(levelIndex)) {
		err := tree.compact(levelIndex)
		if err != nil {
			return err
		}

		err = tree.checkLevel(levelIndex + 1)
		if err != nil {
			return err
		}

		return tree.checkLevel(0)
	}
	return nil
}

func (tree *LSMTree) AddSSTable(sstable *sstable.SSTable) error {
	closeSSTable(sstable)
	tree.sstableArrays[0] = append(tree.sstableArrays[0], sstable)
	tree.SaveToFile()
	return tree.checkLevel(0)
}
