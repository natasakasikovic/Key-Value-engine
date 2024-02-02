package lsmtree

/*
STA MI TREBA:
	-OBAVEZNO:
	u konfig:
		LSMFirstLevelSize (ceo broj)
		LSMGrowthFactor (ceo broj)
		LSMCompactionType (string "leveled" ili "sizetiered")
	oni atributi za sstable da su javni
	-Bilo bi jako lepo:
		LoadSSTable metodica
*/

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

// deserializes a record by reading field by field from the file
// returns Record, number of bytes read, error
func Deserialize(file *os.File) (*model.Record, uint64, error) {
	var err error
	var record model.Record = model.Record{}

	var keySizeBuffer []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(file, keySizeBuffer, 8)
	if err != nil {
		return nil, 0, err
	}
	record.KeySize = binary.BigEndian.Uint64(keySizeBuffer)

	var bufferKey []byte = make([]byte, record.KeySize)
	_, err = io.ReadAtLeast(file, bufferKey, int(record.KeySize))
	if err != nil {
		return nil, 0, err
	}
	record.Key = string(bufferKey)

	var crcBuffer []byte = make([]byte, 4)
	_, err = io.ReadAtLeast(file, crcBuffer, 4)
	if err != nil {
		return nil, 0, err
	}
	crc := binary.BigEndian.Uint32(crcBuffer)
	record.Crc = crc

	var timestampBuffer []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(file, timestampBuffer, 8)
	if err != nil {
		return nil, 0, err
	}
	timestamp := binary.BigEndian.Uint64(timestampBuffer)
	record.Timestamp = timestamp

	var tombstoneBuffer []byte = make([]byte, 1)
	_, err = io.ReadAtLeast(file, tombstoneBuffer, 1)
	if err != nil {
		return nil, 0, err
	}
	tombstone := tombstoneBuffer[0]
	record.Tombstone = byte(tombstone)

	read := 8 + record.KeySize + 1 + 8 + 4
	if tombstone != 1 {
		var valueSizeBuffer []byte = make([]byte, 8)
		_, err = io.ReadAtLeast(file, valueSizeBuffer, 8)
		if err != nil {
			return nil, 0, err
		}
		record.ValueSize = binary.BigEndian.Uint64(valueSizeBuffer)

		var valueBuffer []byte = make([]byte, record.ValueSize)
		_, err = io.ReadAtLeast(file, valueBuffer, int(record.ValueSize))

		if err != nil {
			return nil, 0, err
		}
		record.Value = valueBuffer
		read += (8 + record.ValueSize)
	} else {
		record.ValueSize = 0
		record.Value = []byte{}
	}

	return &record, read, nil
}

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

const LSM_PATH string = "../data/LSMTree.json"

func LoadLSMTreeFromFile(conf *config.Config) *LSMTree {
	var lsm LSMTree = NewLSMTree(conf)

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

// Merges the passed sstables and splits them into new sstables
// Each new sstable contains at most maxSSTableElem elements
// Merges into a single sstable if maxSSTableElem is 0
func mergeSSTables(sstableArray []*sstable.SSTable, conf config.Config) (*sstable.SSTable, error) {
	var sstableCount int = len(sstableArray)

	var records []*model.Record = make([]*model.Record, sstableCount)
	var fileOffsets []int64 = make([]int64, sstableCount)
	var fileOffsetLimits []int64 = make([]int64, sstableCount)

	const EMPTY_KEY string = ""

	//Initialize file iterators
	for i := 0; i < sstableCount; i++ {
		fileOffsets[i] = sstableArray[i].DataOffset

		var err error
		if sstableArray[i].DataOffset == 0 {
			fileOffsetLimits[i], err = utils.GetFileLength(sstableArray[i].Data)
			if err != nil {
				return nil, err
			}
		} else {
			fileOffsetLimits[i] = sstableArray[i].IndexOffset
		}

		sstableArray[i].Data.Seek(fileOffsets[i], io.SeekStart)

		records[i], _, err = Deserialize(sstableArray[i].Data)
		if err != nil {
			return nil, err
		}

		fileOffsets[i], err = sstableArray[i].Data.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}

	//Get the smallest key of the keys at the current iterators of the sstables
	var getMinKey func() string = func() string {
		var minimum string = EMPTY_KEY

		for i := 0; i < sstableCount; i++ {
			if records[i].Key != EMPTY_KEY && (records[i].Key < minimum || minimum == EMPTY_KEY) {
				minimum = records[i].Key
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

	var readNextRecord func(int) error = func(index int) error {
		if fileOffsets[index] < fileOffsetLimits[index] {
			var err error
			records[index], _, err = Deserialize(sstableArray[index].Data)
			if err != nil {
				return err
			}

			fileOffsets[index], err = sstableArray[index].Data.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
		} else {
			//If EOF, just give the record an empty key
			records[index] = &model.Record{Key: EMPTY_KEY}
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
			if records[i].Key == key && (records[i].Timestamp > latestTS || latestIndex == -1) {
				latestIndex = i
				latestTS = records[i].Timestamp
			}
		}

		//Move all duplicates that arent the latest record with the key forward
		for i := 0; i < sstableCount; i++ {
			if records[i].Key == key && i != latestIndex {
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
	//TODO
	//Complete when config gets updated
	newSSTable, err := sstable.CreateSStable(newRecords, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexDegree), int(conf.SummaryDegree))

	if err != nil {
		return nil, err
	}

	return newSSTable, nil
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
		nameMapping[dirContent[j]] = dirContent[j+1]
	}

	for i := 0; i < int(tree.maxDepth); i++ {
		for j := 0; j < len(tree.sstableArrays[i]); j++ {
			newName, exists := nameMapping[tree.sstableArrays[i][j].Name]
			if exists {
				tree.sstableArrays[i][j].Name = newName
			}
		}
	}

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
	var leftIndex int = -1
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

	if leftIndex == -1 || rightIndex == -1 || (rightIndex < leftIndex) {
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

		merged, err := mergeSSTables(toMerge, *tree.config)

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
	merged, err := mergeSSTables(toMerge, *tree.config)

	if err != nil {
		return err
	}

	//Delete old sstables
	for i := 0; i < len(tree.sstableArrays[levelIndex]); i++ {
		tree.deleteTable(tree.sstableArrays[levelIndex][i])
	}
	//Empty out the array for the compacted level
	clear(tree.sstableArrays[levelIndex])
	tree.sstableArrays[levelIndex] = tree.sstableArrays[levelIndex][:0]

	tree.sstableArrays[levelIndex+1] = append(tree.sstableArrays[levelIndex+1], merged)
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
	var result uint32 = x
	for i = 1; i < y; i++ {
		result *= x
	}
	return result
}

func (tree *LSMTree) getCapacityOfLevel(levelIndex uint32) uint32 {
	//TODO: Update with config values
	if tree.compactionType == "leveled" {
		return uintPow(5, levelIndex)
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
