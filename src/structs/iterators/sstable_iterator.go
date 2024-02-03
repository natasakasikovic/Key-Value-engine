package iterators

import (
	"fmt"
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

type SSTableIterator struct {
	data                *os.File
	current_offset      int64
	end_offset          int64
	isSSTableCompressed bool
}

func isSSTableInSingleFile(tableName string) (bool, error) {
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

// Returns the offset where data begins, and the offset right after the data ends
func getDataOffsets(table *sstable.SSTable) (int64, int64, error) {
	oneFile, err := isSSTableInSingleFile(table.Name)
	if err != nil {
		return 0, 0, err
	}

	if !oneFile {
		//IF THE SSTABLE IS IN SEPERATE FILES
		table.Data, err = os.Open(table.Data.Name())
		if err != nil {
			return 0, 0, err
		}

		fileLen, err := table.Data.Seek(0, io.SeekEnd)
		table.Data.Close()
		return table.DataOffset, fileLen, err
	} else {
		//IF THE WHOLE SSTABLE IS IN THE SAME FILE
		return table.DataOffset, table.IndexOffset, nil
	}
}

// Returns a pointer to a new sstable iterator.
// The iterator becomes unusable after any sstables get inserted into the LSM tree -
// - due to the possibility of the data file being renamed.
func NewSSTableIterator(table *sstable.SSTable, isCompressed bool) (*SSTableIterator, error) {
	table.Data.Close()
	table.Index.Close()
	table.Summary.Close()

	var iterator *SSTableIterator = &SSTableIterator{isSSTableCompressed: isCompressed}
	var err error

	iterator.current_offset, iterator.end_offset, err = getDataOffsets(table)
	if err != nil {
		return nil, err
	}

	iterator.data, err = os.Open(table.Data.Name())
	if err != nil {
		return nil, err
	}

	_, err = iterator.data.Seek(iterator.current_offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

// Returns a pointer to the next non-deleted record, also returns an error
// Will NEVER return a deleted record
// If all non-deleted records have been iterated over, returns nil as the record pointer
// If any errors occur, the returned record is nil and the error is returned
func (iter *SSTableIterator) Next() (*model.Record, error) {

	for iter.current_offset < iter.end_offset {
		record_p, _, err := model.Deserialize(iter.data, iter.isSSTableCompressed)
		if err != nil {
			return nil, err
		}
		iter.current_offset, err = iter.data.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		if record_p.Tombstone == 0 {
			return record_p, nil
		}
	}

	return nil, nil
}

// Closes the data file that was being iterated over
func (iter *SSTableIterator) Stop() {
	iter.data.Close()
}
