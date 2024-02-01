package iterators

import (
	"fmt"
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	lsmtree "github.com/natasakasikovic/Key-Value-engine/src/structs/LSMTree"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

type SSTableIterator struct {
	data           *os.File
	current_offset int64
	end_offset     int64
}

func isSSTableInSingleFile(table *sstable.SSTable) (bool, error) {
	//sstableFolder - Array of the names of all files in the sstable folder
	sstableFolder, err := utils.GetDirContent(fmt.Sprintf("%s/%s", sstable.PATH, table.Name))
	if err != nil {
		return false, err
	}

	if len(sstableFolder) > 1 {
		return false, nil
	} else {
		return true, nil
	}
}

// Returns the offset where data begins, and the offset right after the data ends
func getDataOffsets(table *sstable.SSTable) (int64, int64, error) {
	oneFile, err := isSSTableInSingleFile(table)
	if err != nil {
		return 0, 0, err
	}

	if !oneFile {
		//IF THE SSTABLE IS IN SEPERATE FILES
		fileLen, err := table.Data.Seek(0, io.SeekEnd)
		return table.DataOffset, fileLen, err
	} else {
		//IF THE WHOLE SSTABLE IS IN THE SAME FILE
		return table.DataOffset, table.IndexOffset, nil
	}
}

// Returns a pointer to a new sstable iterator.
// The iterator becomes unusable after any sstables get inserted into the LSM tree -
// - due to the possibility of the data file being renamed.
func NewSSTableIterator(table *sstable.SSTable) (*SSTableIterator, error) {
	var iterator *SSTableIterator = &SSTableIterator{}
	var err error
	iterator.data, err = os.Open(table.Data.Name())
	if err != nil {
		return nil, err
	}

	iterator.current_offset, iterator.end_offset, err = getDataOffsets(table)
	if err != nil {
		return nil, err
	}
	return iterator, nil
}

// Returns a pointer to the next iterated record, also returns an error
// If all records have been iterated over, returns nil as the record pointer
// If any errors occur, the returned record is nil and the error is returned
func (iter *SSTableIterator) Next() (*model.Record, error) {

	for iter.current_offset < iter.end_offset {
		record_p, _, err := lsmtree.Deserialize(iter.data)
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
