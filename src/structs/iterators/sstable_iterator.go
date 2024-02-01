package iterators

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

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
		table.Data, err = os.Open(table.Data.Name())
		if err != nil {
			return 0, 0, err
		}

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

// Returns a pointer to the next iterated record, also returns an error
// If all records have been iterated over, returns nil as the record pointer
// If any errors occur, the returned record is nil and the error is returned
func (iter *SSTableIterator) Next() (*model.Record, error) {

	for iter.current_offset < iter.end_offset {
		record_p, _, err := Deserialize(iter.data)
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
