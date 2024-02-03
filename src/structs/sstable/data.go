package sstable

import (
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

// serializes records - turns them to [][]byte
func (sstable *SSTable) serializeData(records []*model.Record) [][]byte {
	var content [][]byte
	for _, record := range records {
		content = append(content, record.Serialize(sstable.compressionOn))
	}
	return content
}

// function that searches data in sstable
// returns record if it is found, otherwise returns nil
func (sstable *SSTable) searchData(isSeparate bool, offset1 int, offset2 int, key string) (*model.Record, error) {
	if offset2 == 0 {
		if isSeparate {
			fileSize, _ := utils.GetFileLength(sstable.Data)
			offset2 = int(fileSize)
		} else {
			offset2 = int(sstable.IndexOffset)
		}
	}
	sstable.Data.Seek(int64(offset1), 0)
	for offset1 < offset2 {
		record, bytesRead, err := model.Deserialize(sstable.data, sstable.compressionOn)
		if err != nil {
			return nil, err
		}
		if record.Key == key {
			return record, nil
		}
		offset1 += int(bytesRead)
	}
	return nil, nil
}
