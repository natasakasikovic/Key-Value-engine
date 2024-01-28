package sstable

import (
	"io"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
)

// serializes records - turns them to [][]byte
func (sstable *SSTable) serializeData(records []*model.Record) [][]byte {
	var content [][]byte
	for _, record := range records {
		content = append(content, record.ToBytes())
	}
	return content
}

// return a pointer to record if found, otherwise returns nil
func (sstable *SSTable) searchData(isSeparate bool, offset1 int, offset2 int, key string) *model.Record {
	var data []byte
	var err error

	if isSeparate {
		if offset2 == 0 {
			sstable.data.Seek(int64(offset1), 0)
			data, err = io.ReadAll(sstable.data)
		} else {
			offset := int(offset2 - offset1)
			data = make([]byte, offset)
			sstable.data.Seek(int64(offset1), 0)
			_, err = io.ReadAtLeast(sstable.data, data, offset)
			if err != nil {
				return nil
			}
		}

		for len(data) > 0 {
			record, read, err := model.FromBytes(data)
			if err != nil {
				return nil
			}
			if record.Key == key {
				return &record
			}
			data = data[read:]
		}
	}

	return nil
}
