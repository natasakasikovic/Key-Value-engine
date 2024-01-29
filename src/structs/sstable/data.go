package sstable

import (
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

// returns a pointer to record if found, otherwise returns nil
func (sstable *SSTable) searchData(isSeparate bool, offset1 int, offset2 int, key string) (*model.Record, error) {
	var data []byte
	var err error

	if isSeparate {
		data, err = sstable.loadDataSeparate(offset2, offset1)
	} else {
		data, err = sstable.loadDataSingle(offset2, offset1)
	}

	if err != nil {
		return nil, err
	}

	for len(data) > 0 { // read records until you have data
		record, readBytes, err := model.FromBytes(data)
		if err != nil {
			return nil, err
		}
		if record.Key == key {
			return &record, nil
		}
		data = data[readBytes:]
	}

	return nil, nil
}
