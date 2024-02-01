package iterators

import (
	"sort"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
)

type MemtableIterator struct {
	data          memtable.DataStructure
	key_list      []string //Sorted list of keys in the memtable
	key_count     int      //Number of keys
	current_index int      //Current position of the iterator
}

// Returns a pointer to a new memtable iterator as well as an error value
// The iterator gets invalidated if the memtable data is changed
func NewMemtableIterator(table *memtable.Memtable) (*MemtableIterator, error) {
	var iter *MemtableIterator = &MemtableIterator{}
	iter.data = table.Data
	iter.current_index = 0
	iter.key_count = len(table.Keys)
	iter.key_list = make([]string, iter.key_count)
	copy(iter.key_list, table.Keys)
	sort.Strings(iter.key_list)

	return iter, nil
}

func (iter *MemtableIterator) Next() (*model.Record, error) {
	for iter.current_index < iter.key_count {
		record, err := iter.data.Find(iter.key_list[iter.current_index])
		if err != nil {
			return nil, err
		}

		if record.Tombstone == 0 {
			return &record, nil
		}
	}
	return nil, nil
}
