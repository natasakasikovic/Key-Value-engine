package iterators

import (
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
)

type RangeIterator struct {
	iterGroup *IteratorGroup
	record    *model.Record
	rangeMin  string
	rangeMax  string
}

// Return a pointer to a new range iterator
// The range iterator allows iteration over records whose key fall into the given range
// The method RangeIterator.Stop() MUST be called after creating a new iterator in order to free it's resources
// An error is returned if it occurs during the creation of the iterator
func NewRangeIterator(minKey string, maxKey string, isSStableCompressed bool, compressionMap map[string]uint64) (*RangeIterator, error) {
	var rangeIter *RangeIterator = &RangeIterator{rangeMin: minKey, rangeMax: maxKey}
	var iterators []Iterator

	allSStables, err := loadAllSStables()
	if err != nil {
		return nil, err
	}

	//Loop through every sstable
	//If it contains records in the given range, create an iterator to it
	for i := 0; i < len(allSStables); i++ {
		if allSStables[i].MinKey <= maxKey && allSStables[i].MaxKey >= minKey {
			sstableIter, err := NewSSTableIterator(allSStables[i], isSStableCompressed, compressionMap)
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, sstableIter)
		}
	}

	//Get iterators to all memtables
	allMemtables := memtable.Memtables.Collection
	for i := 0; i < len(allMemtables); i++ {
		memtableIter, err := NewMemtableIterator(allMemtables[i])
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, memtableIter)
	}

	//Group up all the sstable and memtable iterators
	iterGroup, err := NewIteratorGroup(iterators)
	if err != nil {
		return nil, err
	}

	//Initialize the group iterator to be at the first key in the given range
	for {
		record_p, err := iterGroup.Next()
		if err != nil {
			return nil, err
		}

		if record_p == nil || (record_p.Key >= minKey && record_p.Tombstone == 0) {
			rangeIter.record = record_p
			break
		}
	}

	rangeIter.iterGroup = iterGroup
	return rangeIter, nil
}

// Returns a pointer to the next non-deleted record within the given range, also returns an error
// Will NEVER return a deleted record
// If all non-deleted records within the given range have been iterated over, returns nil as the record pointer
// If any errors occur, the returned record is nil and the error is returned
func (rangeIter *RangeIterator) Next() (*model.Record, error) {
	//If all the records in the range have been read
	if rangeIter.record == nil {
		return nil, nil
	}

	//Otherwise, return the saved record
	var retRecord *model.Record = rangeIter.record
	var err error

	//Find the next record, and save it
	rangeIter.record, err = rangeIter.iterGroup.Next()
	if err != nil {
		return nil, err
	}
	//If the found record is out of range, save the record as nil;
	//We have iterated through all records in the given range
	if rangeIter.record != nil && rangeIter.record.Key > rangeIter.rangeMax {
		rangeIter.record = nil
	}

	return retRecord, nil
}

// Frees the memory and closes the files used by the range iterator
func (rangeIter *RangeIterator) Stop() {
	rangeIter.iterGroup.Stop()
	rangeIter.record = nil
}
