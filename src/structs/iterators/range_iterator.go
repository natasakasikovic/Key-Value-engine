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

func NewRangeIterator(minKey string, maxKey string) (*RangeIterator, error) {
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
			sstableIter, err := NewSSTableIterator(allSStables[i])
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, sstableIter)
		} else {
			//If the sstable contains no record with a key in the given range, close it to prevent leaks
			allSStables[i].Data.Close()
			allSStables[i].Index.Close()
			allSStables[i].Summary.Close()
			allSStables[i] = nil
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

func (rangeIter *RangeIterator) Stop() {
	rangeIter.iterGroup.Stop()
	rangeIter.record = nil
}
