package iterators

import (
	"strings"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
)

type PrefixIterator struct {
	iterGroup *IteratorGroup
	record    *model.Record
	prefix    string
}

func NewPrefixIterator(prefix string, isSStableCompressed bool) (*PrefixIterator, error) {
	var prefixIter *PrefixIterator = &PrefixIterator{prefix: prefix}
	var iterators []Iterator

	allSStables, err := loadAllSStables()
	if err != nil {
		return nil, err
	}

	//Loop through every sstable
	//If it could contain records with the given prefix, create an iterator for it
	for i := 0; i < len(allSStables); i++ {
		if allSStables[i].MaxKey < prefix || (allSStables[i].MinKey > prefix && !strings.HasPrefix(allSStables[i].MinKey, prefix)) {
			//If the sstable definitely contains no record with the given prefix, free it's resources
			allSStables[i].Data.Close()
			allSStables[i].Index.Close()
			allSStables[i].Summary.Close()
			allSStables[i] = nil
		} else {
			//Otherwise, create an iterator for the sstable
			sstableIter, err := NewSSTableIterator(allSStables[i], isSStableCompressed)
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

		if record_p == nil || strings.HasPrefix(record_p.Key, prefix) {
			prefixIter.record = record_p
			break
		}
	}

	prefixIter.iterGroup = iterGroup
	return prefixIter, nil
}

func (prefixIter *PrefixIterator) Next() (*model.Record, error) {
	//If all the records in the range have been read
	if prefixIter.record == nil {
		return nil, nil
	}

	//Otherwise, return the saved record
	var retRecord *model.Record = prefixIter.record
	var err error

	//Find the next record, and save it
	prefixIter.record, err = prefixIter.iterGroup.Next()
	if err != nil {
		return nil, err
	}
	//If the found record doesn't have the needed prefix, save the record as nil;
	//We have iterated through all records in the given range
	if prefixIter.record != nil && !strings.HasPrefix(prefixIter.record.Key, prefixIter.prefix) {
		prefixIter.record = nil
	}

	return retRecord, nil
}

func (prefixIter *PrefixIterator) Stop() {
	prefixIter.iterGroup.Stop()
	prefixIter.record = nil
}
