package iterators

import (
	"errors"
	"fmt"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
)

type Iterator interface {
	Next() (*model.Record, error) //Should return a pointer to the next non-deleted record
	Stop()                        //Should close files and free resources
}

// Iterator groups allow you to iterate using multiple iterators at the same time
type IteratorGroup struct {
	iterators     []Iterator
	iteratorCount int
	records       []*model.Record
}

// Creates and initializes a new iterator group containing all the passed iterators
func NewIteratorGroup(iterators []Iterator) (*IteratorGroup, error) {
	var group IteratorGroup = IteratorGroup{
		iterators:     iterators,
		iteratorCount: len(iterators),
		records:       make([]*model.Record, len(iterators)),
	}

	//Initialize record array to the first record of each iterator
	var err error
	for i := 0; i < group.iteratorCount; i++ {
		group.records[i], err = group.iterators[i].Next()
		if err != nil {
			group.Stop()
			return nil, errors.New(fmt.Sprintf("Error initializing iterator %d: %s", i, err.Error()))
		}
	}
	return &group, nil
}

// Returns a pointer to the next record in the iterator group
// The next record is the record containing the smallest key from all the iterators in the group
// Will never return a deleted record
func (iterGroup *IteratorGroup) Next() (*model.Record, error) {
	const EMPTY_KEY string = ""

	var getMinKey func() string = func() string {
		var min string = EMPTY_KEY
		for i := 0; i < iterGroup.iteratorCount; i++ {
			if iterGroup.records[i] != nil && (iterGroup.records[i].Key < min || min == EMPTY_KEY) {
				min = iterGroup.records[i].Key
			}
		}
		return min
	}

	var getKeyCount func(string) uint = func(key string) uint {
		var count uint = 0
		for i := 0; i < iterGroup.iteratorCount; i++ {
			if iterGroup.records[i] != nil && iterGroup.records[i].Key == key {
				count += 1
			}
		}
		return count
	}

	var moveDuplicatesForward func(string) error = func(key string) error {
		var latestTS uint64 = 0
		var latestIndex int = -1
		var err error

		for i := 0; i < iterGroup.iteratorCount; i++ {
			if iterGroup.records[i] != nil && iterGroup.records[i].Key == key && iterGroup.records[i].Timestamp > latestTS {
				latestIndex = i
				latestTS = iterGroup.records[i].Timestamp
			}
		}

		for i := 0; i < iterGroup.iteratorCount; i++ {
			if iterGroup.records[i] != nil && iterGroup.records[i].Key == key && i != latestIndex {
				iterGroup.records[i], err = iterGroup.iterators[i].Next()
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	var getIndexOfIteratorWithKey func(string) int = func(key string) int {
		for i := 0; i < iterGroup.iteratorCount; i++ {
			if iterGroup.records[i] != nil && iterGroup.records[i].Key == key {
				return i
			}
		}
		return -1
	}

	var err error
	for {
		//Find the smallest key of the records in the current iteration
		var minKey string = getMinKey()
		if minKey == EMPTY_KEY {
			return nil, nil
		}

		//If there are multiple records with the same key, move all but the latest iterator forward
		for getKeyCount(minKey) > 1 {
			err = moveDuplicatesForward(minKey)
			if err != nil {
				return nil, err
			}
		}

		//Get the index of the record and iterator with the smallest key
		var minIndex int = getIndexOfIteratorWithKey(minKey)
		//Create a copy of the record
		var recordCopy *model.Record = iterGroup.records[minIndex]

		//Move the iterator forward
		iterGroup.records[minIndex], err = iterGroup.iterators[minIndex].Next()
		if err != nil {
			return nil, err
		}

		if recordCopy != nil {
			return recordCopy, nil
		}
	}
}

// Frees allocated resources and closes files
func (iterGroup *IteratorGroup) Stop() {
	for i := 0; i < iterGroup.iteratorCount; i++ {
		iterGroup.iterators[i].Stop()
	}
}
