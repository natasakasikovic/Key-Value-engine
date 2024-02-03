package scan

import (
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/iterators"
)

// Returns an array of records containing keys within the passed range
// The array contains at most pageSize records from the page with the passed page number
// If an invalid page number or page size is passed, an empty array is returned with no error
func RangeScan(minKey string, maxKey string, pageNumber int, pageSize int, SSTableCompressionOn bool) ([]*model.Record, error) {
	var records []*model.Record = make([]*model.Record, 0)

	if pageNumber < 1 {
		return records, nil
	}

	if pageSize < 1 {
		return records, nil
	}

	rangeIter, err := iterators.NewRangeIterator(minKey, maxKey, SSTableCompressionOn)
	defer rangeIter.Stop()

	if err != nil {
		return records, err
	}

	//The number of records we need to skip before we get to the right page
	var recordsToSkip int = (pageNumber - 1) * pageSize

	for i := 0; i < recordsToSkip; i++ {
		record, err := rangeIter.Next()
		if err != nil {
			return records, err
		}

		//If we iterated through all the records before getting to the right page return the empty array
		if record == nil {
			return records, nil
		}
	}

	//Read the records on the right page
	for i := 0; i < pageSize; i++ {
		record, err := rangeIter.Next()
		if err != nil {
			return records, err
		}

		if record == nil {
			break
		}
		records = append(records, record)
	}

	return records, nil
}
