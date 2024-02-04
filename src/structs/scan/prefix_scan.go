package scan

import (
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/iterators"
)

// Returns an array of records with keys containing the passed prefix
// The array contains at most pageSize records from the page with the passed page number
// If an invalid page number or page size is passed, an empty array is returned with no error
func PrefixScan(prefix string, pageNumber int, pageSize int, SSTableCompressionOn bool, compressionMap map[string]uint64) ([]*model.Record, error) {
	var records []*model.Record = make([]*model.Record, 0)

	//If the page number is invalid, return an empty array
	if pageNumber < 1 {
		return records, nil
	}

	//If the page size is invalid, return an empty array
	if pageSize < 1 {
		return records, nil
	}

	//Create a new prefix iterator
	prefixIter, err := iterators.NewPrefixIterator(prefix, SSTableCompressionOn, compressionMap)
	if err != nil {
		return records, err
	}
	//Stop the iterator once we are finished
	defer prefixIter.Stop()

	//The number of records we need to skip before we get to the right page
	var recordsToSkip int = (pageNumber - 1) * pageSize

	//Iterating through the records we need to skip
	for i := 0; i < recordsToSkip; i++ {
		record, err := prefixIter.Next()
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
		record, err := prefixIter.Next()
		if err != nil {
			return records, err
		}

		//If there are no more records left, return the non-full array
		if record == nil {
			break
		}
		records = append(records, record)
	}

	return records, nil
}
