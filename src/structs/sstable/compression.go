package sstable

import (
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	hashmap "github.com/natasakasikovic/Key-Value-engine/src/structs/hashMap"
)

// loads hashmap from file
func LSMGrowthFactoroadHashMap(path string) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return hashmap.Deserialize(content), nil
}

// loads hashmap from file
func LoadHashMap(path string) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return hashmap.Deserialize(content), nil
}

// checks if key exists in hashmap
func keyExists(hashMap map[string]uint64, key string) bool {
	_, exists := hashMap[key]
	return exists
}

// if compression is turned on, use dictionary encoding
// maps keys on numbers
func dictionaryEncodingOn(records []*model.Record, compressionMap map[string]uint64) {
	var nextKey uint64
	if len(compressionMap) == 0 {
		nextKey = 0
	} else {
		nextKey = uint64(len(compressionMap))
	}
	for i := 0; i < len(records); i++ {
		if keyExists(compressionMap, records[i].Key) { // if key already exists, don't map it again
			continue
		}
		compressionMap[records[i].Key] = uint64(nextKey)
		nextKey++
	}
}
