package countminsketch

import (
	"math"
)

type CMS struct {
	table  [][]uint32     // Table of occurrences
	k      uint32         // Number of hash functions / number of rows
	m      uint32         // Number of columns
	hashes []HashWithSeed // Array of used hash functions
}

// Allocates memory for the value table and hash list, and returns an empty CMS object
func CreateCMS(k uint32, m uint32) *CMS {
	table := make([][]uint32, k)
	for i := range table {
		table[i] = make([]uint32, m)
	}

	hashes := CreateHashFunctions(uint(k))

	cms := &CMS{table: table, k: k, m: m, hashes: hashes}

	return cms
}

func (cms *CMS) Insert(new_element string) {
	for hash_index, hash := range cms.hashes {
		index := hash.Hash([]byte(new_element))
		index %= uint64(cms.m)
		cms.table[hash_index][index] += 1
	}
}

func (cms *CMS) Search(new_element string) uint32 {
	var minimum uint32 = math.MaxUint32
	for hash_index, hash := range cms.hashes {
		index := hash.Hash([]byte(new_element))
		index %= uint64(cms.m)

		counter := cms.table[hash_index][index]
		if counter < minimum {
			minimum = counter
		}
	}
	return minimum
}
