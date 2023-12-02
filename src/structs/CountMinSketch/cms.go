package countminsketch

import (
	"github.com/natasakasikovic/Key-Value-engine/src/structs/hash"
	"encoding/binary"
	"math"
)

type CMS struct {
	table  [][]uint32     // Table of occurrences
	k      uint32         // Number of hash functions / number of rows
	m      uint32         // Number of columns
	hashes []hash.HashWithSeed // Array of used hash functions
}

// Allocates memory for the value table and hash list, and returns an empty CMS object
func CreateCMS(epsilon float64, delta float64) *CMS {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	table := make([][]uint32, k)
	for i := range table {
		table[i] = make([]uint32, m)
	}

	hashes := hash.CreateHashFunctions(uint(k))

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

// Returns a byte array representing a serialized CMS object
func (cms *CMS) Serialize() []byte {
	var size uint32 = 0
	size += 4                 // 4 Bytes for K
	size += 4                 // 4 Bytes for M
	size += 4 * cms.k         // 4 Bytes K times because of K seeds
	size += 4 * cms.k * cms.m // 4 Bytes per table field

	data := make([]byte, size)

	binary.BigEndian.PutUint32(data, cms.k)

	m_slice := data[4:8]
	binary.BigEndian.PutUint32(m_slice, cms.m)

	hashes_slice_end := 8 + cms.k*4
	hashes_slice := data[8:hashes_slice_end]

	// Serialization of hash list
	for i, hash := range cms.hashes {
		seedBytes := hash.Seed
		for j := 0; j < len(seedBytes); j++ {
			hashes_slice[i*len(seedBytes)+j] = seedBytes[j]
		}
	}

	// Serialization of table data
	tableData := data[hashes_slice_end:]
	for i := 0; i < int(cms.k); i++ {
		for j := 0; j < int(cms.m); j++ {
			value := cms.table[i][j]
			offset := 4 * (i*int(cms.m) + j)
			binary.BigEndian.PutUint32(tableData[offset:offset+4], value)
		}
	}

	return data
}

// Creates and returns a new CMS object from serialized data
func Deserialize(data []byte) *CMS {
	cms := &CMS{}

	K := binary.BigEndian.Uint32(data[0:4])
	M := binary.BigEndian.Uint32(data[4:8])

	hash_list := make([]hash.HashWithSeed, K)

	hashes_slice := data[8:]
	// i is the number in order of the hash we are currently deserializing
	for i := 0; i < int(K); i++ {
		hash_slice := hashes_slice[i*4 : i*4+4]
		seed_value := binary.BigEndian.Uint32(hash_slice) // Value of seed as a 32bit Integer

		seed := make([]byte, 4) // Value of seed as an array of 4 bytes

		binary.BigEndian.PutUint32(seed, seed_value) //Copy of seed value into the seed array
		hash_list[i] = hash.HashWithSeed{Seed: seed}
	}

	hashes_slice_end := 8 + K*4

	// Allocation of table matrix
	table := make([][]uint32, K)
	for i := range table {
		table[i] = make([]uint32, M)
	}

	// Deserialization of table
	table_data_slice := data[hashes_slice_end:]
	for i := uint32(0); i < K; i++ {
		for j := uint32(0); j < M; j++ {
			// Slice containing the data of one value from the table
			value_index := (i*M + j) * 4 // i-th row, j-th column, and the size of each value is 4 bytes
			value_slice := table_data_slice[value_index : value_index+4]
			table[i][j] = binary.BigEndian.Uint32(value_slice)
		}
	}

	cms.k = K
	cms.m = M
	cms.hashes = hash_list
	cms.table = table

	return cms
}

func CalculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}
