package bloomFilter

import (
	"encoding/binary"
	"math"
)

type BloomFilter struct {
	bitset   []byte
	k, m     uint
	hashFunc []HashWithSeed
}

func NewBf(n int, p float64) BloomFilter {
	m := calculateM(n, p)
	k := calculateK(n, m)

	return BloomFilter{
		bitset:   make([]byte, m),
		k:        k,
		m:        m,
		hashFunc: CreateHashFunctions(k),
	}
}

func calculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func calculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (b *BloomFilter) Find(s string) bool {
	for _, fn := range b.hashFunc {
		var index uint = uint(fn.Hash([]byte(s)))
		compressed := index % b.m
		if b.bitset[compressed] == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) Insert(s string) {
	for _, fn := range b.hashFunc {
		var index uint = uint(fn.Hash([]byte(s)))
		compressed := index % b.m
		b.bitset[compressed] = 1
	}
}

func Serialize(b *BloomFilter) []byte {
	var size int = 4 + 4 + int(b.k)*4 + int(b.m)
	bytes := make([]byte, size)
	binary.BigEndian.PutUint32(bytes[0:4], uint32(b.m)) //Bitset length
	binary.BigEndian.PutUint32(bytes[4:8], uint32(b.k)) //Number of hash functions
	i := 0
	for _, fn := range b.hashFunc {
		copy(bytes[8+8*i:16+8*i], fn.Seed) //Hash seeds
		i++
	}
	for i := 0; i < int(b.m); i++ {
		bytes = append(bytes, b.bitset[i]) //Bitset
	}
	return bytes
}
