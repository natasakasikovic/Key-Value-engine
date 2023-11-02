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

func (b *BloomFilter) Serialize() []byte {

	var size int = 4 + 4 + int(b.k)*32 + int(b.m)
	bytes := make([]byte, size)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(b.m)) //Bitset length
	binary.BigEndian.PutUint32(bytes[4:8], uint32(b.k)) //Number of hash functions

	i := 0
	for _, fn := range b.hashFunc {
		copy(bytes[8+32*i:40+32*i], fn.Seed) //Hash seeds
		i++
	}

	copy(bytes[size-int(b.m):], b.bitset) //bitset

	return bytes
}

func Deserialize(bytes []byte) *BloomFilter {
	b := &BloomFilter{}

	m := uint(binary.BigEndian.Uint32(bytes[0:4]))
	k := uint(binary.BigEndian.Uint32(bytes[4:8]))

	hashFunc := make([]HashWithSeed, k)
	for i := 0; i < int(k); i++ {
		hashFunc[i] = HashWithSeed{Seed: bytes[8+i*32 : 40+i*32]}
	}

	i := 0
	for _, fn := range b.hashFunc {
		copy(bytes[8+32*i:40+32*i], fn.Seed) //Hash seeds
		i++
	}

	b.m = m
	b.k = k
	b.hashFunc = hashFunc
	b.bitset = bytes[8+k*32:]

	return b
}
