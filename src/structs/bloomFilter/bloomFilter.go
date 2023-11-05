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
		bitset:   make([]byte, m/8+1),
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
		var hash uint = uint(fn.Hash([]byte(s)))
		compressed := hash % b.m
		index := compressed / 8
		mask := byte(1 << (7 - (compressed % 8)))
		if b.bitset[index]&mask == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) Insert(s string) {
	for _, fn := range b.hashFunc {
		var hash uint = uint(fn.Hash([]byte(s)))
		compressed := hash % b.m
		index := compressed / 8
		mask := byte(1 << (7 - (compressed % 8)))
		b.bitset[index] = b.bitset[index] | mask
	}
}

func (b *BloomFilter) Serialize() []byte {

	var size int = 4 + 4 + int(b.k)*4 + int(b.m)/8 + 1
	bytes := make([]byte, size)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(b.m)) //Bitset length
	binary.BigEndian.PutUint32(bytes[4:8], uint32(b.k)) //Number of hash functions

	i := 0
	for _, fn := range b.hashFunc {
		copy(bytes[8+4*i:12+4*i], fn.Seed) //Hash seeds
		i++
	}

	copy(bytes[4+4+int(b.k)*4:], b.bitset) //bitset

	return bytes
}

func Deserialize(bytes []byte) *BloomFilter {

	m := uint(binary.BigEndian.Uint32(bytes[0:4]))
	k := uint(binary.BigEndian.Uint32(bytes[4:8]))

	hashFunc := make([]HashWithSeed, k)
	for i := 0; i < int(k); i++ {
		hashFunc[i] = HashWithSeed{Seed: bytes[8+i*4 : 12+i*4]}
	}
	return &BloomFilter{
		m:        m,
		k:        k,
		hashFunc: hashFunc,
		bitset:   bytes[8+k*4:],
	}
}
