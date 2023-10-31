package bloomFilter

import(
	"math"
)

type BloomFilter struct {
	bitset   []byte
	k, m     uint
	hashFunc []HashWithSeed
}

func newBf(n int, p float64) BloomFilter { 
	m := CalculateM(n, p)
	k := CalculateK(n, m)

	return BloomFilter{
		bitset:   make([]byte, m),
		k:        k,
		m:        m,
		hashFunc: CreateHashFunctions(k),
	}
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (b *BloomFilter) find(s string) bool {
	for _, fn := range b.hashFunc {
		var index uint = uint(fn.Hash([]byte(s)))
		compressed := index % b.m
		if b.bitset[compressed] == 0 {
			return false
		}
	}
	return true
}

func(b *Bloomfilter) insert(s string) {
	for _, fn := range b.hashFunc {
		var index uint = uint(fn.Hash([]byte(s)))
		compressed := index % b.m
		b.bitset[compressed] = 1
	}
}
