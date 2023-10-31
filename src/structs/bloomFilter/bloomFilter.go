package bloomFilter

type BloomFilter struct {
	bitset   []byte
	k, m     uint
	hashFunc []HashWithSeed
}
