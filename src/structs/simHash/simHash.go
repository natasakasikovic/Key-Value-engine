package simHash

import (
	"crypto/md5"
	"encoding/binary"
	"math/bits"
	"strings"
)

func GetFingerprint(text string) uint64 {
	words := filterWords(strings.Fields(text))
	hashes := make(map[uint64]uint16) // key is hash, value is number of repetitions
	for word, repeat := range words {
		hashes[getWordHash(word)] = repeat
	}

	var fingerprint uint64 = 0
	var mask uint64 = 1

	for i := 0; i < 64; i++ {
		sum := 0
		for key, value := range hashes { // iterate over hashes, sum columns
			if bits.OnesCount64(key&mask) == 1 {
				sum += int(value)
			} else {
				sum -= int(value)
			}
		}
		if sum > 0 { // if sum is greater than zero, put 1 in fingerprint
			fingerprint |= mask
		}
		mask <<= 1 // shift mask
	}
	return fingerprint
}

// function removes stop words and gives weight if some word appeared more than once
func filterWords(words []string) map[string]uint16 {
	retVal := make(map[string]uint16)
	for _, word := range words {
		if len(word) > 2 && containsKey(retVal, word) { // if len of word is > 2 and exists in hashMap
			retVal[word]++
		} else if len(word) > 2 { // if len of word is > 2, and it does not exist in map
			retVal[word] = 1
		}
	}
	return retVal
}

// checks if a value exists in the given map
func containsKey(words map[string]uint16, key string) bool {
	_, exists := words[key]
	return exists
}

func getWordHash(text string) uint64 {
	fn := md5.New()
	fn.Write(append([]byte(text)))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

// calculates hamming distance between two fingerprints
func HammingDistance(x, y uint64) uint8 {
	return uint8(bits.OnesCount64(x ^ y))
}
