package simHash

import (
	"crypto/md5"
	"encoding/binary"
	"math/bits"
)

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
