package hashmap

import (
	"encoding/binary"
)

// serializes hash map
// for each key-value pair saves length of the key, key, length of the value, value.
func Serialize(hashmap map[string]uint64) []byte {
	var data []byte

	for key, value := range hashmap {
		keyLength := make([]byte, 8)
		binary.BigEndian.PutUint64(keyLength, uint64(len(key)))
		data = append(data, keyLength...)

		data = append(data, []byte(key)...)

		valueBuffer := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuffer, value)
		data = append(data, valueBuffer...)

	}
	return data
}

// reconstructs a hash map from a serialized byte slice
func Deserialize(data []byte) map[string]uint64 {
	hashmap := make(map[string]uint64)
	for len(data) > 0 {
		keyLength := binary.BigEndian.Uint64(data[:8])
		data = data[8:]

		key := string(data[:keyLength])
		data = data[keyLength:]

		hashmap[key] = binary.BigEndian.Uint64(data[:8])
		data = data[8:]
	}

	return hashmap
}
