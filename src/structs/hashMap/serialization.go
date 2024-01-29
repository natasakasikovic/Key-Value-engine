package hashmap

import (
	"encoding/binary"
)

// serializes hash map
// for each key-value pair saves length of the key, key, length of the value, value.
func Serialize(hashmap map[string]string) []byte {
	var data []byte

	for key, value := range hashmap {
		keyLength := make([]byte, 8)
		binary.BigEndian.PutUint64(keyLength, uint64(len(key)))
		data = append(data, keyLength...)

		data = append(data, []byte(key)...)

		valueLength := make([]byte, 8)
		binary.BigEndian.PutUint64(valueLength, uint64(len(value)))
		data = append(data, valueLength...)

		data = append(data, []byte(value)...)
	}
	return data
}

// reconstructs a hash map from a serialized byte slice
func Deserialize(data []byte) map[string]string {
	hashmap := make(map[string]string)
	for len(data) > 0 {
		keyLength := binary.BigEndian.Uint64(data[:8])
		data = data[8:]

		key := string(data[:keyLength])
		data = data[keyLength:]

		valueLength := binary.BigEndian.Uint64(data[:8])
		data = data[8:]

		value := string(data[:valueLength])
		data = data[valueLength:]

		hashmap[key] = value
	}

	return hashmap
}
