package model

import "encoding/binary"

type Record struct {
	Key       string
	Value     []byte
	Tombstone byte
	Timestamp uint64
}

func (r *Record) ToBytes() []byte {
	var data []byte
	data = append(data, []byte(r.Key)...)
	data = append(data, r.Value...)
	data = append(data, r.Tombstone)
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, r.Timestamp)
	data = append(data, timestamp...)
	return data
}