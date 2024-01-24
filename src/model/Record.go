package model

import (
	"bytes"
	"encoding/binary"
)

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func (r *Record) ToBytes() []byte {
	var buffer bytes.Buffer
	binary.Write(&buffer, binary.BigEndian, r.KeySize)
	buffer.Write([]byte(r.Key))
	binary.Write(&buffer, binary.BigEndian, r.Crc)
	binary.Write(&buffer, binary.BigEndian, r.Timestamp)
	buffer.WriteByte(r.Tombstone)

	// Conditionally write valueSize and value based on tombstone value
	if r.Tombstone != 1 {
		binary.Write(&buffer, binary.BigEndian, r.ValueSize)
		buffer.Write([]byte(r.Value))
	}

	return buffer.Bytes()
}

func FromBytes(data []byte) (Record, error) {
	var record Record
	buffer := bytes.NewReader(data)

	binary.Read(buffer, binary.BigEndian, &record.KeySize)
	keyBytes := make([]byte, record.KeySize)
	buffer.Read(keyBytes)
	record.Key = string(keyBytes)

	binary.Read(buffer, binary.BigEndian, &record.Crc)
	binary.Read(buffer, binary.BigEndian, &record.Timestamp)
	tombstone, err := buffer.ReadByte()
	if err != nil {
		return record, err
	}
	record.Tombstone = tombstone

	if record.Tombstone != 1 {
		binary.Read(buffer, binary.BigEndian, &record.ValueSize)
		valueBytes := make([]byte, record.ValueSize)
		buffer.Read(valueBytes)
		record.Value = valueBytes

	} else {
		record.ValueSize = 0
		record.Value = []byte{}
	}

	return record, nil
}
