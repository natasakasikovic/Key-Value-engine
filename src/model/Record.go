package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
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

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// deserializes a record by reading field by field from the file
// returns Record, number of bytes read, error
func Deserialize(file *os.File) (*Record, uint64, error) {
	var err error
	var record Record = Record{}

	var keySizeBuffer []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(file, keySizeBuffer, 8)
	if err != nil {
		return nil, 0, err
	}
	record.KeySize = binary.BigEndian.Uint64(keySizeBuffer)

	var bufferKey []byte = make([]byte, record.KeySize)
	_, err = io.ReadAtLeast(file, bufferKey, int(record.KeySize))
	if err != nil {
		return nil, 0, err
	}
	record.Key = string(bufferKey)

	var crcBuffer []byte = make([]byte, 4)
	_, err = io.ReadAtLeast(file, crcBuffer, 4)
	if err != nil {
		return nil, 0, err
	}
	crc := binary.BigEndian.Uint32(crcBuffer)
	record.Crc = crc

	var timestampBuffer []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(file, timestampBuffer, 8)
	if err != nil {
		return nil, 0, err
	}
	timestamp := binary.BigEndian.Uint64(timestampBuffer)
	record.Timestamp = timestamp

	var tombstoneBuffer []byte = make([]byte, 1)
	_, err = io.ReadAtLeast(file, tombstoneBuffer, 1)
	if err != nil {
		return nil, 0, err
	}
	tombstone := tombstoneBuffer[0]
	record.Tombstone = byte(tombstone)

	read := 8 + record.KeySize + 1 + 8 + 4
	if tombstone != 1 {
		var valueSizeBuffer []byte = make([]byte, 8)
		_, err = io.ReadAtLeast(file, valueSizeBuffer, 8)
		if err != nil {
			return nil, 0, err
		}
		record.ValueSize = binary.BigEndian.Uint64(valueSizeBuffer)

		var valueBuffer []byte = make([]byte, record.ValueSize)
		_, err = io.ReadAtLeast(file, valueBuffer, int(record.ValueSize))

		if err != nil {
			return nil, 0, err
		}
		record.Value = valueBuffer
		read += (8 + record.ValueSize)
	} else {
		record.ValueSize = 0
		record.Value = []byte{}
	}

	crcChech := append([]byte(record.Key), record.Value...)
	if CRC32(crcChech) != record.Crc {
		return nil, read, errors.New("not valid record")
	}

	return &record, read, nil
}
