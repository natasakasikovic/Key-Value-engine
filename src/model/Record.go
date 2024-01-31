package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func (r *Record) String() string {
	if r.Tombstone == 0 {
		return fmt.Sprintf("  %s\t%s", r.Key, string(r.Value))
	} else {
		return fmt.Sprintf("✝ %s\t%s", r.Key, string(r.Value))
	}

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

func FromBytes(data []byte) (Record, uint64, error) {
	var record Record
	buffer := bytes.NewReader(data)

	binary.Read(buffer, binary.BigEndian, &record.KeySize)
	keyBytes := make([]byte, record.KeySize)
	buffer.Read(keyBytes)
	record.Key = string(keyBytes)

	binary.Read(buffer, binary.BigEndian, &record.Crc)
	binary.Read(buffer, binary.BigEndian, &record.Timestamp)
	binary.Read(buffer, binary.BigEndian, &record.Tombstone)

	read := 8 + record.KeySize + 1 + 8 + 4
	if record.Tombstone != 1 {
		binary.Read(buffer, binary.BigEndian, &record.ValueSize)
		valueBytes := make([]byte, record.ValueSize)
		buffer.Read(valueBytes)
		record.Value = valueBytes
		read += (8 + record.ValueSize)

	} else {
		record.ValueSize = 0
		record.Value = []byte{}
	}

	return record, read, nil
}

// FOR WAL
func NewRecord(tombstone byte, key string, value []byte) *Record {
	crcCheck := append([]byte(key), value...)
	return &Record{Crc: CRC32(crcCheck), Timestamp: uint64(time.Now().UnixNano()), Tombstone: tombstone, KeySize: uint64(len(key)), ValueSize: uint64(len(value)), Key: key, Value: value}
}

func NewRecordTimestamp(tombstone byte, key string, value []byte, timestamp uint64) *Record {
	crcCheck := append([]byte(key), value...)
	return &Record{Crc: CRC32(crcCheck), Timestamp: timestamp, Tombstone: tombstone, KeySize: uint64(len(key)), ValueSize: uint64(len(value)), Key: key, Value: value}
}

func (r *Record) GetRecordLength() uint64 {
	return 4 + 8 + 1 + 8 + 8 + r.KeySize + r.ValueSize
}
func (r *Record) RecordToBytes() []byte {
	size := r.GetRecordLength()
	bytes := make([]byte, size)
	binary.BigEndian.PutUint32(bytes[CRC_START:CRC_START+CRC_SIZE], r.Crc)
	binary.BigEndian.PutUint64(bytes[TIMESTAMP_START:TIMESTAMP_START+TIMESTAMP_SIZE], r.Timestamp)
	bytes[TOMBSTONE_START] = r.Tombstone
	binary.BigEndian.PutUint64(bytes[KEY_SIZE_START:KEY_SIZE_START+KEY_SIZE_SIZE], r.KeySize)
	binary.BigEndian.PutUint64(bytes[VALUE_SIZE_START:VALUE_SIZE_START+VALUE_SIZE_SIZE], r.ValueSize)
	keySlice := bytes[KEY_START : KEY_START+r.KeySize]
	valueSlice := bytes[KEY_START+r.KeySize:]
	for i := uint64(0); i < r.KeySize; i++ {
		keySlice[i] = r.Key[i]
	}
	for i := uint64(0); i < r.ValueSize; i++ {
		valueSlice[i] = r.Value[i]
	}
	return bytes
}
func ReadSingleRecord(data []byte) (*Record, int, error) {
	r := &Record{}
	crc := binary.BigEndian.Uint32(data[CRC_START : CRC_START+CRC_SIZE])

	timestamp := binary.BigEndian.Uint64(data[TIMESTAMP_START : TIMESTAMP_START+TIMESTAMP_SIZE])
	tombstone := data[TOMBSTONE_START]
	keySize := binary.BigEndian.Uint64(data[KEY_SIZE_START : KEY_SIZE_START+KEY_SIZE_SIZE])
	valueSize := binary.BigEndian.Uint64(data[VALUE_SIZE_START : VALUE_SIZE_START+VALUE_SIZE_SIZE])
	keySlice := data[KEY_START : KEY_START+keySize]
	key := string(keySlice)

	value := data[KEY_START+keySize : KEY_START+keySize+valueSize]
	crcCheck := append([]byte(key), value...)
	if crc != CRC32(crcCheck) {
		return r, 0, errors.New("corrupted file")
	}
	r.Crc = crc
	r.Timestamp = timestamp
	r.Tombstone = tombstone
	r.KeySize = keySize
	r.ValueSize = valueSize
	r.Key = key
	r.Value = value
	return r, int(r.GetRecordLength()), nil
}
