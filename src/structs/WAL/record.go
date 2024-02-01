package WAL

import (
	"encoding/binary"
	"errors"
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
	crc       uint32
	timestamp uint64
	tombstone byte
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
	toNext    uint32 // number of bytes that we
}

func NewRecord(tombstone byte, key string, value []byte) *Record {
	crcCheck := append([]byte(key), value...)
	return &Record{crc: CRC32(crcCheck), timestamp: uint64(time.Now().UnixNano()), tombstone: tombstone, keySize: uint64(len(key)), valueSize: uint64(len(value)), key: key, value: value}
}
func (r *Record) GetRecordLength() uint64 {
	return 4 + 8 + 1 + 8 + 8 + r.keySize + r.valueSize
}
func (r *Record) RecordToBytes() []byte {
	size := r.GetRecordLength()
	bytes := make([]byte, size)
	binary.BigEndian.PutUint32(bytes[CRC_START:CRC_START+CRC_SIZE], r.crc)
	binary.BigEndian.PutUint64(bytes[TIMESTAMP_START:TIMESTAMP_START+TIMESTAMP_SIZE], r.timestamp)
	bytes[TOMBSTONE_START] = r.tombstone
	binary.BigEndian.PutUint64(bytes[KEY_SIZE_START:KEY_SIZE_START+KEY_SIZE_SIZE], r.keySize)
	binary.BigEndian.PutUint64(bytes[VALUE_SIZE_START:VALUE_SIZE_START+VALUE_SIZE_SIZE], r.valueSize)
	keySlice := bytes[KEY_START : KEY_START+r.keySize]
	valueSlice := bytes[KEY_START+r.keySize:]
	for i := uint64(0); i < r.keySize; i++ {
		keySlice[i] = r.key[i]
	}
	for i := uint64(0); i < r.valueSize; i++ {
		valueSlice[i] = r.value[i]
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
	r.crc = crc
	r.timestamp = timestamp
	r.tombstone = tombstone
	r.keySize = keySize
	r.valueSize = valueSize
	r.key = key
	r.value = value
	return r, int(r.GetRecordLength()), nil
}