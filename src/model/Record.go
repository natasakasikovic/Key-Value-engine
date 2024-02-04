package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"fmt"
	"time"

	"github.com/natasakasikovic/Key-Value-engine/src/utils"
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

func (r *Record) Serialize(compressionOn bool, compressionMap map[string]uint64) ([]byte, error) {
	if compressionOn {
		return SerializeWithCompression(r, compressionMap)
	}
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

	return buffer.Bytes(), nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// deserializes a record by reading field by field from the file
// returns Record, number of bytes read, any error
func Deserialize(file *os.File, compressionOn bool, compressionMap map[string]uint64) (*Record, uint64, error) {
	if compressionOn {
		return deserializeWithCompression(file, compressionMap)
	}
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

// serializes a Record with variable-length fields, returning the resulting byte slice.
func SerializeWithCompression(r *Record, compressionMap map[string]uint64) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, compressionMap[r.Key])
	if err != nil {
		return nil, err
	}

	utils.PutUvarint(&buf, uint64(r.Crc))
	utils.PutUvarint(&buf, r.Timestamp)
	buf.WriteByte(r.Tombstone)
	if r.Tombstone != 1 {
		utils.PutUvarint(&buf, uint64(len(r.Value)))
		buf.Write(r.Value)
	}

	return buf.Bytes(), nil
}

// deserializes a compressed Record from the given file, returning the record, total bytes read, and any error.
func deserializeWithCompression(file *os.File, compressionMap map[string]uint64) (*Record, uint64, error) {
	record := &Record{}
	var totalBytesRead uint64 = 0

	keyBuf := make([]byte, 8)
	n, err := io.ReadAtLeast(file, keyBuf, 8)
	if err != nil {
		return nil, totalBytesRead + uint64(n), err
	}
	totalBytesRead += uint64(n)
	key := binary.BigEndian.Uint64(keyBuf)
	keyString := utils.GetKeyByValue(key, compressionMap)
	record.Key = keyString

	crc, bytesRead, err := utils.ReadUvarint(file)
	if err != nil {
		return nil, totalBytesRead, err
	}
	record.Crc = uint32(crc)
	totalBytesRead += bytesRead

	timestamp, bytesRead, err := utils.ReadUvarint(file)
	if err != nil {
		return nil, totalBytesRead, err
	}
	record.Timestamp = timestamp
	totalBytesRead += bytesRead

	tombstoneByte := make([]byte, 1)
	n, err = io.ReadAtLeast(file, tombstoneByte, 1)
	if err != nil {
		return nil, totalBytesRead + uint64(n), err
	}
	totalBytesRead += uint64(n)
	record.Tombstone = tombstoneByte[0]

	if record.Tombstone != 1 {
		valueSize, bytesRead, err := utils.ReadUvarint(file)
		if err != nil {
			return nil, totalBytesRead, err
		}
		record.ValueSize = valueSize
		totalBytesRead += bytesRead

		valueBuf := make([]byte, valueSize)
		n, err := io.ReadFull(file, valueBuf)
		if err != nil {
			return nil, totalBytesRead + uint64(n), err
		}
		totalBytesRead += uint64(n)
		record.Value = valueBuf
	}

	// crcChech := append([]byte(record.Key), record.Value...)
	// if CRC32(crcChech) != record.Crc {
	// 	return nil, totalBytesRead, errors.New("not valid record")
	// }

	return record, totalBytesRead, nil
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
