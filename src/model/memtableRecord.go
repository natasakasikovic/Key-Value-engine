package model

type MemtableRecord struct {
	Value     []byte
	Tombstone byte
	Timestamp uint64
}
