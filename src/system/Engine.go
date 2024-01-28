package system

import (
	"github.com/natasakasikovic/Key-Value-engine/src/structs/LRUCache"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/WAL"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
	"time"
)

type Engine struct {
	wal         *WAL.WAL
	cache       *LRUCache.LRUCache
	bloomFilter *bloomFilter.BloomFilter
}

// Get Checks Memtable, Cache, BloomFilter and SSTable for given key
func (engine *Engine) Get(key string) []byte {
	memtableRecord, err := memtable.Get(key)
	if err == nil {
		return memtableRecord.Value
	}
	value := engine.cache.Get(key)
	if value != nil {
		return value
	}
	//Zavrsiti za SStable
	return nil
}

// Put Adds record to WAL and to Memtable with tombstone 0
func (engine *Engine) Put(key string, value []byte) error {
	timestamp := uint64(time.Now().UnixNano())
	r := WAL.NewRecordTimestamp(0, key, value, timestamp)
	err := engine.wal.Append(r)
	if err != nil {
		return err
	}
	memtable.Put(key, value, timestamp, 0)
	return nil
}

// Delete Adds record to WAL and to Memtable with tombstone 1
func (engine *Engine) Delete(key string) error {
	timestamp := uint64(time.Now().UnixNano())
	r := WAL.NewRecordTimestamp(0, key, make([]byte, 0), timestamp)
	err := engine.wal.Append(r)
	if err != nil {
		return err
	}
	memtable.Put(key, make([]byte, 0), timestamp, 1)
	return nil
}
