package memtable

import (
	"fmt"
	"sort"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
)

type DataStructure interface {
	Insert(key string, value model.MemtableRecord)
	Delete(key string) //should be logical
	IsFull(capacity uint64) bool
	Find(key string) (model.MemtableRecord, error) //return value of the key
	ClearData()                                    //empty data from data structure
}

type Memtable struct {
	data     DataStructure
	capacity uint64
	keys     []string
}

func NewMemtable(data DataStructure, capacity uint64) *Memtable {
	if capacity == 0 {
		capacity = 20
	}

	return &Memtable{
		data:     data,
		capacity: capacity,
	}
}

func (memtable *Memtable) Delete(key string) {
	memtable.data.Delete(key)
}

func (memtable *Memtable) Get(key string) (model.MemtableRecord, error) {
	return memtable.data.Find(key)
}
func (memtable *Memtable) Put(key string, value []byte, timestamp uint64) {

	if memtable.data.IsFull(memtable.capacity) {
		//do flush
		memtable.FlushToSSTable()
		//empty memtable
		memtable.data.ClearData()
		memtable.keys = nil
	}
	memValue := model.MemtableRecord{
		Value:     value,
		Tombstone: 0,
		Timestamp: timestamp,
	}
	//put data to memtable
	memtable.data.Insert(key, memValue)
	memtable.keys = append(memtable.keys, key)

}
func (memtable *Memtable) FlushToSSTable() {
	// sstableData := NewSSTable()	//uncomment this line once the SSTable is implemented
	sort.Strings(memtable.keys)
	for _, key := range memtable.keys {
		fmt.Println(key)
		// sstableData.Put(memtable.Get(key))	//uncomment this line once the SSTable is implemented
	}

}
