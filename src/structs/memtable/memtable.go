package memtable

import (
	"errors"
	"sort"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	bTree "github.com/natasakasikovic/Key-Value-engine/src/structs/B-Tree"
	hashMap "github.com/natasakasikovic/Key-Value-engine/src/structs/hashMap"
	skiplist "github.com/natasakasikovic/Key-Value-engine/src/structs/skipList"
)

type DataStructure interface {
	Insert(key string, value model.Record)
	Delete(key string) //should be logical
	IsFull(capacity uint64) bool
	Find(key string) (model.Record, error) //return value of the key
	ClearData()                            //empty data from data structure
}

var Memtables = struct {
	size       uint
	current    uint
	flush      uint
	collection []*Memtable
}{size: 0, current: 0, flush: 0, collection: nil}

type Memtable struct {
	data     DataStructure
	capacity uint64
	Keys     []string
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
func InitMemtables(memtable_size uint64, memtable_structure string, num_of_instances uint64, b_tree_order uint32) {
	Memtables.collection = make([]*Memtable, num_of_instances)
	Memtables.size = uint(num_of_instances)

	switch memtable_structure {
	case "skipList":
		for i := 0; i < int(num_of_instances); i++ {
			Memtables.collection[i] = NewMemtable(skiplist.NewSkipList(), memtable_size)
		}
	case "bTree":
		for i := 0; i < int(num_of_instances); i++ {
			Memtables.collection[i] = NewMemtable(bTree.NewBTree(int(b_tree_order)), memtable_size)
		}
	case "hashMap":
		for i := 0; i < int(num_of_instances); i++ {
			Memtables.collection[i] = NewMemtable(hashMap.NewHashMap(), memtable_size)
		}
	default:
		for i := 0; i < int(num_of_instances); i++ {
			Memtables.collection[i] = NewMemtable(hashMap.NewHashMap(), memtable_size)
		}
	}

}

func (memtable *Memtable) delete(key string) {
	memtable.data.Delete(key)
}

func find(key string) (model.Record, error) {
	return Memtables.collection[Memtables.current].data.Find(key)
}

func findAndDelete(key string) bool {
	for _, memtable := range Memtables.collection {
		_, err := memtable.data.Find(key)
		if err == nil {
			memtable.delete(key)
			return true
		}
	}
	return false
}

func Put(key string, value []byte, timestamp uint64, tombstone byte) {
	if tombstone == 1 && findAndDelete(key) {
		return
	}

	memtable := Memtables.collection[Memtables.current] //current memtable

	if memtable.data.IsFull(memtable.capacity) {
		if Memtables.current == Memtables.size-1 { //if current memtable is full and is last
			//do flush
			Memtables.current = Memtables.flush
			Memtables.collection[Memtables.flush].flushToSSTable()
			//empty memtable
			memtable = Memtables.collection[Memtables.flush]
			if Memtables.flush == Memtables.size-1 { //when flushed memtable is last in collection, next for flush is memtable at position 0
				Memtables.flush = 0
			} else {
				Memtables.flush += 1
			}
			memtable.data.ClearData()
			memtable.Keys = nil
		} else {
			Memtables.current += 1
			memtable = Memtables.collection[Memtables.current]
			if memtable.data.IsFull(memtable.capacity) { //If there is data, flush it; we don't want to overwrite it
				memtable.flushToSSTable()
				if Memtables.flush == Memtables.size-1 { //when flushed memtable is last in collection, next for flush is memtable at position 0
					Memtables.flush = 0
				} else {
					Memtables.flush += 1
				}
				memtable.data.ClearData()
				memtable.Keys = nil
			}
		}

	}
	memValue := model.Record{
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}
	//put data to memtable
	memtable.data.Insert(key, memValue)
	memtable.Keys = append(memtable.Keys, key)

}

func Get(key string) (model.Record, error) {
	for _, memtable := range Memtables.collection {
		record, err := memtable.data.Find(key)
		if err == nil {
			return record, nil
		}
	}
	return model.Record{}, errors.New("record not found")
}

func (memtable *Memtable) flushToSSTable() {
	var records []*model.Record
	sort.Strings(memtable.Keys)
	for _, key := range memtable.Keys {
		record, err := find(key)
		if err == nil {
			records = append(records, &record)
		}
	}
	// CreateSStable(records,singleFile, compressionOn, indexDegree, summaryDegree) //uncomment this line and call CreateSStable when merge sstable branch to develop
}
