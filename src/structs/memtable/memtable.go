package memtable

import (
	"fmt"
	"sort"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	bTree "github.com/natasakasikovic/Key-Value-engine/src/structs/B-Tree"
	hashMap "github.com/natasakasikovic/Key-Value-engine/src/structs/hashMap"
	skiplist "github.com/natasakasikovic/Key-Value-engine/src/structs/skipList"
)

type DataStructure interface {
	Insert(key string, value model.MemtableRecord)
	Delete(key string) //should be logical
	IsFull(capacity uint64) bool
	Find(key string) (model.MemtableRecord, error) //return value of the key
	ClearData()                                    //empty data from data structure
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

func Delete(key string) {
	Memtables.collection[Memtables.current].data.Delete(key)
}

func Get(key string) (model.MemtableRecord, error) {
	return Memtables.collection[Memtables.current].data.Find(key)
}
func Put(key string, value []byte, timestamp uint64) {

	memtable := Memtables.collection[Memtables.current] //current memtable

	if memtable.data.IsFull(memtable.capacity) {
		if Memtables.current == Memtables.size-1 { //if current memtable is full and is last
			//do flush
			Memtables.collection[Memtables.flush].FlushToSSTable()
			//empty memtable
			memtable = Memtables.collection[Memtables.flush]
			Memtables.current = Memtables.flush
			if Memtables.flush == Memtables.size-1 { //when flushed memtable is last in collection, next for flush is memtable at position 0
				Memtables.flush = 0
			} else {
				Memtables.flush += 1
			}
			memtable.data.ClearData()
			memtable.keys = nil
		} else {
			Memtables.current += 1
			memtable = Memtables.collection[Memtables.current]
		}

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
