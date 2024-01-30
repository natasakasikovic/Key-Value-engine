package hashmap

import (
	"errors"

	model "github.com/natasakasikovic/Key-Value-engine/src/model"
)

type HashMap struct {
	data map[string]*model.MemtableRecord
}

func (hashMap *HashMap) IsFull(capacity uint64) bool {
	return len(hashMap.data) >= int(capacity)
}

func NewHashMap() *HashMap {

	return &HashMap{
		data: make(map[string]*model.MemtableRecord),
	}
}
func (hashMap *HashMap) Insert(key string, value model.MemtableRecord) {
	hashMap.data[key] = &value
}
func (hashMap *HashMap) Delete(key string) {
	_, exists := hashMap.data[key]
	if exists {
		hashMap.data[key].Tombstone = 1
	}

}

func (hashMap *HashMap) Find(key string) (model.MemtableRecord, error) {
	record, exists := hashMap.data[key]
	if exists && record.Tombstone == 0 {
		return *hashMap.data[key], nil
	}
	return model.MemtableRecord{}, errors.New("record not found")
}

func (hashMap *HashMap) ClearData() {
	hashMap.data = make(map[string]*model.MemtableRecord)
}
