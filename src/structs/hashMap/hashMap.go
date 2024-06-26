package hashmap

import (
	"errors"

	model "github.com/natasakasikovic/Key-Value-engine/src/model"
)

type HashMap struct {
	data map[string]*model.Record
}

func (hashMap *HashMap) IsFull(capacity uint64) bool {
	return len(hashMap.data) >= int(capacity)
}

func NewHashMap() *HashMap {

	return &HashMap{
		data: make(map[string]*model.Record),
	}
}
func (hashMap *HashMap) Insert(key string, value model.Record) {
	hashMap.data[key] = &value
}
func (hashMap *HashMap) Delete(key string) {
	_, exists := hashMap.data[key]
	if exists {
		hashMap.data[key].Tombstone = 1
	}

}

func (hashMap *HashMap) Find(key string) (model.Record, error) {
	_, exists := hashMap.data[key]
	if exists {
		return *hashMap.data[key], nil
	}
	return model.Record{}, errors.New("record not found")
}

func (hashMap *HashMap) ClearData() {
	hashMap.data = make(map[string]*model.Record)
}
