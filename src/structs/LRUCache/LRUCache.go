package LRUCache

import (
	"container/list"
	"fmt"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
)

type Data struct {
	key   string
	value []byte
}

func NewData(value []byte, key string) *Data {
	return &Data{value: value, key: key}
}

type LRUCache struct {
	hashMap    map[string]*list.Element
	list       *list.List
	limit      uint32
	numOfElems uint32
}

func NewLRUCache(limit uint32) *LRUCache {
	return &LRUCache{hashMap: make(map[string]*list.Element), list: list.New(), limit: limit, numOfElems: 0}
}

func (lru *LRUCache) Add(key string, value []byte) {
	data := NewData(value, key)
	lru.hashMap[key] = lru.list.PushFront(data)
	lru.numOfElems++
	if lru.numOfElems > lru.limit {
		//Delete from hashmap
		delete(lru.hashMap, lru.list.Back().Value.(*Data).key)
		//Delete from list
		lru.list.Remove(lru.list.Back())
	}
}

func (lru *LRUCache) Get(key string) []byte {
	elem, exists := lru.hashMap[key]
	if !exists {
		return nil
	}
	//Move to front of the list
	lru.list.MoveToFront(elem)
	return elem.Value.(*Data).value
}

func (lru *LRUCache) UpdateKeys(mt *memtable.Memtable) {
	for i := 0; i < len(mt.Keys); i++ {
		elem, exists := lru.hashMap[mt.Keys[i]]
		if exists {
			record, _ := memtable.Get(mt.Keys[i])
			value := record.Value
			elem.Value.(*Data).value = value
		}
	}
}
func (lru *LRUCache) Print() {
	elem := lru.list.Front()
	for elem != nil {
		fmt.Println(elem.Value)
		elem = elem.Next()
	}
}
