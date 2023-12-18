package LRUCache

import "container/list"

type LRUCache struct {
	hashMap    map[string]*list.Element
	list       *list.List
	limit      uint32
	numOfElems uint32
}

func NewLRUCache() *LRUCache {
	return &LRUCache{hashMap: make(map[string]*list.Element), list: list.New()}
}

func (lru *LRUCache) Add(key string, value []byte) {
	lru.hashMap[key] = lru.list.PushFront(value)
	lru.numOfElems++
	if lru.numOfElems > lru.limit {
		lru.list.Remove(lru.list.Back())
	}
}

func (lru *LRUCache) Get(key string) any {
	elem := lru.hashMap[key]
	lru.list.MoveToFront(elem)
	return elem.Value
}
