package btree

import "testing"

func TestInsert(t *testing.T) {
	var tree BTree = NewBTree(2)
	tree.Insert("1", MemtableValue{})
	tree.Insert("2", MemtableValue{})
	tree.Insert("3", MemtableValue{})
	tree.Insert("4", MemtableValue{})
	tree.Insert("5", MemtableValue{})
	tree.Insert("6", MemtableValue{})
	tree.Insert("7", MemtableValue{})
}
