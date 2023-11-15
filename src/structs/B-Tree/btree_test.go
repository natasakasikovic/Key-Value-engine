package btree

import "testing"

func tstParentPointers(t *testing.T, node *btree_node) {
	for i := 0; i < node.key_value_list.Size()+1; i++ {
		if !node.is_leaf {
			if node.subtrees[i].parent != node {
				t.Errorf("Parent pointer does not match parent address.")
			}
			tstParentPointers(t, node.subtrees[i])
		}
	}
}

func TestInsert(t *testing.T) {
	var tree BTree = NewBTree(2)
	tree.Insert("1", MemtableValue{})
	tree.Insert("2", MemtableValue{})
	tree.Insert("3", MemtableValue{})
	tree.Insert("4", MemtableValue{})
	tree.Insert("5", MemtableValue{})
	tree.Insert("6", MemtableValue{})
	tree.Insert("7", MemtableValue{})
	tree.Insert("8", MemtableValue{})
	tree.Insert("9", MemtableValue{})
	tree.Insert("10", MemtableValue{})
	// tstParentPointers(t, tree.root)
	tree.Insert("11", MemtableValue{})
	tree.Insert("asd", MemtableValue{})
	tree.Insert("ge", MemtableValue{})
	tree.Insert("30", MemtableValue{})
	tree.Insert("1030", MemtableValue{})
	tree.Insert("1asd", MemtableValue{})
	tree.Insert("mag", MemtableValue{})
	tree.Insert("test", MemtableValue{})
	tree.Insert("oke", MemtableValue{})
	tstParentPointers(t, tree.root)
}
