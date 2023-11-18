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
	if tree.height != 2 {
		t.Errorf("Tree height is incorrect. It is %d, but it should be 2.", tree.height)
	}
	tstParentPointers(t, tree.root)
}

func TestDelete(t *testing.T) {
	var tree BTree = NewBTree(2)
	tree.Insert("1", MemtableValue{})
	tree.Insert("2", MemtableValue{})
	tree.Insert("3", MemtableValue{})
	tree.Insert("4", MemtableValue{})
	tree.Insert("5", MemtableValue{})
	tree.Insert("6", MemtableValue{})
	tree.Insert("7", MemtableValue{})
	tree.Delete("7")
	if tree.height != 1 {
		t.Errorf("Tree height is incorrect. It is %d, but it should be 1.", tree.height)
	}
	tstParentPointers(t, tree.root)
}

func TestIdeGas(t *testing.T) {
	var tree BTree = NewBTree(2)
	tree.Insert("1", MemtableValue{})
	tree.Insert("2", MemtableValue{})
	tree.Insert("3", MemtableValue{})
	tree.Insert("4", MemtableValue{})
	tree.Insert("5", MemtableValue{})
	tree.Insert("6", MemtableValue{})
	tree.Insert("7", MemtableValue{})
	tree.Delete("4")
}
