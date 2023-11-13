package btree

//Temporary type representing a value stored in the memtable
//Will be changed in the future
//TODO: Change when a concrete type is defined
type MemtableValue struct{}

type BTree struct {
	root   *btree_node //The first node
	order  int         //Maximum number of subtrees
	length int         //Depth of tree
}

type btree_node struct {
	key_count     int             //Number of keys in node
	key_list      []string        //List of keys splitting the node and containing values
	value_list    []MemtableValue //Values for keys from the key_list
	subtree_count int
	subtrees      []*btree_node //Child nodes
}

func (btree *BTree) Insert(key string, value MemtableValue) {

}

func (btree *BTree) Delete(key string, value MemtableValue) {

}
