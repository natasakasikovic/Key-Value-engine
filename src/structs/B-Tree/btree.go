package btree

//Temporary type representing a value stored in the memtable
//Will be changed in the future
//TODO: Change the type when a concrete type for memtable data is made
type MemtableValue struct{}

type BTree struct {
	root   *btree_node //The first node
	order  int         //Maximum number of keys per node
	length int         //Depth of the tree
}

type btree_node struct {
	key_count  int             //Number of keys in node
	key_list   []string        //List of keys splitting the node and containing values
	value_list []MemtableValue //Values for keys from the key_list
	is_leaf    bool            //Tells if this node is a leaf (contains only keys and values)
	subtrees   []*btree_node   //Child nodes
}

//Initializes an empty BTree of the passed order
func NewBTree(order int) BTree {
	return BTree{root: nil, order: order, length: 0}
}

//SearchRef returns a pointer to the value mapped to a certain key
//If the key does not have an associated value, nil is returned
func (btree *BTree) SearchRef(key string) *MemtableValue {
	//Start from the root
	var current_node *btree_node = btree.root

	//Iterate through the tree
	for current_node != nil {
		var key_count int = current_node.key_count

		//Check if the key is in the node
		for i := 0; i < current_node.key_count; i++ {
			if current_node.key_list[i] == key {
				return &current_node.value_list[i]
			}
		}

		//If the key isn't found in a leaf node, it doesn't exist in the tree
		if current_node.is_leaf {
			return nil
		}

		//If the key isn't found in an internal node, traverse the appropriate child node
		var next_node *btree_node = current_node.subtrees[key_count] //Assume the key belongs in the last child node
		for i := 0; i < key_count; i++ {
			if key < current_node.key_list[i] {
				next_node = current_node.subtrees[i]
			}
		}

		current_node = next_node
	}

	return nil
}

//Search returns the value mapped to a certain key, as well as a boolean indicating if the key exists
//If the key isn't found, the value returned will be an empty struct
func (btree *BTree) Search(key string) (MemtableValue, bool) {
	var ref *MemtableValue = btree.SearchRef(key)
	if ref == nil {
		return MemtableValue{}, false
	} else {
		return *ref, true
	}
}

func (btree *BTree) Insert(key string, value MemtableValue) {

}

func (btree *BTree) Delete(key string) {

}
