package btree

//Temporary type representing a value stored in the memtable
//Will be changed in the future
//TODO: Change the type when a concrete type for memtable data is made
type MemtableValue struct{}

type BTree struct {
	root   *btree_node //The first node
	order  int         //Maximum number of keys per node
	height int         //Height of the tree
}

type btree_node struct {
	key_count  int             //Number of keys in node
	key_list   []string        //List of keys splitting the node and containing values
	value_list []MemtableValue //Values for keys from the key_list
	is_leaf    bool            //Tells if this node is a leaf (contains only keys and values)
	subtrees   []*btree_node   //Child nodes
	parent     *btree_node     //The parent node
}

//Initializes an empty BTree of the passed order
func NewBTree(order int) BTree {
	var head *btree_node = &btree_node{key_count: 0,
		key_list:   make([]string, order),
		value_list: make([]MemtableValue, order),
		is_leaf:    true,
		subtrees:   make([]*btree_node, order+1),
		parent:     nil,
	}

	return BTree{root: head, order: order, height: 0}
}

//searchNodeWithKey returns the pointer to a tree node that COULD contain the passed key.
//If the key is present in the Tree, the node containing it will be returned.
//Otherwise, a leaf node in which the key could be inserted will be returned.
//The function also returns an integer representing the index of the found key in the key list.
//If the key isn't present in the node, -1 is returned.
func (btree *BTree) searchNodeForKey(key string) (*btree_node, int) {
	//Start from the root
	var next_node *btree_node = btree.root
	var current_node *btree_node

	//Iterate through the tree
	for {
		current_node = next_node
		var key_count int = current_node.key_count

		//Check if the key is in the node
		for i := 0; i < current_node.key_count; i++ {
			if current_node.key_list[i] == key {
				return current_node, i
			}
		}

		//If the key isn't found in a leaf, it doesn't exist in the tree
		if current_node.is_leaf {
			break
		}

		//If the key isn't found in an internal node, traverse the appropriate child node
		next_node = current_node.subtrees[key_count] //Assume the key belongs in the last child node
		for i := 0; i < key_count; i++ {
			if key < current_node.key_list[i] {
				next_node = current_node.subtrees[i]
			}
		}

	}

	return current_node, -1
}

//SearchRef returns a pointer to the value represented by the passed key.
//If the passed key is not present in the tree, nil is returned.
func (btree *BTree) SearchRef(key string) *MemtableValue {
	node_p, index := btree.searchNodeForKey(key)

	if index == -1 {
		return nil
	}

	return &node_p.value_list[index]
}

//Search returns the value represented by the passed key, as well as a boolean indicating if the key exists.
//If the key isn't found, the value returned will be an empty struct.
func (btree *BTree) Search(key string) (MemtableValue, bool) {
	var ref *MemtableValue = btree.SearchRef(key)
	if ref == nil {
		return MemtableValue{}, false
	} else {
		return *ref, true
	}
}

func (btree *BTree) splitNode(node *btree_node, key string, value MemtableValue) {

}

func (btree *BTree) insertIntoNode(node *btree_node, key string, value MemtableValue) {
	if node.key_count == btree.order {
		//If a node already contains the maximum number of keys, it has to be split
		//The reason splitting is a different method is for better readability
		btree.splitNode(node, key, value)
	} else {
		//If a node contains enough space for the key, certain elements might have to be
		//shifted to the right for the key to fit, and the key list to remain sorted

		for index := 0; index < node.key_count; index++ {
			if key < node.key_list[index] {
				//The variable index represents the index of the first key from -
				//the key list larger than the passed key.
				//All keys and values starting from index
				copy(node.key_list[index:], node.key_list[index+1:])
				copy(node.value_list[index:], node.value_list[index+1:])

				node.key_list[index] = key
				node.value_list[index] = value

				node.key_count += 1
				return
			}
		}

		//If no elements had to be shifted, simply append the key and value
		node.key_list[node.key_count] = key
		node.value_list[node.key_count] = value
		node.key_count += 1
	}
}

func (btree *BTree) Insert(key string, value MemtableValue) {
	node, key_index := btree.searchNodeForKey(key)

	//If the key already exists in the tree, just change it's represented value.
	if key_index != -1 {
		node.value_list[key_index] = value
	} else {
		btree.insertIntoNode(node, key, value)
	}

}

func (btree *BTree) Delete(key string) {

}
