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
	key_value_list kv_vector
	is_leaf        bool          //Tells if this node is a leaf (contains only keys and values)
	subtrees       []*btree_node //Child nodes
	parent         *btree_node   //The parent node
}

//Initializes an empty BTree of the passed order
func NewBTree(order int) BTree {
	var head *btree_node = &btree_node{key_value_list: kv_vector{},
		is_leaf:  true,
		subtrees: make([]*btree_node, order+2),
		parent:   nil,
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
		var key_count int = current_node.key_value_list.Size()

		//Check if the key is in the node
		for i := 0; i < key_count; i++ {
			if current_node.key_value_list.GetKeyAt(i) == key {
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
			if key < current_node.key_value_list.GetKeyAt(i) {
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

	return node_p.key_value_list.GetValueReferenceAt(index)
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

//Returns the median key and the value associated with it of a certain node.
func (btree *BTree) mediansOfNode(node *btree_node) (string, MemtableValue) {
	var median_index int = node.key_value_list.Size() / 2
	return node.key_value_list.Get(median_index)
}

//Returns the left and right nodes created from splitting an existing node.
func (btree *BTree) newNodesFromSplit(node *btree_node) (*btree_node, *btree_node) {
	var left_node *btree_node = &btree_node{parent: node.parent,
		key_value_list: kv_vector{},
		is_leaf:        node.is_leaf,
		subtrees:       make([]*btree_node, btree.order+2)}
	var right_node *btree_node = &btree_node{parent: node.parent,
		key_value_list: kv_vector{},
		is_leaf:        node.is_leaf,
		subtrees:       make([]*btree_node, btree.order+2)}

	var median_index int = node.key_value_list.Size() / 2

	for i := 0; i < median_index; i++ {
		key, val := node.key_value_list.Get(i)
		left_node.key_value_list.PushBack(key, val)
	}

	for i := median_index + 1; i < node.key_value_list.Size(); i++ {
		key, val := node.key_value_list.Get(i)
		right_node.key_value_list.PushBack(key, val)
	}

	//Assign subtrees to appropriate nodes
	for i := 0; i <= median_index; i++ {
		left_node.subtrees[i] = node.subtrees[i]
	}

	for i := median_index + 1; i <= btree.order+1; i++ {
		right_node.subtrees[i-median_index-1] = node.subtrees[i]
	}

	return left_node, right_node
}

func (btree *BTree) newRootNode() *btree_node {
	btree.root = &btree_node{key_value_list: kv_vector{},
		is_leaf:  false,
		subtrees: make([]*btree_node, btree.order+2),
		parent:   nil,
	}
	return btree.root
}

func (btree *BTree) splitNode(node *btree_node) {
	med_key, med_value := btree.mediansOfNode(node)
	left_tree, right_tree := btree.newNodesFromSplit(node)

	//If 'node' is the root node, we need to create a new root.
	if node.parent == nil {
		node.parent = btree.newRootNode()
	}

	var new_med_index int = btree.insertIntoNode(node.parent, med_key, med_value)
	node.parent.subtrees[new_med_index] = left_tree
	node.parent.subtrees[new_med_index+1] = right_tree

	left_tree.parent = node.parent
	right_tree.parent = node.parent

	btree.checkNodeValidity(node.parent)
}

// To be called after inserting a key-value into a node.
// Checks whether a node contains too much keys and needs to be split.
func (btree *BTree) checkNodeValidity(node *btree_node) {
	if node.key_value_list.Size() > btree.order {
		btree.splitNode(node)
	}
}

//Inserts a key-value pair into a node and returns the index it was inserted at.
func (btree *BTree) insertIntoNode(node *btree_node, key string, value MemtableValue) int {
	//If a node contains enough space for the key, certain elements might have to be
	//shifted to the right for the key to fit, and the key list to remain sorted

	for index := 0; index < node.key_value_list.Size(); index++ {
		if key < node.key_value_list.GetKeyAt(index) {
			//The variable index represents the index of the first key from the -
			//key list larger than the passed key.
			node.key_value_list.Insert(uint(index), key, value)
			return index
		}
	}

	//If no elements had to be shifted, simply append the key and value
	node.key_value_list.PushBack(key, value)
	return node.key_value_list.Size() - 1
}

func (btree *BTree) Insert(key string, value MemtableValue) {
	node, key_index := btree.searchNodeForKey(key)

	//If the key already exists in the tree, just change it's represented value.
	if key_index != -1 {
		btree.root.key_value_list.SetValueAt(key_index, value)
	} else {
		btree.insertIntoNode(node, key, value)
		btree.checkNodeValidity(node)
	}

}

func (btree *BTree) Delete(key string) {

}
