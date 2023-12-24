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

//Call when a node is no longer needed, so it can be removed from memory by the garbage collector.
func (node *btree_node) freeMemory() {
	node.parent = nil
	for i := 0; i < node.key_value_list.Size()+1; i++ {
		node.subtrees[i] = nil
	}
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
func (btree *BTree) Find(key string) (MemtableValue, bool) {
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

	//Assign subtrees to appropriate nodes if the split node is not a leaf
	if !node.is_leaf {
		for i := 0; i <= median_index; i++ {
			left_node.subtrees[i] = node.subtrees[i]
			left_node.subtrees[i].parent = left_node
		}

		for i := median_index + 1; i <= btree.order+1; i++ {
			right_node.subtrees[i-median_index-1] = node.subtrees[i]
			right_node.subtrees[i-median_index-1].parent = right_node
		}
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

//Splits a node if it contains too many key-value pairs
func (btree *BTree) splitNode(node *btree_node) {
	med_key, med_value := btree.mediansOfNode(node)
	left_tree, right_tree := btree.newNodesFromSplit(node)

	//If 'node' is the root node, we need to create a new root.
	if node.parent == nil {
		node.parent = btree.newRootNode()
		btree.height++
	}

	var new_med_index int = btree.insertIntoNode(node.parent, med_key, med_value)
	//Shift the existing subtrees to the right by one
	copy(node.parent.subtrees[new_med_index+2:], node.parent.subtrees[new_med_index+1:])

	node.parent.subtrees[new_med_index] = left_tree
	node.parent.subtrees[new_med_index+1] = right_tree

	left_tree.parent = node.parent
	right_tree.parent = node.parent

	node.freeMemory()

	btree.checkNodeOverflow(left_tree.parent)
}

// To be called after inserting a key-value into a node.
// Checks whether a node contains too many keys and needs to be split.
func (btree *BTree) checkNodeOverflow(node *btree_node) {
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
		btree.checkNodeOverflow(node)
	}
}

//Returns the nodes right sibling, as well as the index of the key seperating them
func (node *btree_node) getRightSibling() (*btree_node, int) {
	if node.parent == nil {
		return nil, -1
	}
	var parent *btree_node = node.parent
	for i := 0; i < parent.key_value_list.Size(); i++ {
		if parent.subtrees[i] == node {
			return parent.subtrees[i+1], i
		}
	}

	return nil, -1
}

//Return the nodes left sibling, as well as the index of the key seperating them
func (node *btree_node) getLeftSibling() (*btree_node, int) {
	if node.parent == nil {
		return nil, -1
	}
	var parent *btree_node = node.parent
	for i := 1; i < parent.key_value_list.Size()+1; i++ {
		if parent.subtrees[i] == node {
			return parent.subtrees[i-1], i - 1
		}
	}
	return nil, -1
}

func (btree *BTree) rotateLeft(node *btree_node, right_sibling *btree_node, separator_index int) {
	//The deficient node steals the separator from his parent and is no longer deficient
	parent := node.parent
	node.key_value_list.PushBack(parent.key_value_list.Get(separator_index))
	//The parent loses the separator and replaces it with the leftmost key-value pair of the right sibling
	key, value := right_sibling.key_value_list.Get(0)
	parent.key_value_list.Set(separator_index, key, value)
	btree.deleteFromNode(right_sibling, 0)
}

func (btree *BTree) rotateRight(node *btree_node, left_sibling *btree_node, separator_index int) {
	//The deficient node steals the separator from his parent ans is no longer deficient
	parent := node.parent
	p_key, p_val := parent.key_value_list.Get(separator_index)
	node.key_value_list.Insert(0, p_key, p_val)
	//The parent loses the separator and replaces it with the right-most key-value pair of the left sibling
	key, value := left_sibling.key_value_list.Get(left_sibling.key_value_list.Size() - 1)
	parent.key_value_list.Set(separator_index, key, value)
	btree.deleteFromNode(left_sibling, left_sibling.key_value_list.Size()-1)
}

func (btree *BTree) rotateNode(node *btree_node) {
	//If the right sibling has more than the minimum amount of key-values, we can rotate one from him
	right_sibling, right_separator := node.getRightSibling()
	if right_sibling != nil {
		if right_sibling.key_value_list.Size() > btree.order/2 {
			btree.rotateLeft(node, right_sibling, right_separator)
			return
		}
	}
	//If the left sibling has more than the minimum amount of key-values, we can rotate one from him
	left_sibling, left_separator := node.getLeftSibling()
	if left_sibling != nil {
		if left_sibling.key_value_list.Size() > btree.order/2 {
			btree.rotateRight(node, left_sibling, left_separator)
			return
		}
	}

	//If neither of them has more than the minimum amount of key-values, we can merge with one of the siblings
	//Find out which node is the left-most one
	var parent *btree_node = node.parent
	var left_node, right_node *btree_node = node, right_sibling
	var separator_index int = right_separator

	if right_sibling == nil {
		left_node, right_node = left_sibling, node
		separator_index = left_separator
	}

	//Move the separator to the end of the left node
	var num_of_subtrees int = left_node.key_value_list.Size() + 1
	left_node.key_value_list.PushBack(parent.key_value_list.Get(separator_index))
	//Copy all the keys from the right node to the end of the left node
	for i := 0; i < right_node.key_value_list.Size(); i++ {
		left_node.key_value_list.PushBack(right_node.key_value_list.Get(i))
	}

	//Copy all the subtrees
	if !node.is_leaf {
		for i := 0; i < right_node.key_value_list.Size()+1; i++ {
			left_node.subtrees[num_of_subtrees+i] = right_node.subtrees[i]
			right_node.subtrees[i].parent = left_node
			right_node.subtrees[i] = nil
		}
	}

	//Shift all the parent separators and subtrees after the separator to the left
	parent.key_value_list.Delete(uint(separator_index))
	copy(parent.subtrees[separator_index+1:], parent.subtrees[separator_index+2:])

	//Since the parent node lost a key-value pair, we need to check if it's deficient.
	btree.checkNodeUnderflow(parent)
}

//Check if an underflow occured at the passed node.
//That is, check whether or not an internal node has at least the minimum number of keys.
//If it does not, the tree needs rebalancing, unless it is the root node.
func (btree *BTree) checkNodeUnderflow(node *btree_node) {
	//The root node is an exception as it can have less than the minimum number of keys.
	//However if it has 0 keys, then it is empty and a new root is needed.
	if (node == btree.root) && (node.key_value_list.Size() == 0) {
		btree.height--

		//If the root has a child, it will become the new root.
		//Otherwise the root, and the whole tree will remain empty.
		if node.subtrees[0] != nil {
			btree.root = node.subtrees[0]
			node.subtrees[0].parent = nil
		}
		return
	}

	//If a non-root node has less than the minimum amount of keys, rotate.
	if (node.key_value_list.Size() < btree.order/2) && (node != btree.root) {
		btree.rotateNode(node)
	}
}

func (btree *BTree) deleteFromNode(node *btree_node, index int) {
	if node.is_leaf {
		//If an element from a leaf was deleted, we need to check if an underflow occured
		node.key_value_list.Delete(uint(index))
		btree.checkNodeUnderflow(node)
	} else {
		//If we're deleting an element from an internal node, we simply need to replace
		//the key-value pair being deleted with the smallest key-value pair larger than it
		//AKA the leftmost key-value pair of the left-most leaf of the right subtree.
		//This is because every key-value pair contained in the right subtree is larger than
		//the key-value pair being deleted.
		//The search for the smallest key-value pair larger than the pair being
		//deleted starts from the right subtree.
		var successor_child *btree_node = node.subtrees[index+1]
		//We need to iteratively get to the leftmost leaf.
		for !successor_child.is_leaf {
			successor_child = successor_child.subtrees[0]
		}
		//Replace the deleted key-value pair with the appropriate values.
		key, value := successor_child.key_value_list.Get(0)
		node.key_value_list.Set(index, key, value)
		//Delete the key-value pair from the child.
		btree.deleteFromNode(successor_child, 0)
	}
}

func (btree *BTree) Delete(key string) {
	node, key_index := btree.searchNodeForKey(key)

	if key_index != -1 {
		btree.deleteFromNode(node, key_index)
	}
}

func (btree *BTree) ClearData() {
	btree.root = nil
	btree.height = 0
}
