package skiplist

import (
	"errors"
	"math/rand"
	"strings"
)

// NOTE: skip list and b-tree must support the same methods (which have the same name)
// TODO: Change the type when a concrete type for memtable data is made
type MemtableValue struct{}

const (
	maxHeight = 16 // applied in the original skip list
)

type node struct {
	key   string
	val   MemtableValue
	tower [maxHeight]*node // a collection of forwarded pointers linking the node to subsequent nodes on each corresponding level of the skip list
}

type SkipList struct {
	head       *node // starting head node
	height     int   // total number of levels that all nodes are currently occupying
	numOfElems int
}

func NewSkipList() *SkipList {
	skipList := &SkipList{}
	skipList.head = &node{}
	skipList.height = 1
	skipList.numOfElems = 1
	return skipList
}

// search algoritham, returns requested node and track of the nodes that we pass through as we descend from level to level
func (skipList *SkipList) search(key string) (*node, [maxHeight]*node) {
	var next *node
	var journey [maxHeight]*node // store of the nodes along the search path, required for insertion and deletion

	prev := skipList.head
	// iteratie through the heights of the skip list, starting from the top and going down
	for level := skipList.height - 1; level >= 0; level-- {
		// iterate through nodes at the current level (level) from the current node (prev) towards the head node
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			if strings.Compare(key, next.key) <= 0 {
				break // move down to a lower level
			}
			prev = next
		}
		journey[level] = prev
	}
	if next != nil && (key == next.key) {
		return next, journey
	}
	return nil, journey
}

// inserting a new node
func (skipList *SkipList) Insert(key string, val MemtableValue) {
	found, journey := skipList.search(key)

	// if the requested key already exists we can swap its current value for the newly supplied value
	if found != nil {
		// update value for existing key
		found.val = val
		skipList.numOfElems -= 1
		return
	}

	height := roll() //pseudo-random height (defines on how many levels to add new node)

	newNode := &node{key: key, val: val} // creating new node

	// go through the skip list to the level where we insert a new node
	for level := 0; level < height; level++ {
		prev := journey[level] // to determine the node neighbors and splice the node with them

		if prev == nil {
			// prev is nil if we are extending the height of the skip list,
			// becaues that level did not exist while the journey was being recorded
			prev = skipList.head
		}
		newNode.tower[level] = prev.tower[level]
		prev.tower[level] = newNode
	}

	// make sure that the correct height is reflected after a potential expansion has occurred
	if height > skipList.height {
		skipList.height = height
	}
	skipList.numOfElems += 1
}

// deleting a node;
// TODO: implement logical delete instead of permanent
func (skipList *SkipList) Delete(key string) { //returns true if the deletion was successful, false otherwise.
	found, journey := skipList.search(key)

	if found == nil {
		return // if a node with the requested key doesn't exist
	}

	// TODO: set attribute Tombstone on true (1) instead of of binding the pointers (loop is unnecessary)
	for level := 0; level < skipList.height; level++ {
		// we backtrack the journey array, looking for the nodes neighboring the node requested for delition
		if journey[level].tower[level] != found {
			break
		}
		// split the node request for delition from its neighbors and splice its former neighbors together
		journey[level].tower[level] = found.tower[level]
		found.tower[level] = nil
	}
	found = nil
	skipList.shrink() // adjust the new height if necessary
}

// TODO: delete this method when you enable logical delete
// make sure the correct height is reflected after possible reduction
func (skipList *SkipList) shrink() {
	for level := skipList.height - 1; level >= 0; level-- {
		if skipList.head.tower[level] == nil {
			skipList.height--
		}
	}
}

// finding the precise value residing at a requested key; returns -1 if key not found and error
func (skipList *SkipList) Find(key string) (MemtableValue, error) {
	found, _ := skipList.search(key)

	if found == nil {
		return MemtableValue{}, errors.New("key not found")
	}

	return found.val, nil
}

// capacity is attribute in interface memtable
func (skipList *SkipList) IsFull(capacity int) bool {
	return skipList.numOfElems >= capacity
}

func roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= maxHeight {
			return level
		}
	}
	return level
}