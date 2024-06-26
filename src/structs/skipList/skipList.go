package skiplist

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
)

type node struct {
	key   string
	val   model.Record
	tower []*node // a collection of forwarded pointers linking the node to subsequent nodes on each corresponding level of the skip list
}

func newNode(key string, val model.Record, maxHeight int32) *node {
	newNode := &node{}
	newNode.key = key
	newNode.val = val
	newNode.tower = make([]*node, maxHeight)
	return newNode
}

type SkipList struct {
	head       *node // starting head node
	height     int   // total number of levels that all nodes are currently occupying
	numOfElems uint64
	maxHeight  uint32
}

func NewSkipList(maxHeight uint32) *SkipList {
	skipList := &SkipList{}
	skipList.head = newNode("", model.Record{}, int32(maxHeight))
	skipList.height = 1
	skipList.numOfElems = 0
	skipList.maxHeight = maxHeight
	return skipList
}

// search algoritham, returns requested node and track of the nodes that we pass through as we descend from level to level
func (skipList *SkipList) search(key string) (*node, []*node) {
	var next *node
	// store of the nodes along the search path, required for insertion and deletion
	var journey []*node = make([]*node, skipList.maxHeight)
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
func (skipList *SkipList) Insert(key string, val model.Record) {
	found, journey := skipList.search(key)

	// if the requested key already exists we can swap its current value for the newly supplied value
	if found != nil && found.val.Tombstone == 1 {
		// update value for existing key
		found.val = val
		return
	}

	height := roll(skipList.maxHeight) //pseudo-random height (defines on how many levels to add new node)

	newNode := newNode(key, val, int32(skipList.maxHeight))
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

// logical deletion
func (skipList *SkipList) Delete(key string) {
	found, _ := skipList.search(key)

	if found == nil {
		return // if a node with the requested key doesn't exist
	}

	found.val.Tombstone = 1
}

// finding the precise value residing at a requested key; returns -1 if key not found and error
func (skipList *SkipList) Find(key string) (model.Record, error) {
	found, _ := skipList.search(key)

	if found == nil {
		return model.Record{}, errors.New("key not found")
	}

	return found.val, nil
}

// capacity is attribute in interface memtable
func (skipList *SkipList) IsFull(capacity uint64) bool {
	return skipList.numOfElems >= capacity
}

func roll(maxHeight uint32) int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= int(maxHeight) {
			return level
		}
	}
	return level
}

func (skipList *SkipList) ClearData() {
	skipList.head = newNode("", model.Record{}, int32(skipList.maxHeight))
	skipList.height = 1
	skipList.numOfElems = 0
}

func (skipList *SkipList) PrintSkipList() {
	fmt.Println("SkipList:")
	for level := int(skipList.maxHeight) - 1; level >= 0; level-- {
		node := skipList.head
		fmt.Printf("Level %d: ", level)
		for node != nil {
			val := "nil"
			if node.val.Tombstone == 1 {
				val = "deleted"
			} else if node.key != "" {
				val = node.key
			}
			fmt.Printf("[%s] ", val)
			node = node.tower[level]
		}
		fmt.Println()
	}
}
