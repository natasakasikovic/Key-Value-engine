package skiplist

import (
	"fmt"
	"math/rand"
	"strings"
)

const (
	maxHeight = 16 // applied in the original skip list, can be changed if necessary

)

type node struct {
	key   string
	val   int
	tower [maxHeight]*node // a collection of forwarded pointers linking the node to subsequent nodes on each corresponding level of the skip list

}

type SkipList struct {
	maxHeight int   // the maximum number of levels
	head      *node // starting head node
	height    int   // total number of levels that all nodes are currently occupying
}

func NewSkipList() *SkipList {
	skipList := &SkipList{}
	skipList.head = &node{}
	skipList.height = 1
	return skipList
}

// search algoritham, returns requested node and track of the nodes that we pass through as we descend from level to level
func (skipList *SkipList) search(key string) (*node, [maxHeight]*node) {
	var next *node
	var journey [maxHeight]*node

	prev := skipList.head
	for level := skipList.height - 1; level >= 0; level-- {
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			if strings.Compare(key, next.key) <= 0 {
				break
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

func (s *SkipList) Delete(key string) {

}

func (s *SkipList) Insert(key string, val int) {

}

func (s *SkipList) Find(key []byte) {

}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func main() {
	s := SkipList{maxHeight: 3}
	for i := 0; i < 10; i++ {
		fmt.Println(s.roll())
	}
}
