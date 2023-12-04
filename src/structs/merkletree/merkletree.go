package merkletree

import (
	"crypto/sha1"
	"errors"
)

type MerkleTree struct {
	Root   *Node
	leaves []*Node
}

type Node struct {
	data   [20]byte
	left   *Node
	right  *Node
	parent *Node
}

func NewTree(content [][]byte) (*MerkleTree, error) {
	leaves, err := buildLeaves(content)

	if err != nil {
		return nil, err
	}

	root := buildTree(leaves)
	return &MerkleTree{
		Root:   root,
		leaves: leaves,
	}, nil
}

// helper function - builds leaves from bytes given
// return value of function (leaves) is used for buildTree function to recursively make tree
// also returns error if there is no content for tree building
func buildLeaves(content [][]byte) ([]*Node, error) {
	var leaves []*Node

	if len(content) == 0 {
		return nil, errors.New("can't build merkle tree if there is no any content")
	}

	for _, c := range content {
		leaves = append(leaves, &Node{
			data:   hash(c),
			left:   nil,
			right:  nil,
			parent: nil,
		})
	}
	// tree needs to be binary, so we add one more leaf if needed
	if len(leaves)%2 != 0 {
		leaves = addEmptyNode(leaves)
	}
	return leaves, nil
}

// makes tree recursively, checks if empty node needs to be added
// this function returns root of built merkle tree
func buildTree(nodes []*Node) *Node {
	length := len(nodes)

	if length%2 == 1 {
		nodes = addEmptyNode(nodes)
	}

	var nodeList []*Node
	for i := 0; i < length; i += 2 {
		firstNode := nodes[i]
		secondNode := nodes[i+1]
		newNode := Node{
			data:   hash(append(firstNode.data[:], secondNode.data[:]...)),
			left:   firstNode,
			right:  secondNode,
			parent: nil,
		}
		firstNode.parent = &newNode
		secondNode.parent = &newNode
		nodeList = append(nodeList, &newNode)
	}

	if len(nodeList) > 1 {
		return buildTree(nodeList)
	}
	return nodeList[0]
}

// ValidateTree function that checks if something changed
// returns empty list if nothing has changed, if something has changed returns indices of leaves that have changed
func (merkle *MerkleTree) ValidateTree(data [][]byte) ([]int, error) {
	otherMerkle, err := NewTree(data)
	var idxNodes []int
	if err != nil {
		return idxNodes, errors.New("there is no sense to check if something changed in tree while content is empty")
	}
	if otherMerkle.Root.data == merkle.Root.data {
		return idxNodes, nil
	} else { // in case roots are not same, we are looking for leaves that have changed
		for i := 0; i < len(merkle.leaves); i++ {
			if merkle.leaves[i] != otherMerkle.leaves[i] {
				idxNodes = append(idxNodes, i)
			}
		}
		return idxNodes, nil
	}
}

// Serialize TODO
func (merkle *MerkleTree) Serialize() {
}

// Deserialize TODO
func Deserialize(bytes []byte) {
}

// TODO helper that will be used for serialization
func (merkle *MerkleTree) breadthFirst() {
}

func hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func addEmptyNode(nodes []*Node) []*Node {
	empty := []byte{}
	nodes = append(nodes, &Node{
		data:   hash(empty),
		left:   nil,
		right:  nil,
		parent: nil,
	})
	return nodes
}
