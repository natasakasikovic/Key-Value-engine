package merkletree

import (
	"crypto/sha1"
	"errors"
	"fmt"
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
			data:   Hash(c),
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
		fmt.Println(i)
		secondNode := nodes[i+1]
		newNode := Node{
			data:   Hash(append(firstNode.data[:], secondNode.data[:]...)),
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

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func addEmptyNode(nodes []*Node) []*Node {
	empty := []byte{}
	nodes = append(nodes, &Node{
		data:   Hash(empty),
		left:   nil,
		right:  nil,
		parent: nil,
	})
	return nodes
}
