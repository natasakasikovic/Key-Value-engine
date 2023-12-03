package merkletree

import (
	"crypto/sha1"
	"encoding/hex"
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

func NewTree(content [][]byte) *MerkleTree {
	leaves := buildLeaves(content)
	root := buildTree(leaves)
	return &MerkleTree{
		Root:   root,
		leaves: leaves,
	}
}

func buildLeaves(content [][]byte) []*Node {
	var leaves []*Node
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
		empty := []byte{}
		leaves = append(leaves, &Node{
			data:   Hash(empty),
			left:   nil,
			right:  nil,
			parent: nil,
		})
	}
	return leaves
}

func buildTree(nodes []*Node) *Node {
	length := len(nodes)
	var nodeList []*Node
	for i := 0; i < length; i += 2 {
		firstNode := nodes[i]
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

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}
