package merkletree

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"reflect"
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

// helper - builds leaves from bytes given
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

// VerifyTree function that checks if something changed
// returns empty list if nothing has changed, if something has changed returns indices of leaves that have changed
func (merkle *MerkleTree) VerifyTree(data [][]byte) ([]int, error) {
	otherMerkle, err := NewTree(data)
	var idxNodes []int
	if err != nil {
		return idxNodes, errors.New("there is no sense to check if something changed in tree while content is empty")
	}
	if otherMerkle.Root.data == merkle.Root.data {
		return idxNodes, nil
	} else { // in case roots are not same, we are looking for leaves that have changed
		for i := 0; i < len(merkle.leaves); i++ {
			if merkle.leaves[i].data != otherMerkle.leaves[i].data {
				idxNodes = append(idxNodes, i)
			}
		}
		return idxNodes, nil
	}
}

// function that serializes merkle tree using bfs
func (merkle *MerkleTree) Serialize() []byte {
	nodeData := merkle.bfs()
	var size int = 20 * len(nodeData)
	bytes := make([]byte, size)

	for i, data := range nodeData {
		copy(bytes[i*20:(i+1)*20], data[:])
	}

	return bytes
}

// Deserialize reconstructs a Merkle tree from serialized content
// uses helper - createNode to emilinate redudancy
func Deserialize(content []byte) *MerkleTree {
	var nodes []*Node
	var emptyHash [20]byte = hash([]byte{})
	root := &Node{
		data:   [20]byte(content[0:20]),
		left:   nil,
		right:  nil,
		parent: nil,
	}

	nodes = append(nodes, root)
	nodesToBuild := len(content)/20 - 1
	i := 1

	for nodesToBuild > 0 {
		current := nodes[0]

		current.left = createNode(content, i, current)
		i++
		current.right = createNode(content, i, current)
		i++

		nodes = append(nodes, current.left)

		// if node in tree was added as empty - then it wont have any children and we wont add it to list except in one case - if it is leaf
		if (current.right.data == emptyHash && nodesToBuild == 2) || current.right.data != emptyHash {
			nodes = append(nodes, current.right)
		}

		nodes = nodes[1:]
		nodesToBuild -= 2
	}

	return &MerkleTree{
		Root:   root,
		leaves: nodes,
	}
}

// helper - used in deserialization
func createNode(content []byte, i int, parent *Node) *Node {
	return &Node{
		data:   [20]byte(content[20*i : (i+1)*20]),
		left:   nil,
		right:  nil,
		parent: parent,
	}
}

// helper for serialization
func (merkle *MerkleTree) bfs() [][20]byte {
	q := make([]*Node, 0)
	retVal := make([][20]byte, 0)
	q = append(q, merkle.Root) // add root
	for len(q) != 0 {
		node := q[0]
		if node.left != nil && node.right != nil {
			q = append(q, node.left, node.right)
		}
		retVal = append(retVal, node.data)
		q = q[1:]
	}
	return retVal
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

func TestMerkle() {
	content := [][]byte{[]byte("Data1"), []byte("Data2"), []byte("Data3"), []byte("Data1"), []byte("Data2"), []byte("Data1")}
	merkle, _ := NewTree(content)
	// list, _ := merkle.VerifyTree([][]byte{[]byte("Data1"), []byte("Data2"), []byte("Data5")}) // OK
	// fmt.Println(list)
	bytes := merkle.Serialize() // OK
	fmt.Println(bytes)
	deserialziedMerkle := Deserialize(bytes) // OK
	if reflect.DeepEqual(merkle, deserialziedMerkle) {
		fmt.Println("Trees have same data")
	} else {
		fmt.Println("Trees dont have same data, some error occured")
	}
}