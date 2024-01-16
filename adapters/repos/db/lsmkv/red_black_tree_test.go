//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
)

const (
	R = true
	B = false
)

// This test adds keys to the RB tree. Afterwards the same nodes are added in the expected order, eg in the way
// the RB tree is expected to re-order the nodes
var rbTests = []struct {
	name           string
	keys           []uint
	ReorderedKeys  []uint
	expectedColors []bool // with respect to the original keys
}{
	{
		"Requires recoloring but no reordering",
		[]uint{61, 52, 83, 93},
		[]uint{61, 52, 83, 93},
		[]bool{B, B, B, R},
	},
	{
		"Requires left rotate around root",
		[]uint{61, 83, 99},
		[]uint{83, 61, 99},
		[]bool{R, B, R},
	},
	{
		"Requires left rotate with more nodes",
		[]uint{61, 52, 85, 93, 99},
		[]uint{61, 52, 93, 85, 99},
		[]bool{B, B, R, B, R},
	},
	{
		"Requires right and then left rotate",
		[]uint{61, 52, 85, 93, 87},
		[]uint{61, 52, 87, 85, 93},
		[]bool{B, B, R, R, B},
	},
	{
		"Requires right rotate around root",
		[]uint{61, 30, 10},
		[]uint{30, 10, 61},
		[]bool{R, B, R},
	},
	{
		"Requires right rotate with more nodes",
		[]uint{61, 52, 85, 21, 10},
		[]uint{61, 85, 21, 10, 52},
		[]bool{B, R, B, B, R},
	},
	{
		"Requires left and then right rotate",
		[]uint{61, 52, 85, 21, 36},
		[]uint{61, 85, 36, 21, 52},
		[]bool{B, R, B, R, B},
	},
	{
		"Require reordering for two nodes",
		[]uint{61, 52, 40, 85, 105, 110},
		[]uint{52, 40, 85, 61, 105, 110},
		[]bool{B, B, B, R, B, R},
	},
	{
		"Ordered nodes increasing",
		[]uint{1, 2, 3, 4, 5, 6, 7, 8},
		[]uint{4, 2, 6, 1, 3, 5, 7, 8},
		[]bool{B, R, B, B, B, R, B, R},
	},
	{
		"Ordered nodes decreasing",
		[]uint{8, 7, 6, 5, 4, 3, 2, 1},
		[]uint{5, 3, 7, 2, 4, 6, 8, 1},
		[]bool{B, R, B, B, B, R, B, R},
	},
	{
		"Multiple rotations along the tree and colour changes",
		[]uint{166, 92, 33, 133, 227, 236, 71, 183, 18, 139, 245, 161},
		[]uint{166, 92, 227, 33, 139, 183, 236, 18, 71, 133, 161, 245},
		[]bool{B, R, B, R, R, B, R, B, R, B, R, R},
	},
}

func TestRBTree(t *testing.T) {
	for _, tt := range rbTests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &binarySearchTree{}
			for _, key := range tt.keys {
				iByte := []byte{uint8(key)}
				tree.insert(iByte, iByte, nil)
				require.Empty(t, tree.root.parent)
			}
			validateRBTree(t, tree.root)

			flattenTree := tree.flattenInOrder()
			require.Equal(t, len(tt.keys), len(flattenTree)) // no entries got lost

			// add tree with the same nodes in the "optimal" order to be able to compare their order afterwards
			treeCorrectOrder := &binarySearchTree{}
			for _, key := range tt.ReorderedKeys {
				iByte := []byte{uint8(key)}
				treeCorrectOrder.insert(iByte, iByte, nil)
			}

			flattenTreeInput := treeCorrectOrder.flattenInOrder()
			for i := range flattenTree {
				byteKey := flattenTree[i].key
				originalIndex := getIndexInSlice(tt.keys, byteKey)
				require.Equal(t, byteKey, flattenTreeInput[i].key)
				require.Equal(t, flattenTree[i].colourIsRed, tt.expectedColors[originalIndex])
			}
		})
	}
}

func TestRBTreeMap(t *testing.T) {
	for _, tt := range rbTests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &binarySearchTreeMap{}
			for _, key := range tt.keys {
				tree.insert([]byte{uint8(key)}, MapPair{
					Key:   []byte("map-key-1"),
					Value: []byte("map-value-1"),
				})
				require.Empty(t, tree.root.parent)
			}
			validateRBTree(t, tree.root)

			flatten_tree := tree.flattenInOrder()
			require.Equal(t, len(tt.keys), len(flatten_tree)) // no entries got lost

			// add tree with the same nodes in the "optimal" order to be able to compare their order afterwards
			treeCorrectOrder := &binarySearchTreeMap{}
			for _, key := range tt.ReorderedKeys {
				treeCorrectOrder.insert([]byte{uint8(key)}, MapPair{
					Key:   []byte("map-key-1"),
					Value: []byte("map-value-1"),
				})
			}

			flatten_tree_input := treeCorrectOrder.flattenInOrder()
			for i := range flatten_tree {
				byte_key := flatten_tree[i].key
				originalIndex := getIndexInSlice(tt.keys, byte_key)
				require.Equal(t, byte_key, flatten_tree_input[i].key)
				require.Equal(t, flatten_tree[i].colourIsRed, tt.expectedColors[originalIndex])
			}
		})
	}
}

func TestRBTreeMulti(t *testing.T) {
	for _, tt := range rbTests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &binarySearchTreeMulti{}
			for _, key := range tt.keys {
				values := []value{}
				for j := uint(0); j < 5; j++ {
					values = append(values, value{value: []byte{uint8(key * j)}, tombstone: false})
				}
				tree.insert([]byte{uint8(key)}, values)
				require.Empty(t, tree.root.parent)
			}
			validateRBTree(t, tree.root)

			flatten_tree := tree.flattenInOrder()
			require.Equal(t, len(tt.keys), len(flatten_tree)) // no entries got lost

			// add tree with the same nodes in the "optimal" order to be able to compare their order afterwards
			treeCorrectOrder := &binarySearchTreeMulti{}
			for _, key := range tt.ReorderedKeys {
				values := []value{}
				for j := uint(0); j < 5; j++ {
					values = append(values, value{value: []byte{uint8(key * j)}, tombstone: false})
				}
				treeCorrectOrder.insert([]byte{uint8(key)}, values)
			}

			flatten_tree_input := treeCorrectOrder.flattenInOrder()
			for i := range flatten_tree {
				byte_key := flatten_tree[i].key
				originalIndex := getIndexInSlice(tt.keys, byte_key)
				require.Equal(t, byte_key, flatten_tree_input[i].key)
				require.Equal(t, flatten_tree[i].colourIsRed, tt.expectedColors[originalIndex])
			}
		})
	}
}

// add keys as a) normal keys b) tombstone keys and c) half tombstone, half normal.
// The resulting (rebalanced) trees must have the same order and colors
var tombstoneTests = []struct {
	name string
	keys []uint
}{
	{"Rotate left around root", []uint{61, 83, 99}},
	{"Rotate right around root", []uint{61, 30, 10}},
	{"Multiple rotations along the tree and colour changes", []uint{166, 92, 33, 133, 227, 236, 71, 183, 18, 139, 245, 161}},
	{"Ordered nodes increasing", []uint{1, 2, 3, 4, 5, 6, 7, 8}},
	{"Ordered nodes decreasing", []uint{8, 7, 6, 5, 4, 3, 2, 1}},
}

func TestRBTrees_Tombstones(t *testing.T) {
	for _, tt := range tombstoneTests {
		t.Run(tt.name, func(t *testing.T) {
			treeNormal := &binarySearchTree{}
			treeTombstone := &binarySearchTree{}
			treeHalfHalf := &binarySearchTree{}
			for i, key := range tt.keys {
				iByte := []byte{uint8(key)}
				treeNormal.insert(iByte, iByte, nil)
				treeTombstone.setTombstone(iByte, nil)
				if i%2 == 0 {
					treeHalfHalf.insert(iByte, iByte, nil)
				} else {
					treeHalfHalf.setTombstone(iByte, nil)
				}
			}
			validateRBTree(t, treeNormal.root)
			validateRBTree(t, treeTombstone.root)
			validateRBTree(t, treeHalfHalf.root)

			treeNormalFlatten := treeNormal.flattenInOrder()
			treeTombstoneFlatten := treeTombstone.flattenInOrder()
			treeHalfHalfFlatten := treeHalfHalf.flattenInOrder()
			require.Equal(t, len(tt.keys), len(treeNormalFlatten))
			require.Equal(t, len(tt.keys), len(treeTombstoneFlatten))
			require.Equal(t, len(tt.keys), len(treeHalfHalfFlatten))

			for i := range treeNormalFlatten {
				require.Equal(t, treeNormalFlatten[i].key, treeTombstoneFlatten[i].key)
				require.Equal(t, treeNormalFlatten[i].key, treeHalfHalfFlatten[i].key)
				require.Equal(t, treeNormalFlatten[i].colourIsRed, treeTombstoneFlatten[i].colourIsRed)
				require.Equal(t, treeNormalFlatten[i].colourIsRed, treeHalfHalfFlatten[i].colourIsRed)
			}
		})
	}
}

type void struct{}

var member void

func mustRandIntn(max int64) int {
	randInt, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(fmt.Sprintf("mustRandIntn error: %v", err))
	}
	return int(randInt.Int64())
}

func TestRBTrees_Random(t *testing.T) {
	tree := &binarySearchTree{}
	amount := mustRandIntn(100000)
	keySize := mustRandIntn(100)
	uniqueKeys := make(map[string]void)
	for i := 0; i < amount; i++ {
		key := make([]byte, keySize)
		rand.Read(key)
		uniqueKeys[fmt.Sprint(key)] = member
		if mustRandIntn(5) == 1 { // add 20% of all entries as tombstone
			tree.setTombstone(key, nil)
		} else {
			tree.insert(key, key, nil)
		}
	}

	// all added keys are still part of the tree
	treeFlattened := tree.flattenInOrder()
	require.Equal(t, len(uniqueKeys), len(treeFlattened))
	for _, entry := range treeFlattened {
		_, ok := uniqueKeys[fmt.Sprint(entry.key)]
		require.True(t, ok)
	}
	validateRBTree(t, tree.root)
}

func TestRBTreesMap_Random(t *testing.T) {
	tree := &binarySearchTreeMap{}
	amount := mustRandIntn(100000)
	keySize := mustRandIntn(100)
	uniqueKeys := make(map[string]void)
	for i := 0; i < amount; i++ {
		key := make([]byte, keySize)
		rand.Read(key)
		uniqueKeys[fmt.Sprint(key)] = member
		tree.insert(key, MapPair{
			Key:   []byte("map-key-1"),
			Value: []byte("map-value-1"),
		})
	}

	// all added keys are still part of the tree
	treeFlattened := tree.flattenInOrder()
	require.Equal(t, len(uniqueKeys), len(treeFlattened))
	for _, entry := range treeFlattened {
		_, ok := uniqueKeys[fmt.Sprint(entry.key)]
		require.True(t, ok)
	}
	validateRBTree(t, tree.root)
}

func TestRBTreesMulti_Random(t *testing.T) {
	tree := &binarySearchTreeMulti{}
	amount := mustRandIntn(100000)
	keySize := mustRandIntn(100)
	uniqueKeys := make(map[string]void)
	for i := 0; i < amount; i++ {
		key := make([]byte, keySize)
		rand.Read(key)
		uniqueKeys[fmt.Sprint(key)] = member
		values := []value{}
		for j := 0; j < 5; j++ {
			values = append(values, value{value: []byte{uint8(i * j)}, tombstone: false})
		}
		tree.insert(key, values)
	}

	// all added keys are still part of the tree
	treeFlattened := tree.flattenInOrder()
	require.Equal(t, len(uniqueKeys), len(treeFlattened))
	for _, entry := range treeFlattened {
		_, ok := uniqueKeys[fmt.Sprint(entry.key)]
		require.True(t, ok)
	}
	validateRBTree(t, tree.root)
}

func getIndexInSlice(reorderedKeys []uint, key []byte) int {
	for i, v := range reorderedKeys {
		if v == uint(key[0]) {
			return i
		}
	}
	return -1
}

// Checks if a tree is a RB tree
//
// There are several properties that valid RB trees follow:
// 1) The root node is always black
// 2) The max depth of a tree is 2* Log2(N+1), where N is the number of nodes
// 3) Every path from root to leave has the same number of _black_ nodes
// 4) Red nodes only have black (or nil) children
//
// In addition this also validates some general tree properties:
//   - root has no parent
//   - if node A is a child of B, B must be the parent of A)
func validateRBTree(t *testing.T, rootNode rbtree.Node) {
	require.False(t, rootNode.IsRed())
	require.True(t, rootNode.Parent().IsNil())

	treeDepth, nodeCount, _ := walkTree(t, rootNode)
	maxDepth := 2 * math.Log2(float64(nodeCount)+1)
	require.True(t, treeDepth <= int(maxDepth))
}

// Walks through the tree and counts the depth, number of nodes and number of black nodes
func walkTree(t *testing.T, node rbtree.Node) (int, int, int) {
	if reflect.ValueOf(node).IsNil() {
		return 0, 0, 0
	}
	leftNode := node.Left()
	leftNodeIsNil := reflect.ValueOf(leftNode).IsNil()
	rightNode := node.Right()
	rightNodeIsNil := reflect.ValueOf(rightNode).IsNil()

	// validate parent/child connections
	if !rightNodeIsNil {
		require.Equal(t, rightNode.Parent(), node)
	}
	if !leftNodeIsNil {
		require.Equal(t, leftNode.Parent(), node)
	}

	// red nodes need black (or nil) children
	if node.IsRed() {
		require.True(t, leftNodeIsNil || !node.Left().IsRed())
		require.True(t, rightNodeIsNil || !node.Left().IsRed())
	}

	blackNode := int(1)
	if node.IsRed() {
		blackNode = 0
	}

	if node.Right().IsNil() && node.Left().IsNil() {
		return 1, 1, blackNode
	}

	depthRight, nodeCountRight, blackNodesDepthRight := walkTree(t, node.Right())
	depthLeft, nodeCountLeft, blackNodesDepthLeft := walkTree(t, node.Left())
	require.Equal(t, blackNodesDepthRight, blackNodesDepthLeft)

	nodeCount := nodeCountLeft + nodeCountRight + 1
	if depthRight > depthLeft {
		return depthRight + 1, nodeCount, blackNodesDepthRight + blackNode
	} else {
		return depthLeft + 1, nodeCount, blackNodesDepthRight + blackNode
	}
}
