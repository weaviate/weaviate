//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

// THis file is a copy of adapters/repos/db/lsmkv/red_black_tree_test.go and reuses parts of it.

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// This test adds keys to the RB tree. Afterwards the same nodes are added in the expected order, eg in the way
// the RB tree is expected to re-order the nodes
func TestRbTreesMulti(t *testing.T) {
	for _, tt := range rbTests {
		t.Run(tt.name, func(t *testing.T) {
			tree := &binarySearchTreeMulti{}
			for _, key := range tt.keys {
				iByte := []byte{uint8(key)}
				values := []value{}
				for j := uint(0); j < 5; j++ {
					values = append(values, value{value: []byte{uint8(key * j)}, tombstone: false})
				}
				tree.insert(iByte, values)
				require.Empty(t, tree.root.parent)
			}
			ValidateRBTreeMulti(t, tree)

			flatten_tree := tree.flattenInOrder()
			require.Equal(t, len(tt.keys), len(flatten_tree)) // no entries got lost

			// add tree with the same nodes in the "optimal" order to be able to compare their order afterwards
			treeCorrectOrder := &binarySearchTreeMulti{}
			for _, key := range tt.ReorderedKeys {
				iByte := []byte{uint8(key)}
				values := []value{}
				for j := uint(0); j < 5; j++ {
					values = append(values, value{value: []byte{uint8(key * j)}, tombstone: false})
				}

				treeCorrectOrder.insert(iByte, values)
			}

			flatten_tree_input := treeCorrectOrder.flattenInOrder()
			for i := range flatten_tree {
				byte_key := flatten_tree[i].key
				originalIndex := getIndexInSlice(tt.keys, byte_key)
				require.Equal(t, byte_key, flatten_tree_input[i].key)
				require.Equal(t, flatten_tree[i].colourIsred, tt.expectedColors[originalIndex])
			}
		})
	}
}

func TestRandomTreesMulti(t *testing.T) {
	setSeed(t)
	tree := &binarySearchTreeMulti{}
	amount := rand.Intn(100000)
	keySize := rand.Intn(100)
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
	ValidateRBTreeMulti(t, tree)
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
//  - root has no parent
//  - if node A is a child of B, B must be the parent of A)
func ValidateRBTreeMulti(t *testing.T, tree *binarySearchTreeMulti) {
	require.False(t, tree.root.colourIsred)
	require.True(t, tree.root.parent == nil)

	treeDepth, nodeCount, _ := WalkTreeMulti(t, tree.root)
	maxDepth := 2 * math.Log2(float64(nodeCount)+1)
	require.True(t, treeDepth <= int(maxDepth))
}

// Walks through the tree and counts the depth, number of nodes and number of black nodes
func WalkTreeMulti(t *testing.T, node *binarySearchNodeMulti) (int, int, int) {
	if node == nil {
		return 0, 0, 0
	}

	// validate parent/child connections
	if node.right != nil {
		require.Equal(t, node.right.parent, node)
	}
	if node.left != nil {
		require.Equal(t, node.left.parent, node)
	}

	// red nodes need black (or nil) children
	if node.colourIsred {
		require.True(t, node.left == nil || !node.left.colourIsred)
		require.True(t, node.right == nil || !node.right.colourIsred)
	}

	blackNode := int(1)
	if node.colourIsred {
		blackNode = 0
	}

	if node.right == nil && node.left == nil {
		return 1, 1, blackNode
	}

	depthRight, nodeCountRight, blackNodesDepthRight := WalkTreeMulti(t, node.right)
	depthLeft, nodeCountLeft, blackNodesDepthLeft := WalkTreeMulti(t, node.left)
	require.Equal(t, blackNodesDepthRight, blackNodesDepthLeft)

	nodeCount := nodeCountLeft + nodeCountRight + 1
	if depthRight > depthLeft {
		return depthRight + 1, nodeCount, blackNodesDepthRight + blackNode
	} else {
		return depthLeft + 1, nodeCount, blackNodesDepthRight + blackNode
	}
}
