//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func createNodeEntry(key []byte, valueStart, valueSize uint64) Node {
	nodeStruct := Node{
		Key:   key,
		Start: valueStart,
		End:   valueStart + valueSize,
	}
	return nodeStruct
}

func TestDiskTreeSingleNode(t *testing.T) {
	key := []byte("test-key")
	nodeStart := uint64(1000)
	nodeSize := uint64(500)

	// Create compressed entry (root node, no children)
	node := createNodeEntry(key, nodeStart, nodeSize)
	index := NewBalanced([]Node{node})

	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)

	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)

	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			t.Run("get existing key", func(t *testing.T) {
				node, err := tree.Get(key)
				require.NoError(t, err)
				assert.Equal(t, key, node.Key)
				assert.Equal(t, nodeStart, node.Start)
				assert.Equal(t, nodeStart+nodeSize, node.End)
			})

			t.Run("get non-existing key", func(t *testing.T) {
				_, err := tree.Get([]byte("non-existing"))
				assert.Equal(t, lsmkv.NotFound, err)
			})

			t.Run("all keys", func(t *testing.T) {
				keys, err := tree.AllKeys()
				require.NoError(t, err)
				require.Len(t, keys, 1)
				assert.Equal(t, key, keys[0])
			})
		})
	}
}

func TestDiskTreeMultipleNodes(t *testing.T) {
	// Create a simple tree:
	//       "m" (root at offset 0)
	//      /   \
	//    "d"   "t"

	var entries []Node

	// Root node "m" - children at offsets we'll calculate

	rootEntry := createNodeEntry([]byte("m"), 1000, 100)

	// Left child "d" - offset will be len(rootEntry)
	leftEntry := createNodeEntry([]byte("d"), 2000, 200)

	// Right child "t" - offset will be len(rootEntry) + len(leftEntry)
	rightEntry := createNodeEntry([]byte("t"), 3000, 300)

	entries = append(entries, rootEntry)
	entries = append(entries, leftEntry)
	entries = append(entries, rightEntry)

	index := NewBalanced(entries)

	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)

	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)

	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	tests := []struct {
		name      string
		key       string
		wantStart uint64
		wantSize  uint64
		wantErr   error
	}{
		{"root node", "m", 1000, 100, nil},
		{"left child", "d", 2000, 200, nil},
		{"right child", "t", 3000, 300, nil},
		{"non-existing before", "a", 0, 0, lsmkv.NotFound},
		{"non-existing middle", "p", 0, 0, lsmkv.NotFound},
		{"non-existing after", "z", 0, 0, lsmkv.NotFound},
	}

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					node, err := tree.Get([]byte(tt.key))
					if tt.wantErr != nil {
						assert.Equal(t, tt.wantErr, err)
					} else {
						require.NoError(t, err)
						assert.Equal(t, []byte(tt.key), node.Key)
						assert.Equal(t, tt.wantStart, node.Start)
						assert.Equal(t, tt.wantStart+tt.wantSize, node.End)
					}
				})
			}

			t.Run("all keys", func(t *testing.T) {
				keys, err := tree.AllKeys()
				require.NoError(t, err)
				require.Len(t, keys, 3)

				// Keys should be in level-order
				assert.Equal(t, []byte("m"), keys[0])
				assert.Equal(t, []byte("d"), keys[1])
				assert.Equal(t, []byte("t"), keys[2])
			})
		})
	}
}

func TestDiskTreeSeek(t *testing.T) {
	// Create tree with keys: "b", "d", "f", "h", "j"
	//         "f" (root)
	//        /   \
	//      "d"   "h"
	//      /       \
	//    "b"       "j"

	var entries []Node

	// Calculate offsets
	// We need to build entries to know their sizes first
	root := createNodeEntry([]byte("f"), 5000, 100)
	left := createNodeEntry([]byte("d"), 3000, 100)
	right := createNodeEntry([]byte("h"), 7000, 100)
	leftLeftEntry := createNodeEntry([]byte("b"), 1000, 100)
	rightRightEntry := createNodeEntry([]byte("j"), 9000, 100)

	entries = append(entries, root)
	entries = append(entries, left)
	entries = append(entries, right)
	entries = append(entries, leftLeftEntry)
	entries = append(entries, rightRightEntry)

	index := NewBalanced(entries)
	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)
	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)
	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	tests := []struct {
		name    string
		key     string
		wantKey string
		wantErr error
	}{
		{"exact match b", "b", "b", nil},
		{"exact match d", "d", "d", nil},
		{"exact match f", "f", "f", nil},
		{"exact match h", "h", "h", nil},
		{"exact match j", "j", "j", nil},
		{"seek before first", "a", "b", nil},
		{"seek between b and d", "c", "d", nil},
		{"seek between d and f", "e", "f", nil},
		{"seek between f and h", "g", "h", nil},
		{"seek between h and j", "i", "j", nil},
		{"seek after last", "k", "", lsmkv.NotFound},
	}

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					node, err := tree.Seek([]byte(tt.key))
					if tt.wantErr != nil {
						assert.Equal(t, tt.wantErr, err)
					} else {
						require.NoError(t, err)
						assert.Equal(t, []byte(tt.wantKey), node.Key)
					}
				})
			}
		})
	}
}

func TestDiskTreeVariableKeySize(t *testing.T) {
	// Test with variable-length keys
	keys := [][]byte{
		[]byte("a"),
		[]byte("abc"),
		[]byte("abcdef"),
		[]byte("x"),
	}

	var entries []Node

	// Create a simple linear tree (right children only)
	for i := 0; i < len(keys); i++ {
		entries = append(entries, createNodeEntry(keys[i], uint64(i*1000), uint64(100)))
	}
	index := NewBalanced(entries)

	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)
	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)
	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			t.Run("get all keys", func(t *testing.T) {
				for i, key := range keys {
					node, err := tree.Get(key)
					require.NoError(t, err, "failed to get key %s", string(key))
					assert.Equal(t, key, node.Key)
					assert.Equal(t, uint64(i*1000), node.Start)
					assert.Equal(t, uint64(i*1000+100), node.End)
				}
			})

			t.Run("all keys", func(t *testing.T) {
				allKeys, err := tree.AllKeys()
				require.NoError(t, err)
				require.Len(t, allKeys, len(keys))

				for _, key := range keys {
					assert.True(t, slices.ContainsFunc(allKeys, func(k []byte) bool {
						return bytes.Equal(k, key)
					}))
				}
			})
		})
	}
}

func TestDiskTreeEmpty(t *testing.T) {
	tree := NewDiskTreeRaw([]byte{})
	treeCompressed := NewDiskTreeCompressed([]byte{})

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			t.Run("get on empty tree", func(t *testing.T) {
				_, err := tree.Get([]byte("any"))
				assert.Equal(t, lsmkv.NotFound, err)
			})

			t.Run("seek on empty tree", func(t *testing.T) {
				_, err := tree.Seek([]byte("any"))
				assert.Equal(t, lsmkv.NotFound, err)
			})

			t.Run("all keys on empty tree", func(t *testing.T) {
				keys, err := tree.AllKeys()
				require.NoError(t, err)
				assert.Empty(t, keys)
			})
		})
	}
}

func TestDiskTreeLargeTree(t *testing.T) {
	// Create a larger tree for stress testing
	numKeys := 100
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%05d", i))
	}

	// Sort keys to create a somewhat balanced tree structure
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Build tree entries (simplified: just a linear structure for this test)
	var entries []Node

	for i := 0; i < len(keys); i++ {
		entry := createNodeEntry(keys[i], uint64(i*1000), uint64(100))
		entries = append(entries, entry)
	}

	index := NewBalanced(entries)
	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)
	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)
	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			t.Run("get all keys", func(t *testing.T) {
				for i, key := range keys {
					node, err := tree.Get(key)
					require.NoError(t, err, "failed to get key %s", string(key))
					assert.Equal(t, key, node.Key)
					assert.Equal(t, uint64(i*1000), node.Start)
				}
			})

			t.Run("all keys returns correct count", func(t *testing.T) {
				allKeys, err := tree.AllKeys()
				require.NoError(t, err)
				assert.Len(t, allKeys, numKeys)
			})
		})
	}
}

func TestDiskTreeLargeTreeCompress(t *testing.T) {
	// Create a larger tree for stress testing
	numKeys := 100000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%05d", i))
	}

	// Sort keys to create a somewhat balanced tree structure
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Build tree entries (simplified: just a linear structure for this test)
	var entries []Node

	for i := 0; i < len(keys); i++ {
		entry := createNodeEntry(keys[i], uint64(i*1000), uint64(100))
		entries = append(entries, entry)
	}

	index := NewBalanced(entries)
	treeBytes, err := index.MarshalBinary()
	require.NoError(t, err)
	tree := NewDiskTreeRaw(treeBytes)

	treeBytesCompressed, err := index.MarshalBinaryCompressed()
	require.NoError(t, err)
	treeCompressed := NewDiskTreeCompressed(treeBytesCompressed)

	t.Logf("old_size=%d, new_size=%d", len(treeBytes), len(treeBytesCompressed))
	t.Logf("compression_ratio = %.2f%%", float64(len(treeBytesCompressed))/float64(len(treeBytes))*100)

	numKeysToTest := 100000

	keysToTest := make([][]byte, 0, numKeysToTest)
	for i := 0; i < numKeysToTest; i++ {
		keysToTest = append(keysToTest, keys[i*100])
	}

	for _, tree := range []DiskTree{tree, treeCompressed} {
		t.Run(reflect.TypeOf(tree).String(), func(t *testing.T) {
			t.Run("get all keys", func(t *testing.T) {
				for i, key := range keys {
					node, err := tree.Get(key)
					require.NoError(t, err, "failed to get key %s", string(key))
					assert.Equal(t, key, node.Key)
					assert.Equal(t, uint64(i*1000), node.Start)
				}
			})

			t.Run("all keys returns correct count", func(t *testing.T) {
				allKeys, err := tree.AllKeys()
				require.NoError(t, err)
				assert.Len(t, allKeys, numKeys)
			})

			t.Run("get selected keys", func(t *testing.T) {
				for i, key := range keysToTest {
					node, err := tree.Get(key)
					require.NoError(t, err, "failed to get key %s", string(key))
					assert.Equal(t, key, node.Key)
					assert.Equal(t, uint64(i*100*1000), node.Start)
				}
			})
		})
	}
}
