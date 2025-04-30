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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinarySearchTreeSerialization(t *testing.T) {
	tree := &binarySearchTree{
		root: &binarySearchNode{
			key:         []byte("root"),
			value:       []byte("root-val"),
			tombstone:   false,
			colourIsRed: true,
			secondaryKeys: [][]byte{
				[]byte("sec1"),
				[]byte("sec2"),
			},
			left: &binarySearchNode{
				key:         []byte("left"),
				value:       []byte("left-val"),
				tombstone:   false,
				colourIsRed: false,
			},
			right: &binarySearchNode{
				key:         []byte("right"),
				value:       []byte("right-val"),
				tombstone:   true,
				colourIsRed: true,
			},
		},
	}
	tree.root.left.parent = tree.root
	tree.root.right.parent = tree.root

	// Serialize to file
	tmpFile, err := os.CreateTemp("", "tree-binary-*.bin")
	require.NoError(t, err)

	err = serializeBinarySearchTree(tree, tmpFile)
	require.NoError(t, err)

	err = tmpFile.Close()
	require.NoError(t, err)

	// Deserialize from file
	tmpFile, err = os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer tmpFile.Close()

	deserializedTree, err := deserializeBinarySearchTree(tmpFile)
	require.NoError(t, err)

	require.Equal(t, tree, deserializedTree)
}
