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

package roaringset

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestSerializeDeserializeBinarySearchTree(t *testing.T) {
	tree := &BinarySearchTree{
		root: &BinarySearchNode{
			Key:         []byte("root"),
			colourIsRed: false,
			Value: BitmapLayer{
				Additions: sroar.NewBitmap(),
				Deletions: sroar.NewBitmap(),
			},
			left: &BinarySearchNode{
				Key:         []byte("left"),
				colourIsRed: true,
				Value: BitmapLayer{
					Additions: sroar.NewBitmap(),
					Deletions: sroar.NewBitmap(),
				},
			},
			right: &BinarySearchNode{
				Key:         []byte("right"),
				colourIsRed: false,
				Value: BitmapLayer{
					Additions: sroar.NewBitmap(),
					Deletions: sroar.NewBitmap(),
				},
			},
		},
	}
	tree.root.left.parent = tree.root
	tree.root.right.parent = tree.root

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "tree-bitmap-*.bin")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Serialize
	fw, err := os.OpenFile(tmpFile.Name(), os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := SerializeBinarySearchTree(tree, fw); err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}
	fw.Close()

	// Deserialize
	fr, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	deserializedTree, err := DeserializeBinarySearchTree(fr)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}
	fr.Close()

	require.Equal(t, tree, deserializedTree)
}
