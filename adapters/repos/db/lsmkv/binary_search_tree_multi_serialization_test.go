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

func TestBinarySearchTreeMultiSerialization(t *testing.T) {
	tree := &binarySearchTreeMulti{
		root: &binarySearchNodeMulti{
			key:         []byte("root"),
			colourIsRed: false,
			values: []value{
				{value: []byte("v1"), tombstone: false},
				{value: []byte("v2"), tombstone: true},
			},
			left: &binarySearchNodeMulti{
				key:         []byte("left"),
				colourIsRed: true,
				values:      []value{{value: []byte("vL"), tombstone: false}},
			},
			right: &binarySearchNodeMulti{
				key:         []byte("right"),
				colourIsRed: false,
				values:      []value{{value: []byte("vR"), tombstone: true}},
			},
		},
	}
	tree.root.left.parent = tree.root
	tree.root.right.parent = tree.root

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "tree-multi-*.bin")
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
	if err := serializeSearchTreeMulti(tree, fw); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	fw.Close()

	// Deserialize
	fr, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	deserializedTree, err := deserializeSearchTreeMulti(fr)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	fr.Close()

	require.Equal(t, tree, deserializedTree)
}
