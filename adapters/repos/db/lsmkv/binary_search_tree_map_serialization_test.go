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

func TestSerializeDeserializeTreeMap(t *testing.T) {
	tree := &binarySearchTreeMap{
		root: &binarySearchNodeMap{
			key:         []byte("root"),
			colourIsRed: false,
			values: []MapPair{
				{Key: []byte("k1"), Value: []byte("v1"), Tombstone: false},
				{Key: []byte("k2"), Value: []byte("v2"), Tombstone: true},
			},
			left: &binarySearchNodeMap{
				key:         []byte("left"),
				colourIsRed: true,
				values: []MapPair{
					{Key: []byte("lk"), Value: []byte("lv"), Tombstone: false},
				},
			},
			right: &binarySearchNodeMap{
				key:         []byte("right"),
				colourIsRed: false,
				values: []MapPair{
					{Key: []byte("rk"), Value: []byte("rv"), Tombstone: true},
				},
			},
		},
	}
	tree.root.left.parent = tree.root
	tree.root.right.parent = tree.root

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "tree-map-*.bin")
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
	if err := serializeSearchTreeMap(tree, fw); err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}
	fw.Close()

	// Deserialize
	fr, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	deserializedTree, err := deserializeSearchTreeMap(fr)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}
	fr.Close()

	require.Equal(t, tree, deserializedTree)
}
