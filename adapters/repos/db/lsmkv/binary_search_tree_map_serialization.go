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
	"encoding/binary"
	"io"
)

// ------------------ Serialization ------------------

func serializeSearchTreeMap(tree *binarySearchTreeMap, w io.Writer) error {
	return serializeSearchNodeMap(w, tree.root)
}

func serializeSearchNodeMap(w io.Writer, node *binarySearchNodeMap) error {
	if node == nil {
		return binary.Write(w, binary.LittleEndian, byte(0))
	}
	binary.Write(w, binary.LittleEndian, byte(1)) // presence

	binary.Write(w, binary.LittleEndian, boolToByte(node.colourIsRed))
	writeBytes(w, node.key)

	binary.Write(w, binary.LittleEndian, uint32(len(node.values)))
	for _, pair := range node.values {
		writeBytes(w, pair.Key)
		writeBytes(w, pair.Value)
		binary.Write(w, binary.LittleEndian, boolToByte(pair.Tombstone))
	}

	serializeSearchNodeMap(w, node.left)
	serializeSearchNodeMap(w, node.right)

	return nil
}

// ------------------ Deserialization ------------------

func deserializeSearchTreeMap(r io.Reader) (*binarySearchTreeMap, error) {
	root, err := deserializeSearchNodeMap(r, nil)
	if err != nil {
		return nil, err
	}
	return &binarySearchTreeMap{root: root}, nil
}

func deserializeSearchNodeMap(r io.Reader, parent *binarySearchNodeMap) (*binarySearchNodeMap, error) {
	var marker byte
	binary.Read(r, binary.LittleEndian, &marker)
	if marker == 0 {
		return nil, nil
	}

	node := &binarySearchNodeMap{}
	var b byte

	binary.Read(r, binary.LittleEndian, &b)
	node.colourIsRed = b == 1

	node.key, _ = readBytes(r)

	var numPairs uint32
	binary.Read(r, binary.LittleEndian, &numPairs)
	node.values = make([]MapPair, numPairs)
	for i := uint32(0); i < numPairs; i++ {
		k, _ := readBytes(r)
		v, _ := readBytes(r)
		binary.Read(r, binary.LittleEndian, &b)
		node.values[i] = MapPair{
			Key:       k,
			Value:     v,
			Tombstone: b == 1,
		}
	}

	node.left, _ = deserializeSearchNodeMap(r, node)
	node.right, _ = deserializeSearchNodeMap(r, node)
	node.parent = parent
	return node, nil
}
