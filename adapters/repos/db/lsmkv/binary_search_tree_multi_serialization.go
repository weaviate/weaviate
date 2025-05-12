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

func serializeSearchTreeMulti(tree *binarySearchTreeMulti, w io.Writer) error {
	return serializeSearchNodeMulti(w, tree.root)
}

func serializeSearchNodeMulti(w io.Writer, node *binarySearchNodeMulti) error {
	if node == nil {
		return binary.Write(w, binary.LittleEndian, byte(0))
	}
	binary.Write(w, binary.LittleEndian, byte(1)) // presence

	binary.Write(w, binary.LittleEndian, boolToByte(node.colourIsRed))
	writeBytes(w, node.key)

	binary.Write(w, binary.LittleEndian, uint32(len(node.values)))
	for _, v := range node.values {
		writeBytes(w, v.value)
		binary.Write(w, binary.LittleEndian, boolToByte(v.tombstone))
	}

	serializeSearchNodeMulti(w, node.left)
	serializeSearchNodeMulti(w, node.right)

	return nil
}

// ------------------ Deserialization ------------------

func deserializeSearchTreeMulti(r io.Reader) (*binarySearchTreeMulti, error) {
	root, err := deserializeSearchNodeMulti(r, nil)
	if err != nil {
		return nil, err
	}
	return &binarySearchTreeMulti{root: root}, nil
}

func deserializeSearchNodeMulti(r io.Reader, parent *binarySearchNodeMulti) (*binarySearchNodeMulti, error) {
	var marker byte
	binary.Read(r, binary.LittleEndian, &marker)
	if marker == 0 {
		return nil, nil
	}

	node := &binarySearchNodeMulti{}
	var b byte

	binary.Read(r, binary.LittleEndian, &b)
	node.colourIsRed = b == 1

	node.key, _ = readBytes(r)

	var numValues uint32
	binary.Read(r, binary.LittleEndian, &numValues)
	node.values = make([]value, numValues)
	for i := uint32(0); i < numValues; i++ {
		val, _ := readBytes(r)
		binary.Read(r, binary.LittleEndian, &b)
		node.values[i] = value{value: val, tombstone: b == 1}
	}

	node.left, _ = deserializeSearchNodeMulti(r, node)
	node.right, _ = deserializeSearchNodeMulti(r, node)
	node.parent = parent
	return node, nil
}
