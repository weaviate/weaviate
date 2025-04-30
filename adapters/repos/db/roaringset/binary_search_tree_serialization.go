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
	"encoding/binary"
	"io"

	"github.com/weaviate/sroar"
)

// ------------------ Serialization ------------------

func SerializeBinarySearchTree(tree *BinarySearchTree, w io.Writer) error {
	return serializeBinarySearchNode(w, tree.root)
}

func serializeBinarySearchNode(w io.Writer, node *BinarySearchNode) error {
	if node == nil {
		return binary.Write(w, binary.LittleEndian, byte(0))
	}
	binary.Write(w, binary.LittleEndian, byte(1)) // presence

	binary.Write(w, binary.LittleEndian, boolToByte(node.colourIsRed))
	writeBytes(w, node.Key)

	// Serialize BitmapLayer
	serializeBitmapLayer(w, node.Value)

	serializeBinarySearchNode(w, node.left)
	serializeBinarySearchNode(w, node.right)

	return nil
}

func serializeBitmapLayer(w io.Writer, layer BitmapLayer) {
	if layer.Additions != nil {
		writeBytes(w, layer.Additions.ToBuffer())
	} else {
		writeBytes(w, []byte{})
	}

	if layer.Deletions != nil {
		writeBytes(w, layer.Deletions.ToBuffer())
	} else {
		writeBytes(w, []byte{})
	}
}

// ------------------ Deserialization ------------------

func DeserializeBinarySearchTree(r io.Reader) (*BinarySearchTree, error) {
	root, err := deserializeBinarySearchNode(r, nil)
	if err != nil {
		return nil, err
	}
	return &BinarySearchTree{root: root}, nil
}

func deserializeBinarySearchNode(r io.Reader, parent *BinarySearchNode) (*BinarySearchNode, error) {
	var marker byte
	binary.Read(r, binary.LittleEndian, &marker)
	if marker == 0 {
		return nil, nil
	}

	node := &BinarySearchNode{}
	var b byte

	binary.Read(r, binary.LittleEndian, &b)
	node.colourIsRed = b == 1

	node.Key, _ = readBytes(r)

	// Deserialize BitmapLayer
	node.Value = deserializeBitmapLayer(r)

	node.left, _ = deserializeBinarySearchNode(r, node)
	node.right, _ = deserializeBinarySearchNode(r, node)
	node.parent = parent
	return node, nil
}

func deserializeBitmapLayer(r io.Reader) BitmapLayer {
	var layer BitmapLayer

	additionsData, _ := readBytes(r)
	layer.Additions = sroar.FromBuffer(additionsData)

	deletionsData, _ := readBytes(r)
	layer.Deletions = sroar.FromBuffer(deletionsData)

	return layer
}

// ------------------ Helpers ------------------

func writeBytes(w io.Writer, data []byte) error {
	binary.Write(w, binary.LittleEndian, uint32(len(data)))
	_, err := w.Write(data)
	return err
}

func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	binary.Read(r, binary.LittleEndian, &length)
	buf := make([]byte, length)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
