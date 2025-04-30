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

func serializeBinarySearchTree(tree *binarySearchTree, w io.Writer) error {
	return serializeBinarySearchNode(w, tree.root)
}

func serializeBinarySearchNode(w io.Writer, node *binarySearchNode) error {
	if node == nil {
		return binary.Write(w, binary.LittleEndian, byte(0)) // null marker
	}

	if err := binary.Write(w, binary.LittleEndian, byte(1)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, boolToByte(node.tombstone)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, boolToByte(node.colourIsRed)); err != nil {
		return err
	}
	if err := writeBytes(w, node.key); err != nil {
		return err
	}
	if err := writeBytes(w, node.value); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(node.secondaryKeys))); err != nil {
		return err
	}
	for _, sec := range node.secondaryKeys {
		if err := writeBytes(w, sec); err != nil {
			return err
		}
	}

	if err := serializeBinarySearchNode(w, node.left); err != nil {
		return err
	}
	if err := serializeBinarySearchNode(w, node.right); err != nil {
		return err
	}
	return nil
}

func writeBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// ------------------ Deserialization ------------------

func deserializeBinarySearchTree(r io.Reader) (*binarySearchTree, error) {
	root, err := deserializeBinarySearchNode(r, nil)
	if err != nil {
		return nil, err
	}
	return &binarySearchTree{root: root}, nil
}

func deserializeBinarySearchNode(r io.Reader, parent *binarySearchNode) (*binarySearchNode, error) {
	var marker byte
	if err := binary.Read(r, binary.LittleEndian, &marker); err != nil {
		return nil, err
	}
	if marker == 0 {
		return nil, nil
	}

	node := &binarySearchNode{}
	var b byte

	if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
		return nil, err
	}
	node.tombstone = b == 1

	if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
		return nil, err
	}
	node.colourIsRed = b == 1

	if key, err := readBytes(r); err != nil {
		return nil, err
	} else {
		node.key = key
	}
	if val, err := readBytes(r); err != nil {
		return nil, err
	} else {
		node.value = val
	}

	var numSec uint32
	if err := binary.Read(r, binary.LittleEndian, &numSec); err != nil {
		return nil, err
	}

	if numSec > 0 {
		node.secondaryKeys = make([][]byte, numSec)
		for i := uint32(0); i < numSec; i++ {
			if sec, err := readBytes(r); err != nil {
				return nil, err
			} else {
				node.secondaryKeys[i] = sec
			}
		}
	}

	var err error
	node.left, err = deserializeBinarySearchNode(r, node)
	if err != nil {
		return nil, err
	}
	node.right, err = deserializeBinarySearchNode(r, node)
	if err != nil {
		return nil, err
	}
	node.parent = parent

	return node, nil
}

func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
