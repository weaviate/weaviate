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
	"bytes"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type BinarySearchTreeCursor struct {
	nodes       []*BinarySearchNode
	nextNodePos int
}

func NewBinarySearchTreeCursor(bst *BinarySearchTree) *BinarySearchTreeCursor {
	return &BinarySearchTreeCursor{nodes: bst.FlattenInOrder()}
}

func (c *BinarySearchTreeCursor) First() ([]byte, BitmapLayer, error) {
	c.nextNodePos = 0
	return c.Next()
}

func (c *BinarySearchTreeCursor) Next() ([]byte, BitmapLayer, error) {
	if c.nextNodePos >= len(c.nodes) {
		return nil, BitmapLayer{}, nil
	}

	pos := c.nextNodePos
	c.nextNodePos++
	return c.nodes[pos].Key, c.nodes[pos].Value, nil
}

func (c *BinarySearchTreeCursor) Seek(key []byte) ([]byte, BitmapLayer, error) {
	pos := c.posKeyGreaterThanEqual(key)
	if pos == -1 {
		return nil, BitmapLayer{}, lsmkv.NotFound
	}
	c.nextNodePos = pos
	return c.Next()
}

func (c *BinarySearchTreeCursor) posKeyGreaterThanEqual(key []byte) int {
	// seek from the end, return position of first (from the beginning) node with key >= given key
	// if key > node_key return previous pos
	// if key == node_key return current pos
	// if key < node_key continue or return current pos if all nodes checked
	pos := -1
	for i := len(c.nodes) - 1; i >= 0; i-- {
		if cmp := bytes.Compare(key, c.nodes[i].Key); cmp > 0 {
			break
		} else if cmp == 0 {
			pos = i
			break
		}
		pos = i
	}
	return pos
}
