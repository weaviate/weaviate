//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringset

import (
	"bytes"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// BinarySearchTreeCursorNoCopy is an [InnerCursor] that walks the tree's own
// nodes, handing out their keys and bitmaps rather than copies. Unlike
// [NewBinarySearchTreeCursor], it allocates nothing but itself.
//
// It locks nothing. The caller must hold the tree's read lock for the cursor's
// whole lifetime, or otherwise keep writers away. What it yields belongs to the
// tree, so anything kept past that lock must be copied.
type BinarySearchTreeCursorNoCopy struct {
	root *BinarySearchNode
	node *BinarySearchNode
	// started distinguishes "before the first node" from "past the last", which
	// a nil node alone cannot.
	started bool
}

var _ InnerCursor = (*BinarySearchTreeCursorNoCopy)(nil)

// NewBinarySearchTreeCursorNoCopy walks bst without copying it. See the type's
// documentation for what the caller must guarantee.
func NewBinarySearchTreeCursorNoCopy(bst *BinarySearchTree) *BinarySearchTreeCursorNoCopy {
	return &BinarySearchTreeCursorNoCopy{root: bst.root}
}

func (c *BinarySearchTreeCursorNoCopy) First() ([]byte, BitmapLayer, error) {
	c.started = true
	c.node = leftmost(c.root)
	return c.current()
}

func (c *BinarySearchTreeCursorNoCopy) Next() ([]byte, BitmapLayer, error) {
	if !c.started {
		return c.First()
	}
	if c.node == nil {
		return nil, BitmapLayer{}, nil
	}
	c.node = successor(c.node)
	return c.current()
}

// Seek moves to the first node with key >= key by descending the tree, and
// reports NotFound past the end. On NotFound the position is left unchanged,
// so Next behaves as if Seek were never called.
func (c *BinarySearchTreeCursorNoCopy) Seek(key []byte) ([]byte, BitmapLayer, error) {
	var found *BinarySearchNode
	for node := c.root; node != nil; {
		if bytes.Compare(node.Key, key) < 0 {
			node = node.right
			continue
		}
		// a candidate, but an earlier one may sit to its left
		found = node
		node = node.left
	}

	if found == nil {
		return nil, BitmapLayer{}, lsmkv.NotFound
	}
	c.started = true
	c.node = found
	return c.current()
}

func (c *BinarySearchTreeCursorNoCopy) current() ([]byte, BitmapLayer, error) {
	if c.node == nil {
		return nil, BitmapLayer{}, nil
	}
	return c.node.Key, c.node.Value, nil
}

func leftmost(node *BinarySearchNode) *BinarySearchNode {
	if node == nil {
		return nil
	}
	for node.left != nil {
		node = node.left
	}
	return node
}

// successor is the next node in key order, walking the tree's parent pointers.
func successor(node *BinarySearchNode) *BinarySearchNode {
	if node.right != nil {
		return leftmost(node.right)
	}
	for node.parent != nil && node.parent.right == node {
		node = node.parent
	}
	return node.parent
}
