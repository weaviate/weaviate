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

// SealedBinarySearchTreeCursor walks a tree that is no longer being written to,
// handing out each node's own key and bitmaps.
//
// [NewBinarySearchTreeCursor] flattens the tree into a slice of condensed copies
// first, which is what makes it safe against concurrent writes and what gives
// the flush path the compact bitmaps it writes to disk. A reader of a sealed
// tree needs neither, and the copy dominates the read: on a tree of 100k keys it
// costs some 53ms and 95MB, against 0.75ms and nothing to walk the nodes
// directly.
//
// The tree must not be mutated for the cursor's lifetime, and the returned keys
// and bitmaps belong to the tree — a caller that mutates one corrupts it. Both
// hold for a memtable that has been swapped out of active use and whose writers
// have drained; nothing else should use this.
type SealedBinarySearchTreeCursor struct {
	root *BinarySearchNode
	node *BinarySearchNode
	// started distinguishes "before the first node" from "past the last", which
	// a nil node alone cannot.
	started bool
}

// NewSealedBinarySearchTreeCursor walks bst without copying it. See the type's
// documentation for what the caller must guarantee.
func NewSealedBinarySearchTreeCursor(bst *BinarySearchTree) *SealedBinarySearchTreeCursor {
	return &SealedBinarySearchTreeCursor{root: bst.root}
}

func (c *SealedBinarySearchTreeCursor) First() ([]byte, BitmapLayer, error) {
	c.started = true
	c.node = leftmost(c.root)
	return c.current()
}

func (c *SealedBinarySearchTreeCursor) Next() ([]byte, BitmapLayer, error) {
	if !c.started {
		return c.First()
	}
	if c.node == nil {
		return nil, BitmapLayer{}, nil
	}
	c.node = successor(c.node)
	return c.current()
}

// Seek moves to the first node whose key is greater than or equal to key,
// descending the tree rather than scanning, and reports NotFound past the end.
func (c *SealedBinarySearchTreeCursor) Seek(key []byte) ([]byte, BitmapLayer, error) {
	c.started = true

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

	c.node = found
	if found == nil {
		return nil, BitmapLayer{}, lsmkv.NotFound
	}
	return c.current()
}

func (c *SealedBinarySearchTreeCursor) current() ([]byte, BitmapLayer, error) {
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

// successor is the next node in key order: the leftmost of the right subtree if
// there is one, otherwise the first ancestor this node is in the left subtree of.
func successor(node *BinarySearchNode) *BinarySearchNode {
	if node.right != nil {
		return leftmost(node.right)
	}
	for node.parent != nil && node.parent.right == node {
		node = node.parent
	}
	return node.parent
}
