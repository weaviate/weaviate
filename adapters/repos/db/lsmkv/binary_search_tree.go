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
	"bytes"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type binarySearchTree struct {
	root *binarySearchNode
}

// returns net additions of insert in bytes, and previous secondary keys
func (t *binarySearchTree) insert(key, value []byte, secondaryKeys [][]byte) (int, [][]byte) {
	if t.root == nil {
		t.root = &binarySearchNode{
			key:           key,
			value:         value,
			secondaryKeys: secondaryKeys,
			colourIsRed:   false, // root node is always black
		}
		return len(key) + len(value), nil
	}

	addition, newRoot, previousSecondaryKeys := t.root.insert(key, value, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black

	return addition, previousSecondaryKeys
}

func (t *binarySearchTree) get(key []byte) ([]byte, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTree) setTombstone(key []byte, secondaryKeys [][]byte) {
	if t.root == nil {
		// we need to actively insert a node with a tombstone, even if this node is
		// not present because we still need to propagate the delete into the disk
		// segments. It could refer to an entity which was created in a previous
		// segment and is thus unknown to this memtable
		t.root = &binarySearchNode{
			key:           key,
			value:         nil,
			tombstone:     true,
			secondaryKeys: secondaryKeys,
			colourIsRed:   false, // root node is always black
		}
		return
	}

	newRoot := t.root.setTombstone(key, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

func (t *binarySearchTree) flattenInOrder() []*binarySearchNode {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type countStats struct {
	upsertKeys     [][]byte
	tombstonedKeys [][]byte
}

func (c *countStats) hasUpsert(needle []byte) bool {
	if c == nil {
		return false
	}

	for _, hay := range c.upsertKeys {
		if bytes.Equal(needle, hay) {
			return true
		}
	}

	return false
}

func (c *countStats) hasTombstone(needle []byte) bool {
	if c == nil {
		return false
	}

	for _, hay := range c.tombstonedKeys {
		if bytes.Equal(needle, hay) {
			return true
		}
	}

	return false
}

func (t *binarySearchTree) countStats() *countStats {
	stats := &countStats{}
	if t.root == nil {
		return stats
	}

	t.root.countStats(stats)
	return stats
}

type binarySearchNode struct {
	key           []byte
	value         []byte
	secondaryKeys [][]byte
	left          *binarySearchNode
	right         *binarySearchNode
	parent        *binarySearchNode
	tombstone     bool
	colourIsRed   bool
}

func (n *binarySearchNode) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNode) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNode)
}

func (n *binarySearchNode) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNode) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNode)
}

func (n *binarySearchNode) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNode) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNode)
}

func (n *binarySearchNode) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNode) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNode) IsNil() bool {
	return n == nil
}

func addNewSearchNodeReceiver(nodePtr **binarySearchNode) {
	*nodePtr = &binarySearchNode{}
}

// returns net additions of insert in bytes
func (n *binarySearchNode) insert(key, value []byte, secondaryKeys [][]byte) (netAdditions int, newRoot *binarySearchNode, previousSecondaryKeys [][]byte) {
	if bytes.Equal(key, n.key) {
		// since the key already exists, we only need to take the difference
		// between the existing value and the new one to determine net change
		netAdditions = len(n.value) - len(value)
		if netAdditions < 0 {
			netAdditions *= -1
		}

		// assign new value to node
		n.value = value

		// reset tombstone in case it had one
		n.tombstone = false
		previousSecondaryKeys = n.secondaryKeys
		n.secondaryKeys = secondaryKeys

		newRoot = nil // tree root does not change when replacing node
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			netAdditions, newRoot, previousSecondaryKeys = n.left.insert(key, value, secondaryKeys)
			return
		} else {
			n.left = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsRed:   true, // new nodes are always red, except root node which is handled in the tree itself
			}
			newRoot = binarySearchNodeFromRB(rbtree.Rebalance(n.left))
			netAdditions = len(key) + len(value)
			return
		}
	} else {
		if n.right != nil {
			netAdditions, newRoot, previousSecondaryKeys = n.right.insert(key, value, secondaryKeys)
			return
		} else {
			n.right = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsRed:   true,
			}
			netAdditions = len(key) + len(value)
			newRoot = binarySearchNodeFromRB(rbtree.Rebalance(n.right))
			return
		}
	}
}

func (n *binarySearchNode) get(key []byte) ([]byte, error) {
	if bytes.Equal(n.key, key) {
		if !n.tombstone {
			return n.value, nil
		} else {
			return nil, lsmkv.Deleted
		}
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			return nil, lsmkv.NotFound
		}

		return n.left.get(key)
	} else {
		if n.right == nil {
			return nil, lsmkv.NotFound
		}

		return n.right.get(key)
	}
}

func (n *binarySearchNode) setTombstone(key []byte, secondaryKeys [][]byte) *binarySearchNode {
	if bytes.Equal(n.key, key) {
		n.value = nil
		n.tombstone = true
		n.secondaryKeys = secondaryKeys
		return nil
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			n.left = &binarySearchNode{
				key:           key,
				value:         nil,
				tombstone:     true,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsRed:   true,
			}
			return binarySearchNodeFromRB(rbtree.Rebalance(n.left))

		}
		return n.left.setTombstone(key, secondaryKeys)
	} else {
		if n.right == nil {
			n.right = &binarySearchNode{
				key:           key,
				value:         nil,
				tombstone:     true,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsRed:   true,
			}
			return binarySearchNodeFromRB(rbtree.Rebalance(n.right))
		}
		return n.right.setTombstone(key, secondaryKeys)
	}
}

func (n *binarySearchNode) flattenInOrder() []*binarySearchNode {
	var left []*binarySearchNode
	var right []*binarySearchNode

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNode{n}, right...)
	return append(left, right...)
}

// This is not very allocation friendly, since we basically need to allocate
// once for each element in the memtable. However, these results can
// potentially be cached, as we don't care about the intermediary results, just
// the net additions.
func (n *binarySearchNode) countStats(stats *countStats) {
	if n.tombstone {
		stats.tombstonedKeys = append(stats.tombstonedKeys, n.key)
	} else {
		stats.upsertKeys = append(stats.upsertKeys, n.key)
	}

	if n.left != nil {
		n.left.countStats(stats)
	}

	if n.right != nil {
		n.right.countStats(stats)
	}
}

func binarySearchNodeFromRB(rbNode rbtree.Node) (bsNode *binarySearchNode) {
	if rbNode == nil {
		bsNode = nil
		return
	}
	bsNode = rbNode.(*binarySearchNode)
	return
}
