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

package lsmkv

import (
	"bytes"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type binarySearchTree struct {
	root *binarySearchNode
}

// returns net additions of insert in bytes, previous secondary keys, and
// whether the key was already live (present and not tombstoned)
func (t *binarySearchTree) insert(key, value []byte, secondaryKeys [][]byte) (int, [][]byte, bool) {
	if t.root == nil {
		t.root = &binarySearchNode{
			key:           key,
			value:         value,
			secondaryKeys: secondaryKeys,
			colourIsRed:   false, // root node is always black
		}
		return len(key) + len(value), nil, false
	}

	addition, newRoot, previousSecondaryKeys, wasLive := t.root.insert(key, value, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black

	return addition, previousSecondaryKeys, wasLive
}

func (t *binarySearchTree) get(key []byte) ([]byte, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTree) getNode(key []byte) (*binarySearchNode, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.getNode(key)
}

// exists checks if a key exists and is not deleted, without returning the value.
func (t *binarySearchTree) exists(key []byte) error {
	if t.root == nil {
		return lsmkv.NotFound
	}

	_, err := t.root.getNode(key)
	return err
}

// returns previous secondary keys and whether the key was already tombstoned
func (t *binarySearchTree) setTombstone(key, value []byte, secondaryKeys [][]byte) ([][]byte, bool) {
	if t.root == nil {
		// we need to actively insert a node with a tombstone, even if this node is
		// not present because we still need to propagate the delete into the disk
		// segments. It could refer to an entity which was created in a previous
		// segment and is thus unknown to this memtable
		t.root = &binarySearchNode{
			key:           key,
			value:         value,
			tombstone:     true,
			secondaryKeys: secondaryKeys,
			colourIsRed:   false, // root node is always black
		}
		return nil, false
	}

	newRoot, prevSecondaryKeys, wasTombstoned := t.root.setTombstone(key, value, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
	return prevSecondaryKeys, wasTombstoned
}

func (t *binarySearchTree) flattenInOrder() []*binarySearchNode {
	if t.root == nil {
		return nil
	}

	return t.root.copyInOrder(nil, t.root.subtreeSize())
}

// flattenInOrderRange returns only nodes with min <= key <= max, where a nil
// bound is unbounded. It counts first so the result is allocated exactly once
// at its final size.
func (t *binarySearchTree) flattenInOrderRange(min, max []byte) []*binarySearchNode {
	if t.root == nil {
		return nil
	}

	size := t.root.countInRange(min, max)
	if size == 0 {
		return nil
	}
	return t.root.copyInOrder(min, size)
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

// This is not very allocation friendly, since we basically need to allocate
// once for each element in the memtable. However, these results can
// potentially be cached, as we don't care about the intermediary results, just
// the net additions.
func (t *binarySearchTree) countStats() *countStats {
	stats := &countStats{}
	if t.root == nil {
		return stats
	}

	var buf [pendingCap]*binarySearchNode
	pending := buf[:0]
	for n := t.root; n != nil; n = n.left {
		pending = append(pending, n)
	}
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if n.tombstone {
			stats.tombstonedKeys = append(stats.tombstonedKeys, n.key)
		} else {
			stats.upsertKeys = append(stats.upsertKeys, n.key)
		}
		for n = n.right; n != nil; n = n.left {
			pending = append(pending, n)
		}
	}
	return stats
}

// pendingCap sizes the array an in-order walk keeps its unvisited ancestors in.
// A red-black tree of n nodes is at most 2*log2(n+1) high, so 64 covers any
// tree under 2^32 nodes, and a taller one only costs the walk an allocation.
const pendingCap = 64

// Every walk over binarySearchNode, binarySearchNodeMap or binarySearchNodeMulti
// that allocates is a loop. A recursive one allocates from a new call stack per
// path through the tree, and the heap profile never frees a stack's bucket.
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
func (n *binarySearchNode) insert(key, value []byte, secondaryKeys [][]byte) (netAdditions int, newRoot *binarySearchNode, previousSecondaryKeys [][]byte, wasLive bool) {
	for {
		if bytes.Equal(key, n.key) {
			// since the key already exists, we only need to take the difference
			// between the existing value and the new one to determine net change
			netAdditions = len(n.value) - len(value)
			if netAdditions < 0 {
				netAdditions *= -1
			}

			// assign new value to node
			n.value = value

			wasLive = !n.tombstone
			// reset tombstone in case it had one
			n.tombstone = false
			previousSecondaryKeys = n.secondaryKeys
			n.secondaryKeys = secondaryKeys

			newRoot = nil // tree root does not change when replacing node
			return netAdditions, newRoot, previousSecondaryKeys, wasLive
		}

		if bytes.Compare(key, n.key) < 0 {
			if n.left == nil {
				n.left = &binarySearchNode{
					key:           key,
					value:         value,
					secondaryKeys: secondaryKeys,
					parent:        n,
					colourIsRed:   true, // new nodes are always red, except root node which is handled in the tree itself
				}
				newRoot = binarySearchNodeFromRB(rbtree.Rebalance(n.left))
				netAdditions = len(key) + len(value)
				return netAdditions, newRoot, previousSecondaryKeys, false
			}
			n = n.left
		} else {
			if n.right == nil {
				n.right = &binarySearchNode{
					key:           key,
					value:         value,
					secondaryKeys: secondaryKeys,
					parent:        n,
					colourIsRed:   true,
				}
				netAdditions = len(key) + len(value)
				newRoot = binarySearchNodeFromRB(rbtree.Rebalance(n.right))
				return netAdditions, newRoot, previousSecondaryKeys, false
			}
			n = n.right
		}
	}
}

func (n *binarySearchNode) get(key []byte) ([]byte, error) {
	node, err := n.getNode(key)
	if err != nil {
		return nil, err
	}
	return node.value, nil
}

func (n *binarySearchNode) getNode(key []byte) (*binarySearchNode, error) {
	for n != nil {
		if bytes.Equal(n.key, key) {
			if !n.tombstone {
				return n, nil
			} else {
				return nil, errorFromTombstonedValue(n.value)
			}
		}

		if bytes.Compare(key, n.key) < 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil, lsmkv.NotFound
}

func (n *binarySearchNode) setTombstone(key, value []byte, secondaryKeys [][]byte) (*binarySearchNode, [][]byte, bool) {
	for {
		if bytes.Equal(n.key, key) {
			wasTombstoned := n.tombstone
			prevSecondaryKeys := n.secondaryKeys
			n.value = value
			n.tombstone = true
			n.secondaryKeys = secondaryKeys
			return nil, prevSecondaryKeys, wasTombstoned
		}

		if bytes.Compare(key, n.key) < 0 {
			if n.left == nil {
				n.left = &binarySearchNode{
					key:           key,
					value:         value,
					tombstone:     true,
					secondaryKeys: secondaryKeys,
					parent:        n,
					colourIsRed:   true,
				}
				return binarySearchNodeFromRB(rbtree.Rebalance(n.left)), nil, false
			}
			n = n.left
		} else {
			if n.right == nil {
				n.right = &binarySearchNode{
					key:           key,
					value:         value,
					tombstone:     true,
					secondaryKeys: secondaryKeys,
					parent:        n,
					colourIsRed:   true,
				}
				return binarySearchNodeFromRB(rbtree.Rebalance(n.right)), nil, false
			}
			n = n.right
		}
	}
}

// copyInOrder copies size nodes in key order, starting at the smallest key
// >= min, where a nil min is unbounded. The copies share one backing array.
func (n *binarySearchNode) copyInOrder(min []byte, size int) []*binarySearchNode {
	var buf [pendingCap]*binarySearchNode
	pending := buf[:0]
	for n != nil {
		if min != nil && bytes.Compare(n.key, min) < 0 {
			n = n.right
		} else {
			pending = append(pending, n)
			n = n.left
		}
	}

	copies := make([]binarySearchNode, size)
	res := make([]*binarySearchNode, size)
	for i := range res {
		n = pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		copies[i] = n.shallowCopy()
		res[i] = &copies[i]
		for n = n.right; n != nil; n = n.left {
			pending = append(pending, n)
		}
	}
	return res
}

// countInRange sizes flattenInOrderRange's allocation. It drops a bound (nil)
// once the current node proves it holds for a whole subtree, so interior
// in-range subtrees are counted by subtreeSize, compare-free.
func (n *binarySearchNode) countInRange(min, max []byte) int {
	if n == nil {
		return 0
	}
	if min == nil && max == nil {
		return n.subtreeSize()
	}
	aboveMin := min == nil || bytes.Compare(n.key, min) >= 0
	belowMax := max == nil || bytes.Compare(n.key, max) <= 0
	size := 0
	if aboveMin {
		leftMax := max
		if belowMax {
			leftMax = nil
		}
		size += n.left.countInRange(min, leftMax)
	}
	if aboveMin && belowMax {
		size++
	}
	if belowMax {
		rightMin := min
		if aboveMin {
			rightMin = nil
		}
		size += n.right.countInRange(rightMin, max)
	}
	return size
}

func (n *binarySearchNode) subtreeSize() int {
	if n == nil {
		return 0
	}
	s := 1
	if n.left != nil {
		s += n.left.subtreeSize()
	}
	if n.right != nil {
		s += n.right.subtreeSize()
	}
	return s
}

func (n *binarySearchNode) shallowCopy() binarySearchNode {
	var skeys [][]byte
	if ln := len(n.secondaryKeys); ln > 0 {
		skeys = make([][]byte, ln)
		copy(skeys, n.secondaryKeys)
	}

	return binarySearchNode{
		key:           n.key,
		value:         n.value,
		tombstone:     n.tombstone,
		colourIsRed:   n.colourIsRed,
		secondaryKeys: skeys,
	}
}

func binarySearchNodeFromRB(rbNode rbtree.Node) (bsNode *binarySearchNode) {
	if rbNode == nil {
		bsNode = nil
		return bsNode
	}
	bsNode = rbNode.(*binarySearchNode)
	return bsNode
}
