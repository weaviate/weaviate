//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import "bytes"

type binarySearchTree struct {
	root *binarySearchNode
}

// returns net additions of insert in bytes
func (t *binarySearchTree) insert(key, value []byte, secondaryKeys [][]byte) int {
	if t.root == nil {
		t.root = &binarySearchNode{
			key:           key,
			value:         value,
			secondaryKeys: secondaryKeys,
			colourIsred:   false, // root node is always black
		}
		return len(key) + len(value)
	}

	addition, newRoot := t.root.insert(key, value, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsred = false // Can be flipped in the process of balancing, but root is always black

	return addition
}

func (t *binarySearchTree) get(key []byte) ([]byte, error) {
	if t.root == nil {
		return nil, NotFound
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
			colourIsred:   false, // root node is always black
		}
		return
	}

	newRoot := t.root.setTombstone(key, secondaryKeys)
	if newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsred = false // Can be flipped in the process of balancing, but root is always black
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
	colourIsred   bool
}

// returns net additions of insert in bytes
func (n *binarySearchNode) insert(key, value []byte, secondaryKeys [][]byte) (netAdditions int, newRoot *binarySearchNode) {
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
		n.secondaryKeys = secondaryKeys

		newRoot = nil // tree root does not change when replacing node
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			netAdditions, newRoot = n.left.insert(key, value, secondaryKeys)
			return
		} else {
			n.left = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsred:   true, // new nodes are always red, except root node which is handled in the tree itself
			}
			newRoot = rebalanceRedBlackTree(n.left)

			netAdditions = len(key) + len(value)
			return
		}
	} else {
		if n.right != nil {
			netAdditions, newRoot = n.right.insert(key, value, secondaryKeys)
			return
		} else {
			n.right = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
				parent:        n,
				colourIsred:   true,
			}
			netAdditions = len(key) + len(value)
			newRoot = rebalanceRedBlackTree(n.right)

			return
		}
	}
}

func (n *binarySearchNode) get(key []byte) ([]byte, error) {
	if bytes.Equal(n.key, key) {
		if !n.tombstone {
			return n.value, nil
		} else {
			return nil, Deleted
		}
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			return nil, NotFound
		}

		return n.left.get(key)
	} else {
		if n.right == nil {
			return nil, NotFound
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
				colourIsred:   true,
			}
			return rebalanceRedBlackTree(n.left)

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
				colourIsred:   true,
			}
			return rebalanceRedBlackTree(n.right)
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
