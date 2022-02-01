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

func (t *binarySearchTree) insert(key, value []byte, secondaryKeys [][]byte) {
	if t.root == nil {
		t.root = &binarySearchNode{
			key:           key,
			value:         value,
			secondaryKeys: secondaryKeys,
		}
		return
	}

	t.root.insert(key, value, secondaryKeys)
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
		}
		return
	}

	t.root.setTombstone(key, secondaryKeys)
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
	tombstone     bool
}

func (n *binarySearchNode) insert(key, value []byte,
	secondaryKeys [][]byte) {
	if bytes.Equal(key, n.key) {
		n.value = value
		n.secondaryKeys = secondaryKeys

		// reset tombstone in case it had one
		n.tombstone = false
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			n.left.insert(key, value, secondaryKeys)
			return
		} else {
			n.left = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
			}
			return
		}
	} else {
		if n.right != nil {
			n.right.insert(key, value, secondaryKeys)
			return
		} else {
			n.right = &binarySearchNode{
				key:           key,
				value:         value,
				secondaryKeys: secondaryKeys,
			}
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

func (n *binarySearchNode) setTombstone(key []byte, secondaryKeys [][]byte) {
	if bytes.Equal(n.key, key) {
		n.value = nil
		n.tombstone = true
		n.secondaryKeys = secondaryKeys
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			n.left = &binarySearchNode{
				key:           key,
				value:         nil,
				tombstone:     true,
				secondaryKeys: secondaryKeys,
			}
			return
		}

		n.left.setTombstone(key, secondaryKeys)
		return
	} else {
		if n.right == nil {
			n.right = &binarySearchNode{
				key:           key,
				value:         nil,
				tombstone:     true,
				secondaryKeys: secondaryKeys,
			}
			return
		}

		n.right.setTombstone(key, secondaryKeys)
		return
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
