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

type binarySearchTreeMulti struct {
	root *binarySearchNodeMulti
}

type value struct {
	value     []byte
	tombstone bool
}

func (t *binarySearchTreeMulti) insert(key []byte, values []value) {
	if t.root == nil {
		t.root = &binarySearchNodeMulti{
			key:         key,
			values:      values,
			colourIsRed: false, // root node is always black
		}
		return
	}

	if newRoot := t.root.insert(key, values); newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

func (t *binarySearchTreeMulti) get(key []byte) ([]value, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.get(key)
}

// // set Tombstone for the entire entry, i.e. all values for this key
// func (t *binarySearchTreeMulti) setTombstone(key []byte) {
// 	if t.root == nil {
// 		// we need to actively insert a node with a tombstone, even if this node is
// 		// not present because we still need to propagate the delete into the disk
// 		// segments. It could refer to an entity which was created in a previous
// 		// segment and is thus unknown to this memtable
// 		t.root = &binarySearchNodeMulti{
// 			key:       key,
// 			value:     nil,
// 			tombstone: true,
// 		}
// 	}

// 	t.root.setTombstone(key)
// }

func (t *binarySearchTreeMulti) flattenInOrder() []*binarySearchNodeMulti {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeMulti struct {
	key         []byte
	values      []value
	left        *binarySearchNodeMulti
	right       *binarySearchNodeMulti
	parent      *binarySearchNodeMulti
	colourIsRed bool
}

func (n *binarySearchNodeMulti) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNodeMulti) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeMultiReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNodeMulti)
}

func (n *binarySearchNodeMulti) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNodeMulti) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeMultiReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNodeMulti)
}

func (n *binarySearchNodeMulti) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNodeMulti) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeMultiReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNodeMulti)
}

func (n *binarySearchNodeMulti) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNodeMulti) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNodeMulti) IsNil() bool {
	return n == nil
}

func addNewSearchNodeMultiReceiver(nodePtr **binarySearchNodeMulti) {
	*nodePtr = &binarySearchNodeMulti{}
}

func (n *binarySearchNodeMulti) insert(key []byte, values []value) *binarySearchNodeMulti {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, values...)
		return nil
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			return n.left.insert(key, values)
		} else {
			n.left = &binarySearchNodeMulti{
				key:         key,
				values:      values,
				parent:      n,
				colourIsRed: true,
			}
			return binarySearchNodeMultiFromRB(rbtree.Rebalance(n.left))
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, values)
		} else {
			n.right = &binarySearchNodeMulti{
				key:         key,
				values:      values,
				parent:      n,
				colourIsRed: true,
			}
			return binarySearchNodeMultiFromRB(rbtree.Rebalance(n.right))
		}
	}
}

func (n *binarySearchNodeMulti) get(key []byte) ([]value, error) {
	if bytes.Equal(n.key, key) {
		return n.values, nil
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

func binarySearchNodeMultiFromRB(rbNode rbtree.Node) (bsNode *binarySearchNodeMulti) {
	if rbNode == nil {
		bsNode = nil
		return
	}
	bsNode = rbNode.(*binarySearchNodeMulti)
	return
}

// func (n *binarySearchNodeMulti) setTombstone(key []byte) {
// 	if bytes.Equal(n.key, key) {
// 		n.value = nil
// 		n.tombstone = true
// 	}

// 	if bytes.Compare(key, n.key) < 0 {
// 		if n.left == nil {
// 			n.left = &binarySearchNodeMulti{
// 				key:       key,
// 				value:     nil,
// 				tombstone: true,
// 			}
// 			return
// 		}

// 		n.left.setTombstone(key)
// 		return
// 	} else {
// 		if n.right == nil {
// 			n.right = &binarySearchNodeMulti{
// 				key:       key,
// 				value:     nil,
// 				tombstone: true,
// 			}
// 			return
// 		}

// 		n.right.setTombstone(key)
// 		return
// 	}
// }

func (n *binarySearchNodeMulti) flattenInOrder() []*binarySearchNodeMulti {
	var left []*binarySearchNodeMulti
	var right []*binarySearchNodeMulti

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNodeMulti{n}, right...)
	return append(left, right...)
}
