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

import (
	"bytes"

	"github.com/dgraph-io/sroar"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/rbtree"
)

type binarySearchTreeRoaringSet struct {
	root *binarySearchNodeRoaringSet
}

type roaringSet struct {
	additions *sroar.Bitmap
	deletions *sroar.Bitmap
}

func newRoaringSet() roaringSet {
	return roaringSet{
		additions: sroar.NewBitmap(),
		deletions: sroar.NewBitmap(),
	}
}

func (t *binarySearchTreeRoaringSet) insert(key []byte, values roaringSet) {
	if t.root == nil {
		t.root = &binarySearchNodeRoaringSet{
			key: key,
			value: roaringSet{
				additions: values.additions.Clone(),
				deletions: values.deletions.Clone(),
			},
			colourIsRed: false, // root node is always black
		}
		return
	}

	if newRoot := t.root.insert(key, values); newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

func (t *binarySearchTreeRoaringSet) get(key []byte) (*roaringSet, error) {
	if t.root == nil {
		return nil, NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTreeRoaringSet) flattenInOrder() []*binarySearchNodeRoaringSet {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeRoaringSet struct {
	key         []byte
	value       roaringSet
	left        *binarySearchNodeRoaringSet
	right       *binarySearchNodeRoaringSet
	parent      *binarySearchNodeRoaringSet
	colourIsRed bool
}

func (n *binarySearchNodeRoaringSet) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNodeRoaringSet) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNodeRoaringSet)
}

func (n *binarySearchNodeRoaringSet) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNodeRoaringSet) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNodeRoaringSet)
}

func (n *binarySearchNodeRoaringSet) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNodeRoaringSet) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNodeRoaringSet)
}

func (n *binarySearchNodeRoaringSet) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNodeRoaringSet) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNodeRoaringSet) IsNil() bool {
	return n == nil
}

func addNewSearchNodeRoaringSetReceiver(nodePtr **binarySearchNodeRoaringSet) {
	*nodePtr = &binarySearchNodeRoaringSet{}
}

func (n *binarySearchNodeRoaringSet) insert(key []byte, values roaringSet) *binarySearchNodeRoaringSet {
	if bytes.Equal(key, n.key) {
		// Merging the new additions and deletions into the existing ones is a
		// four-step process:
		//
		// 1. make sure anything that's added is not part of the deleted list, in
		//    case it was previously deleted
		// 2. actually add the new entries to additions
		// 3. make sure anything that's deleted is not part of the additions list,
		//    in case it was recently added
		// 4. actually add the new entries to deletions (this step is vital in case
		//    a delete points to an entry of a previous segment that's not added in
		//    this memtable)
		if !values.additions.IsEmpty() && !n.value.deletions.IsEmpty() {
			n.value.deletions.AndNot(values.additions)
		}
		n.value.additions.Or(values.additions)

		if !values.deletions.IsEmpty() && !n.value.additions.IsEmpty() {
			n.value.additions.AndNot(values.deletions)
		}
		n.value.deletions.Or(values.deletions)
		return nil
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			return n.left.insert(key, values)
		} else {
			n.left = &binarySearchNodeRoaringSet{
				key: key,
				value: roaringSet{
					additions: values.additions.Clone(),
					deletions: values.deletions.Clone(),
				},
				parent:      n,
				colourIsRed: true,
			}
			return binarySearchNodeRoaringSetFromRB(rbtree.Rebalance(n.left))
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, values)
		} else {
			n.right = &binarySearchNodeRoaringSet{
				key: key,
				value: roaringSet{
					additions: values.additions.Clone(),
					deletions: values.deletions.Clone(),
				},
				parent:      n,
				colourIsRed: true,
			}
			return binarySearchNodeRoaringSetFromRB(rbtree.Rebalance(n.right))
		}
	}
}

func (n *binarySearchNodeRoaringSet) get(key []byte) (*roaringSet, error) {
	if bytes.Equal(n.key, key) {
		return &n.value, nil
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

func binarySearchNodeRoaringSetFromRB(rbNode rbtree.Node) (bsNode *binarySearchNodeRoaringSet) {
	if rbNode == nil {
		bsNode = nil
		return
	}
	bsNode = rbNode.(*binarySearchNodeRoaringSet)
	return
}

func (n *binarySearchNodeRoaringSet) flattenInOrder() []*binarySearchNodeRoaringSet {
	var left []*binarySearchNodeRoaringSet
	var right []*binarySearchNodeRoaringSet

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNodeRoaringSet{n}, right...)
	return append(left, right...)
}
