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
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type binarySearchTreeMap struct {
	root *binarySearchNodeMap
}

// insert satisfies mapIndex; it returns 0 because the red-black tree's node
// overhead is not tracked in the memtable's flush-size budget (only the skip
// list reports its value-log backing growth).
func (t *binarySearchTreeMap) insert(key []byte, pair MapPair) int {
	if t.root == nil {
		t.root = &binarySearchNodeMap{
			key:         key,
			values:      []MapPair{pair},
			colourIsRed: false, // root node is always black
		}
		return 0
	}

	if newRoot := t.root.insert(key, pair); newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
	return 0
}

func (t *binarySearchTreeMap) get(key []byte) ([]MapPair, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTreeMap) flattenInOrder() []*binarySearchNodeMap {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeMap struct {
	key         []byte
	values      []MapPair
	left        *binarySearchNodeMap
	right       *binarySearchNodeMap
	parent      *binarySearchNodeMap
	colourIsRed bool
}

func (n *binarySearchNodeMap) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNodeMap) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNodeMap) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNodeMap) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNodeMap) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNodeMap) IsNil() bool {
	return n == nil
}

func addNewSearchNodeMapReceiver(nodePtr **binarySearchNodeMap) {
	*nodePtr = &binarySearchNodeMap{}
}

func (n *binarySearchNodeMap) insert(key []byte, pair MapPair) *binarySearchNodeMap {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, pair)
		return nil // tree root does not change when replacing node
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			return n.left.insert(key, pair)
		} else {
			n.left = &binarySearchNodeMap{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []MapPair{pair},
			}
			return binarySearchNodeMapFromRB(rbtree.Rebalance(n.left))
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, pair)
		} else {
			n.right = &binarySearchNodeMap{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []MapPair{pair},
			}
			return binarySearchNodeMapFromRB(rbtree.Rebalance(n.right))
		}
	}
}

func (n *binarySearchNodeMap) get(key []byte) ([]MapPair, error) {
	if bytes.Equal(n.key, key) {
		return sortAndDedupValues(n.values), nil
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

func (n *binarySearchNodeMap) flattenInOrder() []*binarySearchNodeMap {
	// preallocate capacity to avoid repeated reallocations
	size := n.subtreeSize()
	res := make([]*binarySearchNodeMap, 0, size)
	return n.appendInOrder(res)
}

func (n *binarySearchNodeMap) appendInOrder(dst []*binarySearchNodeMap) []*binarySearchNodeMap {
	if n == nil {
		return dst
	}
	if n.left != nil {
		dst = n.left.appendInOrder(dst)
	}
	dst = append(dst, n.shallowCopy())
	if n.right != nil {
		dst = n.right.appendInOrder(dst)
	}
	return dst
}

func (n *binarySearchNodeMap) subtreeSize() int {
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

// takes a list of MapPair and sorts it while keeping the original order. Then
// removes redundancies (from updates or deletes after previous inserts) using
// a simple deduplication process. The input is left untouched (it may be aliased,
// e.g. a red-black tree node's values slice).
func sortAndDedupValues(in []MapPair) []MapPair {
	out := make([]MapPair, len(in))
	copy(out, in)
	return sortAndDedupValuesInPlace(out)
}

// sortAndDedupValuesInPlace is sortAndDedupValues without the defensive copy: it
// sorts and dedups vals in place and returns the deduped prefix. Only safe when the
// caller owns vals and it is not aliased anywhere else — e.g. a skip-list snapshot,
// which is freshly allocated per read. Do NOT call it on a tree node's values slice.
func sortAndDedupValuesInPlace(vals []MapPair) []MapPair {
	// stable sort so we keep the insert order on duplicates — required for the
	// keep-last dedup below. SortStableFunc (generic) avoids sort.SliceStable's
	// per-call reflection allocations on this hot BM25 read path.
	slices.SortStableFunc(vals, func(a, b MapPair) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// now deduping is as simple as looking one key ahead - if it's the same key
	// simply skip the current element. Meaning the result is a subset of sorted vals.
	outIndex := 0
	for inIndex, pair := range vals {
		// look ahead
		if inIndex+1 < len(vals) && bytes.Equal(vals[inIndex+1].Key, pair.Key) {
			continue
		}

		vals[outIndex] = pair
		outIndex++
	}

	return vals[:outIndex]
}

func binarySearchNodeMapFromRB(rbNode rbtree.Node) (bsNode *binarySearchNodeMap) {
	if rbNode == nil {
		bsNode = nil
		return bsNode
	}
	bsNode = rbNode.(*binarySearchNodeMap)
	return bsNode
}

func (n *binarySearchNodeMap) shallowCopy() *binarySearchNodeMap {
	return &binarySearchNodeMap{
		key:         n.key,
		values:      sortAndDedupValues(n.values),
		colourIsRed: n.colourIsRed,
	}
}
