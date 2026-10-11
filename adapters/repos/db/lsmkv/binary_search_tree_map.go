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

type mapElement[V any] interface {
	// implemented per element type, ignoring the receiver: a body generic over V
	// reaches its comparisons through the type dictionary, which costs a quarter
	// of the runtime here. See BenchmarkSortAndDedupValues.
	sortAndDedup(values []V) []V
}

// the type parameters below already enforce this. Spelling it out is what lets
// staticcheck see the two sortAndDedup implementations as reachable.
var (
	_ mapElement[MapPair]      = MapPair{}
	_ mapElement[invertedPair] = invertedPair{}
)

type binarySearchTreeMap[V mapElement[V]] struct {
	root *binarySearchNodeMap[V]
}

func (t *binarySearchTreeMap[V]) insert(key []byte, pair V) {
	if t.root == nil {
		t.root = &binarySearchNodeMap[V]{
			key:         key,
			values:      []V{pair},
			colourIsRed: false, // root node is always black
		}
		return
	}

	if newRoot := t.root.insert(key, pair); newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

func (t *binarySearchTreeMap[V]) get(key []byte) ([]V, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTreeMap[V]) flattenInOrder() []*binarySearchNodeMap[V] {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeMap[V mapElement[V]] struct {
	key         []byte
	values      []V
	left        *binarySearchNodeMap[V]
	right       *binarySearchNodeMap[V]
	parent      *binarySearchNodeMap[V]
	colourIsRed bool
}

func (n *binarySearchNodeMap[V]) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNodeMap[V]) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNodeMap[V])
}

func (n *binarySearchNodeMap[V]) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNodeMap[V]) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNodeMap[V])
}

func (n *binarySearchNodeMap[V]) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNodeMap[V]) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNodeMap[V])
}

func (n *binarySearchNodeMap[V]) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNodeMap[V]) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNodeMap[V]) IsNil() bool {
	return n == nil
}

func addNewSearchNodeMapReceiver[V mapElement[V]](nodePtr **binarySearchNodeMap[V]) {
	*nodePtr = &binarySearchNodeMap[V]{}
}

func (n *binarySearchNodeMap[V]) insert(key []byte, pair V) *binarySearchNodeMap[V] {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, pair)
		return nil // tree root does not change when replacing node
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			return n.left.insert(key, pair)
		} else {
			n.left = &binarySearchNodeMap[V]{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []V{pair},
			}
			return binarySearchNodeMapFromRB[V](rbtree.Rebalance(n.left))
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, pair)
		} else {
			n.right = &binarySearchNodeMap[V]{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []V{pair},
			}
			return binarySearchNodeMapFromRB[V](rbtree.Rebalance(n.right))
		}
	}
}

func (n *binarySearchNodeMap[V]) get(key []byte) ([]V, error) {
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

func (n *binarySearchNodeMap[V]) flattenInOrder() []*binarySearchNodeMap[V] {
	// preallocate capacity to avoid repeated reallocations
	size := n.subtreeSize()
	res := make([]*binarySearchNodeMap[V], 0, size)
	return n.appendInOrder(res)
}

func (n *binarySearchNodeMap[V]) appendInOrder(dst []*binarySearchNodeMap[V]) []*binarySearchNodeMap[V] {
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

func (n *binarySearchNodeMap[V]) subtreeSize() int {
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

func sortAndDedupValues[V mapElement[V]](in []V) []V {
	var v V
	return v.sortAndDedup(in)
}

func compareMapPairByKey(a, b MapPair) int {
	return bytes.Compare(a.Key, b.Key)
}

// takes a list of MapPair and sorts it while keeping the original order. Then
// removes redundancies (from updates or deletes after previous inserts) using
// a simple deduplication process.
func (kv MapPair) sortAndDedup(values []MapPair) []MapPair {
	out := make([]MapPair, len(values))
	copy(out, values)

	// the sort must be stable: the dedup below keeps the last of a run of equal
	// keys, which is only the newest write if insert order survives. The sorted
	// check skips the sort on buckets whose map keys are BigEndian doc IDs, as
	// those arrive ascending. See BenchmarkSortAndDedupValues.
	if !slices.IsSortedFunc(out, compareMapPairByKey) {
		slices.SortStableFunc(out, compareMapPairByKey)
	}

	// now deduping is as simple as looking one key ahead - if it's the same key
	// simply skip the current element. Meaning "out" will be a subset of
	// (sorted) "in".
	outIndex := 0
	for inIndex, pair := range out {
		// look ahead
		if inIndex+1 < len(out) && bytes.Equal(out[inIndex+1].Key, pair.Key) {
			continue
		}

		out[outIndex] = pair
		outIndex++
	}

	return out[:outIndex]
}

func binarySearchNodeMapFromRB[V mapElement[V]](rbNode rbtree.Node) (bsNode *binarySearchNodeMap[V]) {
	if rbNode == nil {
		bsNode = nil
		return bsNode
	}
	bsNode = rbNode.(*binarySearchNodeMap[V])
	return bsNode
}

func (n *binarySearchNodeMap[V]) shallowCopy() *binarySearchNodeMap[V] {
	return &binarySearchNodeMap[V]{
		key:         n.key,
		values:      sortAndDedupValues(n.values),
		colourIsRed: n.colourIsRed,
	}
}
