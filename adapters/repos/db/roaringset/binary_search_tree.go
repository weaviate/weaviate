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
	"unsafe"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type BinarySearchTree struct {
	root        *BinarySearchNode
	count       int
	sizeInBytes int
}

type Insert struct {
	Additions []uint64
	Deletions []uint64
}

// Insert ignores a write carrying neither additions nor deletions, since a node
// for it would be charged its own structure. Get then reports lsmkv.NotFound for
// such a key rather than an empty layer, which the layer merge treats alike.
func (t *BinarySearchTree) Insert(key []byte, values Insert) {
	if len(values.Additions) == 0 && len(values.Deletions) == 0 {
		return
	}

	if t.root == nil {
		t.root = newBinarySearchNode(key, values, nil, false) // root node is always black
		t.count = 1
		t.sizeInBytes = t.root.sizeInBytes()
		return
	}

	newRoot, grownBy, created := t.root.insert(key, values)
	if created {
		t.count++
	}
	if newRoot != nil {
		t.root = newRoot
	}
	t.sizeInBytes += grownBy
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

// SizeInBytes approximates the heap the tree holds, and is zero exactly when the
// tree holds no nodes. It undercounts by each bitmap's len-to-cap slack, and by
// more where a caller hands Insert a key that is a subslice of a larger buffer.
func (t *BinarySearchTree) SizeInBytes() uint64 {
	return uint64(t.sizeInBytes)
}

// Get creates copies of underlying bitmaps to prevent future (concurrent)
// read and writes after layer being returned
func (t *BinarySearchTree) Get(key []byte) (BitmapLayer, error) {
	if t.root == nil {
		return BitmapLayer{}, lsmkv.NotFound
	}

	return t.root.get(key)
}

// FlattenInOrder creates list of ordered copies of bst nodes
// Only Key and Value fields are populated
// The copies carry the source bitmaps' layout, so a caller serializing them
// must pass them through [Condense] itself.
func (t *BinarySearchTree) FlattenInOrder() []*BinarySearchNode {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

// Count returns how many distinct keys the tree holds. A key inserted with only
// deletions counts, and the tree has no removal, so the number only grows. The
// caller's own lock guards it, as it does the rest of the tree.
func (t *BinarySearchTree) Count() int {
	return t.count
}

type BinarySearchNode struct {
	Key         []byte
	Value       BitmapLayer
	left        *BinarySearchNode
	right       *BinarySearchNode
	parent      *BinarySearchNode
	colourIsRed bool
}

func (n *BinarySearchNode) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *BinarySearchNode) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*BinarySearchNode)
}

func (n *BinarySearchNode) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *BinarySearchNode) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*BinarySearchNode)
}

func (n *BinarySearchNode) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *BinarySearchNode) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeRoaringSetReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*BinarySearchNode)
}

func (n *BinarySearchNode) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *BinarySearchNode) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *BinarySearchNode) IsNil() bool {
	return n == nil
}

func addNewSearchNodeRoaringSetReceiver(nodePtr **BinarySearchNode) {
	*nodePtr = &BinarySearchNode{}
}

// nodeFixedSizeInBytes is the heap a node holds whatever it contains: the node
// itself plus the two sroar.Bitmap structs its layer points at.
const nodeFixedSizeInBytes = int(unsafe.Sizeof(BinarySearchNode{}) + 2*unsafe.Sizeof(sroar.Bitmap{}))

func (n *BinarySearchNode) sizeInBytes() int {
	return nodeFixedSizeInBytes + len(n.Key) + n.bitmapsSizeInBytes()
}

// bitmapsSizeInBytes uses sroar.Bitmap.LenInBytes, which counts an
// allocated-but-empty buffer; BitmapLayer.LenInBytes reads one as free.
func (n *BinarySearchNode) bitmapsSizeInBytes() int {
	return n.Value.Additions.LenInBytes() + n.Value.Deletions.LenInBytes()
}

func newBinarySearchNode(key []byte, values Insert, parent *BinarySearchNode, colourIsRed bool) *BinarySearchNode {
	return &BinarySearchNode{
		Key: key,
		Value: BitmapLayer{
			Additions: NewBitmap(values.Additions...),
			Deletions: NewBitmap(values.Deletions...),
		},
		parent:      parent,
		colourIsRed: colourIsRed,
	}
}

// insert reports the bytes the subtree grew by and whether it added a node, both
// of which the caller totals. A nil node means the root did not move, not that
// the key merged.
func (n *BinarySearchNode) insert(key []byte, values Insert) (*BinarySearchNode, int, bool) {
	if bytes.Equal(key, n.Key) {
		bitmapsBefore := n.bitmapsSizeInBytes()

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
		for _, x := range values.Additions {
			n.Value.Deletions.Remove(x)
			n.Value.Additions.Set(x)
		}

		for _, x := range values.Deletions {
			n.Value.Additions.Remove(x)
			n.Value.Deletions.Set(x)
		}

		return nil, n.bitmapsSizeInBytes() - bitmapsBefore, false
	}

	if bytes.Compare(key, n.Key) < 0 {
		if n.left != nil {
			return n.left.insert(key, values)
		} else {
			// held in a local because Rebalance can move it out of n.left
			inserted := newBinarySearchNode(key, values, n, true)
			n.left = inserted
			return BinarySearchNodeFromRB(rbtree.Rebalance(inserted)), inserted.sizeInBytes(), true
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, values)
		} else {
			// held in a local because Rebalance can move it out of n.right
			inserted := newBinarySearchNode(key, values, n, true)
			n.right = inserted
			return BinarySearchNodeFromRB(rbtree.Rebalance(inserted)), inserted.sizeInBytes(), true
		}
	}
}

func (n *BinarySearchNode) get(key []byte) (BitmapLayer, error) {
	if bytes.Equal(n.Key, key) {
		return n.Value.Clone(), nil
	}

	if bytes.Compare(key, n.Key) < 0 {
		if n.left == nil {
			return BitmapLayer{}, lsmkv.NotFound
		}

		return n.left.get(key)
	} else {
		if n.right == nil {
			return BitmapLayer{}, lsmkv.NotFound
		}

		return n.right.get(key)
	}
}

func BinarySearchNodeFromRB(rbNode rbtree.Node) (bsNode *BinarySearchNode) {
	if rbNode == nil {
		bsNode = nil
		return bsNode
	}
	bsNode = rbNode.(*BinarySearchNode)
	return bsNode
}

func (n *BinarySearchNode) flattenInOrder() []*BinarySearchNode {
	var left []*BinarySearchNode
	var right []*BinarySearchNode

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	// Node's Value has to be copied, not to be mutated when BST is updated.
	right = append([]*BinarySearchNode{n.shallowCopy()}, right...)
	return append(left, right...)
}

func (n *BinarySearchNode) shallowCopy() *BinarySearchNode {
	// Clone copies the buffer, so the copy carries whatever slack the source has.
	// Only read cursors reach this, and none of them writes a segment, so the
	// compacted layout Condense produces would buy them no smaller file.
	return &BinarySearchNode{
		Key:   n.Key,
		Value: n.Value.Clone(),
	}
}
