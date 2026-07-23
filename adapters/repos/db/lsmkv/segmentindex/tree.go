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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"sort"

	"github.com/pkg/errors"
)

type Nodes []Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return bytes.Compare(n[i].Key, n[j].Key) < 0 }

type Tree struct {
	nodes []*Node
}

type Node struct {
	Key   []byte
	Start uint64
	End   uint64
}

func NewTree(capacity int) Tree {
	return Tree{
		nodes: make([]*Node, 0, capacity),
	}
}

func NewBalanced(nodes Nodes) Tree {
	// A balanced binary tree in array form (children at 2i+1, 2i+2) needs
	// up to 2^ceil(log2(n+1)) slots. Pre-allocate to avoid grow() reallocations.
	capacity := balancedTreeCapacity(len(nodes))
	t := Tree{nodes: make([]*Node, capacity)}

	if len(nodes) > 0 {
		// sort the slice just once
		sort.Sort(nodes)
		t.buildBalanced(nodes, 0, 0, len(nodes)-1)
	}

	return t
}

// balancedTreeCapacity returns the array size needed to store a balanced
// binary tree with n nodes in heap-indexed layout.
func balancedTreeCapacity(n int) int {
	if n <= 0 {
		return 0
	}
	// smallest power of 2 > n, i.e. 2^ceil(log2(n+1))
	return 1 << bits.Len(uint(n))
}

func (t *Tree) buildBalanced(nodes []Node, targetPos, leftBound, rightBound int) {
	t.grow(targetPos)

	if leftBound > rightBound {
		return
	}

	mid := (leftBound + rightBound) / 2
	t.nodes[targetPos] = &nodes[mid]

	t.buildBalanced(nodes, t.left(targetPos), leftBound, mid-1)
	t.buildBalanced(nodes, t.right(targetPos), mid+1, rightBound)
}

func (t *Tree) Insert(key []byte, start, end uint64) {
	newNode := Node{
		Key:   key,
		Start: start,
		End:   end,
	}

	if len(t.nodes) == 0 {
		t.nodes = append(t.nodes, &newNode)
		return
	}

	t.insertAt(0, newNode)
}

func (t *Tree) insertAt(nodeID int, newNode Node) {
	if !t.exists(nodeID) {
		// we are at the target and can insert now
		t.grow(nodeID)
		t.nodes[nodeID] = &newNode
		return
	}

	if bytes.Equal(newNode.Key, t.nodes[nodeID].Key) {
		// this key already exists, which is an unexpected situation for an index
		// key
		panic(fmt.Sprintf("duplicate key %s", newNode.Key))
	}

	if bytes.Compare(newNode.Key, t.nodes[nodeID].Key) < 0 {
		t.insertAt(t.left(nodeID), newNode)
	} else {
		t.insertAt(t.right(nodeID), newNode)
	}
}

func (t *Tree) Get(key []byte) ([]byte, uint64, uint64) {
	if len(t.nodes) == 0 {
		return nil, 0, 0
	}

	return t.getAt(0, key)
}

func (t *Tree) getAt(nodeID int, key []byte) ([]byte, uint64, uint64) {
	if !t.exists(nodeID) {
		return nil, 0, 0
	}

	node := t.nodes[nodeID]
	if bytes.Equal(node.Key, key) {
		return node.Key, node.Start, node.End
	}

	if bytes.Compare(key, node.Key) < 0 {
		return t.getAt(t.left(nodeID), key)
	} else {
		return t.getAt(t.right(nodeID), key)
	}
}

func (t Tree) left(i int) int {
	return 2*i + 1
}

func (t Tree) right(i int) int {
	return 2*i + 2
}

func (t *Tree) exists(i int) bool {
	if i >= len(t.nodes) {
		return false
	}

	return t.nodes[i] != nil
}

// size calculates the exact size of this node on disk which is helpful to
// figure out the personal offset
func (n *Node) size() int {
	if n == nil {
		return 0
	}
	size := 0
	size += 4 // uint32 for key length
	size += len(n.Key)
	size += 8 // uint64 startPos
	size += 8 // uint64 endPos
	size += 8 // int64 pointer left child
	size += 8 // int64 pointer right child
	return size
}

func (t *Tree) grow(i int) {
	if i < len(t.nodes) {
		return
	}

	oldSize := len(t.nodes)
	newSize := oldSize
	for newSize <= i {
		newSize += oldSize
	}

	newNodes := make([]*Node, newSize)
	copy(newNodes, t.nodes)

	t.nodes = newNodes
}

func (t *Tree) MarshalBinary() ([]byte, error) {
	offsets, size := t.calculateDiskOffsets()

	buf := bytes.NewBuffer(nil)

	for i, node := range t.nodes {
		if node == nil {
			continue
		}

		var leftOffset int64
		var rightOffset int64

		if t.exists(t.left(i)) {
			leftOffset = int64(offsets[t.left(i)])
		} else {
			leftOffset = -1
		}

		if t.exists(t.right(i)) {
			rightOffset = int64(offsets[t.right(i)])
		} else {
			rightOffset = -1
		}

		if len(node.Key) > math.MaxUint32 {
			return nil, errors.Errorf("max key size is %d", math.MaxUint32)
		}

		keyLen := uint32(len(node.Key))
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write(node.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, node.Start); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, node.End); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, leftOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, rightOffset); err != nil {
			return nil, err
		}
	}
	bytes := buf.Bytes()
	if size != len(bytes) {
		return nil, errors.Errorf("corrupt: wrote %d bytes with target %d", len(bytes), size)
	}

	return bytes, nil
}

func (t *Tree) MarshalBinaryInto(w io.Writer) (int64, error) {
	offsets, size := t.calculateDiskOffsets()

	// create buf just once and reuse for each iteration, each iteration
	// overwrites every single byte of the buffer, so no initializing or
	// resetting after a round is required.
	buf := make([]byte, TREE_KEY_STORE_OVERHEAD) // 1x uint32 + 4x uint64

	for i, node := range t.nodes {
		if node == nil {
			continue
		}

		var leftOffset int64
		var rightOffset int64

		if t.exists(t.left(i)) {
			leftOffset = int64(offsets[t.left(i)])
		} else {
			leftOffset = -1
		}

		if t.exists(t.right(i)) {
			rightOffset = int64(offsets[t.right(i)])
		} else {
			rightOffset = -1
		}

		if len(node.Key) > math.MaxUint32 {
			return 0, errors.Errorf("max key size is %d", math.MaxUint32)
		}

		keyLen := uint32(len(node.Key))
		binary.LittleEndian.PutUint32(buf[0:4], keyLen)
		binary.LittleEndian.PutUint64(buf[4:12], node.Start)
		binary.LittleEndian.PutUint64(buf[12:20], node.End)
		binary.LittleEndian.PutUint64(buf[20:28], uint64(leftOffset))
		binary.LittleEndian.PutUint64(buf[28:36], uint64(rightOffset))

		if _, err := w.Write(buf[:4]); err != nil {
			return 0, err
		}
		if _, err := w.Write(node.Key); err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[4:36]); err != nil {
			return 0, err
		}
	}

	return int64(size), nil
}

// returns individual offsets and total size, nil nodes are skipped
func (t *Tree) calculateDiskOffsets() ([]int, int) {
	current := 0
	out := make([]int, len(t.nodes))
	for i, node := range t.nodes {
		out[i] = current
		size := node.size()
		current += size
	}

	return out, current
}

func (t *Tree) Height() int {
	var highestElem int
	for i := len(t.nodes) - 1; i >= 0; i-- {
		if t.nodes[i] != nil {
			highestElem = i
			break
		}
	}

	return int(math.Ceil(math.Log2(float64(highestElem))))
}

func (t *Tree) Size() int {
	size := 0
	for _, node := range t.nodes {
		size += node.size()
	}

	return size
}

// MarshalSortedKeys serializes a balanced BST index directly from sorted
// KeyRedux entries, without constructing intermediate Tree or Node structures.
// Keys must be in sorted order. Each key's ValueEnd is the absolute byte offset
// where that key's data ends in the segment file. dataStartOffset is where the
// first key's data begins — HeaderSize for most strategies, larger for inverted
// segments with an extended header.
func MarshalSortedKeys(w io.Writer, keys []KeyRedux, dataStartOffset uint64) (int64, error) {
	n := len(keys)
	if n == 0 {
		return 0, nil
	}

	// The first key's data spans [dataStartOffset, keys[0].ValueEnd]; a start past
	// the end would silently serialize a corrupt node, so reject it explicitly.
	if dataStartOffset > uint64(keys[0].ValueEnd) {
		return 0, fmt.Errorf("dataStartOffset %d exceeds first key's ValueEnd %d",
			dataStartOffset, keys[0].ValueEnd)
	}

	capacity := balancedTreeCapacity(n)

	// Build BFS-position → sorted-index mapping.
	// -1 means the position is empty (no node).
	bfsToSorted := make([]int32, capacity)
	for i := range bfsToSorted {
		bfsToSorted[i] = -1
	}
	fillBFS(bfsToSorted, 0, 0, n-1)

	// Emit nodes in van Emde Boas order; their offsets follow the write order,
	// so compute them in a first pass before writing.
	order := vebPositions(bfsToSorted)
	if err := checkAllNodesEmitted(order, n); err != nil {
		return 0, err
	}
	diskOffsets, totalSize := vebOffsets(order, capacity, func(p int32) int64 {
		// node size: keyLen(4) + key + start(8) + end(8) + left(8) + right(8)
		return int64(TREE_KEY_STORE_OVERHEAD + len(keys[bfsToSorted[p]].Key))
	})

	buf := make([]byte, TREE_KEY_STORE_OVERHEAD)

	for _, p := range order {
		si := bfsToSorted[p]
		key := keys[si]

		// Derive Start from the previous key's ValueEnd.
		var start uint64
		if si == 0 {
			start = dataStartOffset
		} else {
			start = uint64(keys[si-1].ValueEnd)
		}
		end := uint64(key.ValueEnd)

		leftChild := childOffset(diskOffsets, bfsToSorted, 2*int(p)+1, capacity)
		rightChild := childOffset(diskOffsets, bfsToSorted, 2*int(p)+2, capacity)

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key.Key)))
		binary.LittleEndian.PutUint64(buf[4:12], start)
		binary.LittleEndian.PutUint64(buf[12:20], end)
		binary.LittleEndian.PutUint64(buf[20:28], uint64(leftChild))
		binary.LittleEndian.PutUint64(buf[28:36], uint64(rightChild))

		if _, err := w.Write(buf[:4]); err != nil {
			return 0, err
		}
		if _, err := w.Write(key.Key); err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[4:36]); err != nil {
			return 0, err
		}
	}

	return totalSize, nil
}

// MarshalSortedKeysFromKeys serializes a balanced BST index directly from
// sorted Key entries, without constructing intermediate Tree or Node
// structures. Keys must already be in sorted order (as produced by the replace
// compactor's merge loop). ValueStart and ValueEnd are read directly from each
// Key, so no derivation is needed.
func MarshalSortedKeysFromKeys(w io.Writer, keys []Key) (int64, error) {
	n := len(keys)
	if n == 0 {
		return 0, nil
	}

	capacity := balancedTreeCapacity(n)

	bfsToSorted := make([]int32, capacity)
	for i := range bfsToSorted {
		bfsToSorted[i] = -1
	}
	fillBFS(bfsToSorted, 0, 0, n-1)

	order := vebPositions(bfsToSorted)
	if err := checkAllNodesEmitted(order, n); err != nil {
		return 0, err
	}
	diskOffsets, totalSize := vebOffsets(order, capacity, func(p int32) int64 {
		return int64(TREE_KEY_STORE_OVERHEAD + len(keys[bfsToSorted[p]].Key))
	})

	buf := make([]byte, TREE_KEY_STORE_OVERHEAD)

	for _, p := range order {
		key := keys[bfsToSorted[p]]

		leftChild := childOffset(diskOffsets, bfsToSorted, 2*int(p)+1, capacity)
		rightChild := childOffset(diskOffsets, bfsToSorted, 2*int(p)+2, capacity)

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key.Key)))
		binary.LittleEndian.PutUint64(buf[4:12], uint64(key.ValueStart))
		binary.LittleEndian.PutUint64(buf[12:20], uint64(key.ValueEnd))
		binary.LittleEndian.PutUint64(buf[20:28], uint64(leftChild))
		binary.LittleEndian.PutUint64(buf[28:36], uint64(rightChild))

		if _, err := w.Write(buf[:4]); err != nil {
			return 0, err
		}
		if _, err := w.Write(key.Key); err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[4:36]); err != nil {
			return 0, err
		}
	}

	return totalSize, nil
}

// computePrimaryIndexSize returns the serialized BST size for the primary index
// without allocating any tree structures. Every key occupies exactly one BST
// node of size TREE_KEY_STORE_OVERHEAD + len(key.Key).
func computePrimaryIndexSize(keys []Key) int64 {
	var size int64
	for _, key := range keys {
		size += int64(TREE_KEY_STORE_OVERHEAD + len(key.Key))
	}
	return size
}

// computeSecondaryIndexSize returns the serialized BST size for the secondary
// index at pos without allocating any tree structures.
func computeSecondaryIndexSize(keys []Key, pos int) int64 {
	var size int64
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			continue
		}
		size += int64(TREE_KEY_STORE_OVERHEAD + len(key.SecondaryKeys[pos]))
	}
	return size
}

// marshalSortedSecondaryFromKeys serializes a balanced BST for the secondary
// key at pos directly from the keys slice without constructing intermediate
// Tree or Node structures. It uses a []int32 index buffer (4 bytes/key) instead
// of []Node (40 bytes/key) to reduce memory usage during compaction.
func marshalSortedSecondaryFromKeys(w io.Writer, keys []Key, pos int) (int64, error) {
	// Collect original-key indices for keys that have a secondary key at pos.
	sortedIndices := make([]int32, 0, len(keys))
	for i, key := range keys {
		if pos < len(key.SecondaryKeys) {
			sortedIndices = append(sortedIndices, int32(i))
		}
	}
	n := len(sortedIndices)
	if n == 0 {
		return 0, nil
	}

	// Sort by secondary key value.
	slices.SortFunc(sortedIndices, func(a, b int32) int {
		return bytes.Compare(keys[a].SecondaryKeys[pos], keys[b].SecondaryKeys[pos])
	})

	capacity := balancedTreeCapacity(n)

	// Build BFS-position → position-in-sortedIndices mapping.
	bfsToIdx := make([]int32, capacity)
	for i := range bfsToIdx {
		bfsToIdx[i] = -1
	}
	fillBFS(bfsToIdx, 0, 0, n-1)

	// Emit nodes in van Emde Boas order; their offsets follow the write order,
	// so compute them in a first pass before writing.
	order := vebPositions(bfsToIdx)
	if err := checkAllNodesEmitted(order, n); err != nil {
		return 0, err
	}
	diskOffsets, totalSize := vebOffsets(order, capacity, func(p int32) int64 {
		ki := sortedIndices[bfsToIdx[p]]
		return int64(TREE_KEY_STORE_OVERHEAD + len(keys[ki].SecondaryKeys[pos]))
	})

	buf := make([]byte, TREE_KEY_STORE_OVERHEAD)
	for _, p := range order {
		ki := sortedIndices[bfsToIdx[p]]
		key := keys[ki]
		secKey := key.SecondaryKeys[pos]

		leftChild := childOffset(diskOffsets, bfsToIdx, 2*int(p)+1, capacity)
		rightChild := childOffset(diskOffsets, bfsToIdx, 2*int(p)+2, capacity)

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(secKey)))
		binary.LittleEndian.PutUint64(buf[4:12], uint64(key.ValueStart))
		binary.LittleEndian.PutUint64(buf[12:20], uint64(key.ValueEnd))
		binary.LittleEndian.PutUint64(buf[20:28], uint64(leftChild))
		binary.LittleEndian.PutUint64(buf[28:36], uint64(rightChild))

		if _, err := w.Write(buf[:4]); err != nil {
			return 0, err
		}
		if _, err := w.Write(secKey); err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[4:36]); err != nil {
			return 0, err
		}
	}

	return totalSize, nil
}

// fillBFS recursively maps heap-indexed BFS positions to sorted key indices.
func fillBFS(bfsToSorted []int32, targetPos, leftBound, rightBound int) {
	if leftBound > rightBound || targetPos >= len(bfsToSorted) {
		return
	}
	mid := (leftBound + rightBound) / 2
	bfsToSorted[targetPos] = int32(mid)
	fillBFS(bfsToSorted, 2*targetPos+1, leftBound, mid-1)
	fillBFS(bfsToSorted, 2*targetPos+2, mid+1, rightBound)
}

// vebPositions returns the heap positions of all present nodes in disk-write
// order, using the van Emde Boas layout. The nodes are ordered to keep each node
// near its children in the file, so a root-to-leaf lookup stays within a few
// pages.
//
// Level order (write all of depth 0, then all of depth 1, ...) does the opposite:
// a parent and its children fall in levels that grow exponentially, so they drift
// far apart, and a deep lookup hits a new page at almost every step.
//
// Van Emde Boas order keeps subtrees together instead. Cut the tree in half by
// height into one top subtree and many bottom subtrees, write the top subtree,
// then each bottom subtree, and order each of them the same way recursively. For
// a full height-4 tree (positions 0..14, children of p at 2p+1/2p+2) that is
// 0,1,2, 3,7,8, 4,9,10, 5,11,12, 6,13,14 — top subtree {0,1,2}, then the four
// bottom subtrees {3,7,8}, {4,9,10}, {5,11,12}, {6,13,14} — instead of level
// order's 0..14. A subtree of any size then occupies one contiguous run of bytes,
// so loading a page pulls in a whole subtree of the path rather than scattered
// nodes.
//
// The cut is by height, not by page size, on purpose: each cut makes subtrees
// about the square root in size of the level above, so their sizes fan out across
// scales. Whatever the page size is, some level of the recursion has subtrees
// about that big — the same layout is right for the 4 KiB page and the 64 B cache
// line at once, with no constant to tune. A root-to-leaf lookup then crosses into
// a new page only a handful of times.
//
// Page-sized blocking is the obvious alternative and was measured: it touches
// slightly fewer pages (4.1 vs 4.9 per lookup at 10M keys) but runs ~10% slower
// warm, because filling pages to the brim gives up cache-line locality. It also
// needs a page size baked into the writer, which is wrong on 16 KiB-page
// platforms, and any padding breaks AllKeys/KeyCount/ForEachKey, which assume
// nodes are contiguous.
//
// mapping[p] is >= 0 for a present node and -1 for an empty position; its length
// is the tree capacity and must be a power of two. The root (position 0) is
// always emitted first, so it lands at offset 0.
func vebPositions(mapping []int32) []int32 {
	out := make([]int32, 0, len(mapping))
	height := bits.Len(uint(len(mapping))) - 1 // capacity == 1<<height
	var rec func(root, h int)
	rec = func(root, h int) {
		if h <= 0 || root >= len(mapping) || mapping[root] < 0 {
			return
		}
		if h == 1 {
			out = append(out, int32(root))
			return
		}
		topH := (h + 1) / 2
		rec(root, topH)
		base := ((root + 1) << topH) - 1 // first descendant at relative depth topH
		for j := 0; j < 1<<topH; j++ {
			rec(base+j, h-topH)
		}
	}
	rec(0, height)
	return out
}

// checkAllNodesEmitted fails if vebPositions did not emit every key. vebPositions
// prunes on an empty heap position, which is safe only while every present node
// has a present parent (guaranteed by fillBFS); a dropped node would silently
// omit a key from the index.
func checkAllNodesEmitted(order []int32, n int) error {
	if len(order) != n {
		return fmt.Errorf("segment index writer emitted %d of %d nodes", len(order), n)
	}
	return nil
}

// vebOffsets assigns each present heap position its byte offset in the
// serialized output by walking positions in write order. In van Emde Boas
// order write order and offset order differ, so offsets need this separate pass
// before children can be pointed at. The returned slice is indexed by heap
// position (-1 for empty positions, which the reader never dereferences).
func vebOffsets(order []int32, capacity int, nodeSize func(heapPos int32) int64) (offsetOf []int64, total int64) {
	offsetOf = make([]int64, capacity)
	for i := range offsetOf {
		offsetOf[i] = -1
	}
	for _, p := range order {
		offsetOf[p] = total
		total += nodeSize(p)
	}
	return offsetOf, total
}

// childOffset returns the serialized byte offset of the child at heap position
// childPos, or -1 when that position holds no node.
func childOffset(offsetOf []int64, mapping []int32, childPos, capacity int) int64 {
	if childPos < capacity && mapping[childPos] >= 0 {
		return offsetOf[childPos]
	}
	return -1
}
