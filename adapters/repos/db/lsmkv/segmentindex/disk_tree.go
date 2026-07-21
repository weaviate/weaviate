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
	"errors"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const TREE_KEY_STORE_OVERHEAD = 36

// DiskTree is a read-only wrapper around a marshalled index search tree, which
// can be used for reading, but cannot change the underlying structure. It is
// thus perfectly suited as an index for an (immutable) LSM disk segment, but
// pretty much useless for anything else
type DiskTree struct {
	data []byte

	// dataEnd is the exclusive end of the segment's data region, set once by
	// SetDataEnd after open-time validation has run. Zero (the default)
	// disables the upper-bound half of the read-time node sanity check; the
	// start<=end ordering half always applies regardless.
	dataEnd uint64
}

type dtNode struct {
	key        []byte
	startPos   uint64
	endPos     uint64
	leftChild  int64
	rightChild int64
}

func NewDiskTree(data []byte) *DiskTree {
	return &DiskTree{
		data: data,
	}
}

// SetDataEnd records the segment's data region end for the read-time node
// sanity check (see Get and readNode). Called once, after construction, by
// the segment open path once the data region is known.
func (t *DiskTree) SetDataEnd(dataEnd uint64) {
	t.dataEnd = dataEnd
}

// validateNodeRange reports whether a node's [start, end) is well-formed:
// non-inverted, and within the data region when dataEnd is set. A corrupt
// node with end < start makes every caller's make([]byte, end-start) wrap
// to a huge uint64 and panic makeslice: len out of range; background
// readers (compaction, flush, bloom-rebuild, cursors) have no recover
// barrier for that panic, so this is caught here, once, at the point a
// node's range is read out of the tree.
func (t *DiskTree) validateNodeRange(start, end uint64) error {
	if end < start {
		return fmt.Errorf("node data range [%d,%d) is inverted", start, end)
	}
	if t.dataEnd > 0 && end > t.dataEnd {
		return fmt.Errorf("node data range [%d,%d) is outside the segment's data region (end %d)", start, end, t.dataEnd)
	}
	return nil
}

func (t *DiskTree) Get(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, lsmkv.NotFound
	}
	var out Node
	data := t.data
	dataLen := uint64(len(data))
	pos := uint64(0)

	// jump through the buffer until the node with _key_ is found or return a
	// NotFound error. Node keys are compared in place against the tree data, so
	// the descent allocates nothing; only the matched node's key is materialized
	// (callers may keep it beyond the underlying segment's lifetime).
	//
	// pos can be an arbitrary child offset read from possibly corrupt data, so
	// every read is bounds-checked against dataLen-pos (never pos+n, which would
	// wrap). Truncated or corrupt data yields NotFound or an error, never a panic.
	//
	// A legitimate descent visits at most as many nodes as the tree has total
	// nodes (worst case: a fully degenerate, linked-list-shaped BST). Child
	// offsets that are individually in-bounds can still form a cycle, which
	// would otherwise spin this loop forever - and the hung reader holds the
	// segment-group read view open, so Shutdown and compaction wedge behind
	// it. maxIterations is a generous, allocation-free upper bound (real
	// nodes are never smaller than TREE_KEY_STORE_OVERHEAD bytes) that never
	// cuts off a real walk.
	maxIterations := dataLen/TREE_KEY_STORE_OVERHEAD + 1
	for i := uint64(0); ; i++ {
		if i >= maxIterations {
			return out, fmt.Errorf("node walk exceeded %d iterations at pos %d; corrupt or cyclic index data", maxIterations, pos)
		}

		// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8].
		if pos+4 > dataLen || pos+4 < 4 {
			return out, lsmkv.NotFound
		}

		keyLen := uint64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if keyLen > dataLen-pos {
			return out, fmt.Errorf("node key at %d len %d out of range", pos, keyLen)
		}

		keyEqual := bytes.Compare(key, data[pos:pos+keyLen])
		pos += keyLen
		avail := dataLen - pos
		if keyEqual == 0 {
			if avail < 16 { // start + end
				return out, fmt.Errorf("node value at %d out of range", pos)
			}
			start := binary.LittleEndian.Uint64(data[pos:])
			end := binary.LittleEndian.Uint64(data[pos+8:])
			if err := t.validateNodeRange(start, end); err != nil {
				return out, err
			}
			out.Key = bytes.Clone(data[pos-keyLen : pos])
			out.Start = start
			out.End = end
			return out, nil
		} else if keyEqual < 0 {
			if avail < 24 { // start + end + left child
				return out, fmt.Errorf("node left child at %d out of range", pos)
			}
			pos = binary.LittleEndian.Uint64(data[pos+16:]) // skip start+end, read left child
		} else {
			if avail < 32 { // start + end + left + right child
				return out, fmt.Errorf("node right child at %d out of range", pos)
			}
			pos = binary.LittleEndian.Uint64(data[pos+24:]) // skip start+end+left, read right child
		}
	}
}

// ValidateRootInBounds is an O(1) check that the root node (if the tree is
// non-empty) falls within the given data region. It catches an in-bounds
// but corrupt IndexStart that would otherwise make Get() silently resolve
// to NotFound instead of erroring; every legitimately-written segment's
// root satisfies this by construction.
func (t *DiskTree) ValidateRootInBounds(dataStart, dataEnd uint64) error {
	if len(t.data) == 0 {
		return nil
	}
	root, err := t.readNodeAt(0)
	if err != nil {
		return fmt.Errorf("read root node: %w", err)
	}
	if root.startPos < dataStart || root.startPos > dataEnd ||
		root.endPos < dataStart || root.endPos > dataEnd {
		return fmt.Errorf(
			"root node data range [%d,%d) is outside the segment's data region [%d,%d)",
			root.startPos, root.endPos, dataStart, dataEnd,
		)
	}
	return nil
}

func (t *DiskTree) readNodeAt(offset int64) (dtNode, error) {
	// offset is a child pointer that may be corrupt; bound it before slicing so a
	// stray value yields an error instead of an out-of-range panic.
	if offset < 0 || offset > int64(len(t.data)) {
		return dtNode{}, fmt.Errorf("node offset %d out of range (buffer %d)", offset, len(t.data))
	}
	retNode, _, err := t.readNode(t.data[offset:])
	return retNode, err
}

func (t *DiskTree) readNode(in []byte) (dtNode, int, error) {
	var out dtNode
	// in buffer needs at least 36 bytes of data:
	// 4bytes for key length, 32bytes for position and children
	if len(in) < TREE_KEY_STORE_OVERHEAD {
		return out, 0, io.EOF
	}

	rw := byteops.NewReadWriter(in)

	keyLen := uint64(rw.ReadUint32())
	// the whole node is keyLen + TREE_KEY_STORE_OVERHEAD bytes; the len check
	// above only covers a zero-length key. Reject a keyLen that would push the
	// key or the fixed trailer past the buffer (corrupt/truncated index) so the
	// reads below cannot panic. Compared against len(in)-overhead to avoid wrap.
	if keyLen > uint64(len(in))-TREE_KEY_STORE_OVERHEAD {
		return out, int(rw.Position), fmt.Errorf("node key len %d out of range (buffer %d)", keyLen, len(in))
	}
	copiedBytes, err := rw.CopyBytesFromBuffer(keyLen, nil)
	if err != nil {
		return out, int(rw.Position), fmt.Errorf("copy node key: %w", err)
	}
	out.key = copiedBytes

	out.startPos = rw.ReadUint64()
	out.endPos = rw.ReadUint64()
	if err := t.validateNodeRange(out.startPos, out.endPos); err != nil {
		return out, int(rw.Position), err
	}
	out.leftChild = int64(rw.ReadUint64())
	out.rightChild = int64(rw.ReadUint64())
	return out, int(rw.Position), nil
}

func (t *DiskTree) Seek(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(0, key, true)
}

func (t *DiskTree) Next(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(0, key, false)
}

func (t *DiskTree) seekAt(offset int64, key []byte, includingKey bool) (Node, error) {
	node, err := t.readNodeAt(offset)
	if err != nil {
		return Node{}, err
	}

	self := Node{
		Key:   node.key,
		Start: node.startPos,
		End:   node.endPos,
	}

	if includingKey && bytes.Equal(key, node.key) {
		return self, nil
	}

	if bytes.Compare(key, node.key) < 0 {
		if node.leftChild < 0 {
			return self, nil
		}

		left, err := t.seekAt(node.leftChild, key, includingKey)
		if err == nil {
			return left, nil
		}

		if errors.Is(err, lsmkv.NotFound) {
			return self, nil
		}

		return Node{}, err
	} else {
		if node.rightChild < 0 {
			return Node{}, lsmkv.NotFound
		}

		return t.seekAt(node.rightChild, key, includingKey)
	}
}

// AllKeys is a relatively expensive operation as it basically does a full disk
// read of the index. It is meant for one of operations, such as initializing a
// segment where we need access to all keys, e.g. to build a bloom filter. This
// should not run at query time.
//
// The binary tree is traversed in Level-Order so keys have no meaningful
// order. Do not use this method if an In-Order traversal is required, but only
// for use cases who don't require a specific order, such as building a
// bloom filter.
func (t *DiskTree) AllKeys() ([][]byte, error) {
	var out [][]byte
	bufferPos := 0
	for {
		node, readLength, err := t.readNode(t.data[bufferPos:])
		bufferPos += readLength
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		out = append(out, node.key)
	}

	return out, nil
}

func (t *DiskTree) Size() int {
	return len(t.data)
}

// KeyCount returns the number of keys in the tree without allocating.
// It walks through the serialized nodes, skipping over each one.
func (t *DiskTree) KeyCount() int {
	count := 0
	bufferPos := 0
	// each node: 4 (keyLen) + keyLen + 8 (start) + 8 (end) + 8 (left) + 8 (right)
	for bufferPos+TREE_KEY_STORE_OVERHEAD <= len(t.data) {
		keyLen := int(binary.LittleEndian.Uint32(t.data[bufferPos:]))
		nodeSize := keyLen + TREE_KEY_STORE_OVERHEAD
		if bufferPos+nodeSize > len(t.data) {
			break
		}
		bufferPos += nodeSize
		count++
	}
	return count
}

// ForEachKey iterates over all keys in the tree without allocating a slice.
// The key passed to fn is a subslice of the underlying data and must not
// be retained or modified by the caller.
func (t *DiskTree) ForEachKey(fn func(key []byte)) {
	bufferPos := 0
	for bufferPos+TREE_KEY_STORE_OVERHEAD <= len(t.data) {
		keyLen := int(binary.LittleEndian.Uint32(t.data[bufferPos:]))
		nodeSize := keyLen + TREE_KEY_STORE_OVERHEAD
		if bufferPos+nodeSize > len(t.data) {
			break
		}
		fn(t.data[bufferPos+4 : bufferPos+4+keyLen])
		bufferPos += nodeSize
	}
}
