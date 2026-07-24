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

// GetOffsets walks the tree for key and returns the payload position
// (start, end) of the matching node, or lsmkv.NotFound. It allocates nothing:
// node keys are compared in place against the tree data and no key is
// materialized. Prefer it on hot read paths that only need the payload offsets
// and never read Node.Key.
//
// pos can be an arbitrary child offset read from possibly corrupt data, so
// every read is bounds-checked against dataLen-pos (never pos+n, which would
// wrap). Truncated or corrupt data yields NotFound or an error, never a panic.
func (t *DiskTree) GetOffsets(key []byte) (start, end uint64, err error) {
	if len(t.data) == 0 {
		return 0, 0, lsmkv.NotFound
	}
	data := t.data
	dataLen := uint64(len(data))
	pos := uint64(0)
	steps := 0
	maxSteps := maxDescentSteps(len(data))

	for {
		// A child pointer leading back to an already visited node would keep the
		// descent going forever. No descent visits a node twice, so it cannot take
		// more steps than the buffer can hold nodes.
		steps++
		if steps > maxSteps {
			return 0, 0, errors.New("cyclic child pointers in segment index")
		}

		// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8].
		if pos+4 > dataLen || pos+4 < 4 {
			return 0, 0, lsmkv.NotFound
		}

		keyLen := uint64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if keyLen > dataLen-pos {
			return 0, 0, fmt.Errorf("node key at %d len %d out of range", pos, keyLen)
		}

		keyEqual := bytes.Compare(key, data[pos:pos+keyLen])
		pos += keyLen
		avail := dataLen - pos
		if keyEqual == 0 {
			if avail < 16 { // start + end
				return 0, 0, fmt.Errorf("node value at %d out of range", pos)
			}
			return binary.LittleEndian.Uint64(data[pos:]),
				binary.LittleEndian.Uint64(data[pos+8:]), nil
		} else if keyEqual < 0 {
			if avail < 24 { // start + end + left child
				return 0, 0, fmt.Errorf("node left child at %d out of range", pos)
			}
			pos = binary.LittleEndian.Uint64(data[pos+16:]) // skip start+end, read left child
		} else {
			if avail < 32 { // start + end + left + right child
				return 0, 0, fmt.Errorf("node right child at %d out of range", pos)
			}
			pos = binary.LittleEndian.Uint64(data[pos+24:]) // skip start+end+left, read right child
		}
	}
}

// maxDescentSteps bounds a root-to-leaf descent at the number of nodes a buffer
// of this size can hold, which no descent following intact child pointers
// reaches. It is what makes a cyclic pointer terminate.
func maxDescentSteps(dataLen int) int {
	return dataLen/TREE_KEY_STORE_OVERHEAD + 1
}

// Get returns the matching node with an owned copy of its key, safe to keep
// beyond the underlying segment's lifetime. A match requires exact key equality,
// so the matched node's key is byte-identical to the argument key — Get clones
// the caller's key rather than the (equal) in-tree bytes, which keeps the whole
// allocation-free descent in GetOffsets.
func (t *DiskTree) Get(key []byte) (Node, error) {
	start, end, err := t.GetOffsets(key)
	if err != nil {
		return Node{}, err
	}
	return Node{Key: bytes.Clone(key), Start: start, End: end}, nil
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
	out.leftChild = int64(rw.ReadUint64())
	out.rightChild = int64(rw.ReadUint64())
	return out, int(rw.Position), nil
}

func (t *DiskTree) Seek(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(0, key, true, maxDescentSteps(len(t.data)))
}

func (t *DiskTree) Next(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(0, key, false, maxDescentSteps(len(t.data)))
}

// budget is how many more nodes the descent may visit, so a child pointer
// leading back to an already visited node fails instead of recursing forever.
func (t *DiskTree) seekAt(offset int64, key []byte, includingKey bool, budget int) (Node, error) {
	if budget <= 0 {
		return Node{}, errors.New("cyclic child pointers in segment index")
	}

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

		left, err := t.seekAt(node.leftChild, key, includingKey, budget-1)
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

		return t.seekAt(node.rightChild, key, includingKey, budget-1)
	}
}

// AllKeys is a relatively expensive operation as it basically does a full disk
// read of the index. It is meant for one of operations, such as initializing a
// segment where we need access to all keys, e.g. to build a bloom filter. This
// should not run at query time.
//
// Keys are returned in the tree's on-disk (serialized) order, which is not
// sorted. Do not use this method if an In-Order traversal is required, but only
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
