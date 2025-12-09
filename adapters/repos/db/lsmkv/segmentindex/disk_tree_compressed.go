//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type DiskTreeCompressed struct {
	data         []byte
	keySize      uint32
	fixedKeySize bool
	dataOffset   int // Offset where actual tree data starts (after header)
}

func NewDiskTreeCompressed(data []byte) *DiskTreeCompressed {
	t := &DiskTreeCompressed{
		data: data,
	}

	// Read the compressed tree header
	if len(data) >= compressedTreeHeaderSize {
		t.readCompressedTreeHeader()
	}

	return t
}

// readCompressedTreeHeader reads the header from the compressed tree data
// Header format:
// - Version: 1 byte
// - Flags: 1 byte (bit 0: fixed_key_size)
// - Key size: 4 bytes (0 if variable)
func (t *DiskTreeCompressed) readCompressedTreeHeader() {
	pos := 0

	// Read version (currently unused, for future compatibility)
	_ = t.data[pos]
	pos++

	// Read flags
	flags := t.data[pos]
	pos++
	t.fixedKeySize = (flags & 0x01) != 0

	// Read key size
	t.keySize = binary.LittleEndian.Uint32(t.data[pos : pos+4])
	pos += 4

	t.dataOffset = pos
}

// decompressByteSize unpacks the bit-packed header
func decompressByteSize(headerBytes []byte) (lenKeyBytes, nodeStartBytesLen, nodeEndBytesLen, leftOffsetBytesLen, rightOffsetBytesLen int) {
	headerInt := uint16(headerBytes[0]) | (uint16(headerBytes[1]) << 8)
	lenKeyBytes = int(headerInt & 0b11)
	nodeStartBytesLen = int((headerInt >> 2) & 0b111)
	nodeEndBytesLen = int((headerInt >> 5) & 0b111)
	leftOffsetBytesLen = int((headerInt >> 8) & 0b111)
	rightOffsetBytesLen = int((headerInt >> 11) & 0b111)
	return
}

// decompressNumber reads a little-endian number from bytes
func decompressNumber(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}
	var result uint64
	for i := range data {
		result |= uint64(data[i]) << (8 * i)
	}
	return result
}

// readCompressedNodeAt reads and decompresses a node at the given offset
// offset is relative to the start of the tree data (after header)
func (t *DiskTreeCompressed) readCompressedNodeAt(offset int64, keyOnly bool) (dtNode, int, error) {
	// Adjust offset to account for header
	absoluteOffset := offset

	if absoluteOffset < int64(t.dataOffset) || absoluteOffset >= int64(len(t.data)) {
		return dtNode{}, 0, lsmkv.NotFound
	}

	data := t.data[absoluteOffset:]
	if len(data) < 2 {
		return dtNode{}, 0, io.EOF
	}

	var node dtNode
	pos := 0

	// Read and decompress header
	headerBytes := data[pos : pos+2]
	pos += 2

	lenKeyBytes, nodeStartBytesLen, nodeEndBytesLen, leftOffsetBytesLen, rightOffsetBytesLen := decompressByteSize(headerBytes)

	// Read key length if present
	var lenKey int
	if lenKeyBytes > 0 {
		if pos+lenKeyBytes > len(data) {
			return node, 0, io.EOF
		}
		lenKey = int(decompressNumber(data[pos : pos+lenKeyBytes]))
		pos += lenKeyBytes
	} else {
		lenKey = int(t.keySize)
	}

	// Read key
	if pos+lenKey > len(data) {
		return node, 0, io.EOF
	}
	node.key = make([]byte, lenKey)
	copy(node.key, data[pos:pos+lenKey])
	pos += lenKey

	if keyOnly {
		pos += nodeStartBytesLen + nodeEndBytesLen + leftOffsetBytesLen + rightOffsetBytesLen
		return node, pos, nil
	}
	// Read node start position
	if pos+nodeStartBytesLen > len(data) {
		return node, 0, io.EOF
	}
	nodeStart := decompressNumber(data[pos : pos+nodeStartBytesLen])
	pos += nodeStartBytesLen
	node.startPos = nodeStart

	// Read node size (delta encoded)
	if pos+nodeEndBytesLen > len(data) {
		return node, 0, io.EOF
	}
	nodeSize := decompressNumber(data[pos : pos+nodeEndBytesLen])
	pos += nodeEndBytesLen
	node.endPos = nodeStart + nodeSize

	// Read left offset (delta encoded)
	var leftOffsetDelta int64
	if leftOffsetBytesLen > 0 {
		if pos+leftOffsetBytesLen > len(data) {
			return node, 0, io.EOF
		}
		leftOffsetDelta = int64(decompressNumber(data[pos : pos+leftOffsetBytesLen]))
		pos += leftOffsetBytesLen
	}

	// Read right offset (delta encoded)
	var rightOffsetDelta int64
	if rightOffsetBytesLen > 0 {
		if pos+rightOffsetBytesLen > len(data) {
			return node, 0, io.EOF
		}
		rightOffsetDelta = int64(decompressNumber(data[pos : pos+rightOffsetBytesLen]))
		pos += rightOffsetBytesLen
	}

	// Delta decode left offset
	if leftOffsetDelta != 0 {
		node.leftChild = leftOffsetDelta + absoluteOffset
	} else {
		node.leftChild = -1
	}

	// Delta decode right offset
	if rightOffsetDelta != 0 {
		if node.leftChild != -1 {
			// Right was relative to left
			node.rightChild = rightOffsetDelta + node.leftChild
		} else {
			// Right was relative to previous entry start
			node.rightChild = rightOffsetDelta + absoluteOffset
		}
	} else {
		node.rightChild = -1
	}

	return node, pos, nil
}

// readCompressedNodeAt reads and decompresses a node at the given offset
// offset is relative to the start of the tree data (after header)
func (t *DiskTreeCompressed) readCompressedNodeAtReusable(offset int64, keyOnly bool, node *dtNode) (int, error) {
	// Adjust offset to account for header
	absoluteOffset := offset

	if absoluteOffset < int64(t.dataOffset) || absoluteOffset >= int64(len(t.data)) {
		return 0, lsmkv.NotFound
	}

	data := t.data[absoluteOffset:]
	if len(data) < 2 {
		return 0, io.EOF
	}

	pos := 0

	// Read and decompress header
	headerBytes := data[pos : pos+2]
	pos += 2

	lenKeyBytes, nodeStartBytesLen, nodeEndBytesLen, leftOffsetBytesLen, rightOffsetBytesLen := decompressByteSize(headerBytes)

	// Read key length if present
	var lenKey int
	if lenKeyBytes > 0 {
		if pos+lenKeyBytes > len(data) {
			return 0, io.EOF
		}
		lenKey = int(decompressNumber(data[pos : pos+lenKeyBytes]))
		pos += lenKeyBytes
	} else {
		lenKey = int(t.keySize)
	}

	// Read key
	if pos+lenKey > len(data) {
		return 0, io.EOF
	}
	if len(node.key) < lenKey {
		node.key = make([]byte, lenKey)
	}
	copy(node.key, data[pos:pos+lenKey])
	node.key = node.key[:lenKey]
	pos += lenKey

	if keyOnly {
		pos += nodeStartBytesLen + nodeEndBytesLen + leftOffsetBytesLen + rightOffsetBytesLen
		return pos, nil
	}
	// Read node start position
	if pos+nodeStartBytesLen > len(data) {
		return 0, io.EOF
	}
	nodeStart := decompressNumber(data[pos : pos+nodeStartBytesLen])
	pos += nodeStartBytesLen
	node.startPos = nodeStart

	// Read node size (delta encoded)
	if pos+nodeEndBytesLen > len(data) {
		return 0, io.EOF
	}
	nodeSize := decompressNumber(data[pos : pos+nodeEndBytesLen])
	pos += nodeEndBytesLen
	node.endPos = nodeStart + nodeSize

	// Read left offset (delta encoded)
	var leftOffsetDelta int64
	if leftOffsetBytesLen > 0 {
		if pos+leftOffsetBytesLen > len(data) {
			return 0, io.EOF
		}
		leftOffsetDelta = int64(decompressNumber(data[pos : pos+leftOffsetBytesLen]))
		pos += leftOffsetBytesLen
	}

	// Read right offset (delta encoded)
	var rightOffsetDelta int64
	if rightOffsetBytesLen > 0 {
		if pos+rightOffsetBytesLen > len(data) {
			return 0, io.EOF
		}
		rightOffsetDelta = int64(decompressNumber(data[pos : pos+rightOffsetBytesLen]))
		pos += rightOffsetBytesLen
	}

	// Delta decode left offset
	if leftOffsetDelta != 0 {
		node.leftChild = leftOffsetDelta + absoluteOffset
	} else {
		node.leftChild = -1
	}

	// Delta decode right offset
	if rightOffsetDelta != 0 {
		if node.leftChild != -1 {
			// Right was relative to left
			node.rightChild = rightOffsetDelta + node.leftChild
		} else {
			// Right was relative to previous entry start
			node.rightChild = rightOffsetDelta + absoluteOffset
		}
	} else {
		node.rightChild = -1
	}

	return pos, nil
}

func (t *DiskTreeCompressed) Get(key []byte) (Node, error) {
	if len(t.data) <= t.dataOffset {
		return Node{}, lsmkv.NotFound
	}

	return t.getAt(int64(t.dataOffset), key)
}

func (t *DiskTreeCompressed) getAt(offset int64, key []byte) (Node, error) {
	node, _, err := t.readCompressedNodeAt(offset, false)
	if err != nil {
		return Node{}, err
	}

	cmp := bytes.Compare(key, node.key)
	if cmp == 0 {
		return Node{
			Key:   node.key,
			Start: node.startPos,
			End:   node.endPos,
		}, nil
	} else if cmp < 0 {
		if node.leftChild < 0 {
			return Node{}, lsmkv.NotFound
		}
		return t.getAt(node.leftChild, key)
	} else {
		if node.rightChild < 0 {
			return Node{}, lsmkv.NotFound
		}
		return t.getAt(node.rightChild, key)
	}
}

func (t *DiskTreeCompressed) readNodeAt(offset int64) (dtNode, error) {
	node, _, err := t.readCompressedNodeAt(offset, false)
	if err != nil {
		return dtNode{}, err
	}

	return dtNode{
		key:        node.key,
		startPos:   node.startPos,
		endPos:     node.endPos,
		leftChild:  node.leftChild,
		rightChild: node.rightChild,
	}, nil
}

func (t *DiskTreeCompressed) Seek(key []byte) (Node, error) {
	if len(t.data) <= t.dataOffset {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(int64(t.dataOffset), key, true)
}

func (t *DiskTreeCompressed) seekAt(offset int64, key []byte, includingKey bool) (Node, error) {
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
func (t *DiskTreeCompressed) AllKeys() ([][]byte, error) {
	var out [][]byte
	pos := int64(t.dataOffset)
	var node dtNode
	if t.fixedKeySize {
		node.key = make([]byte, t.keySize)
	}
	for pos < int64(len(t.data)) {
		bytesRead, err := t.readCompressedNodeAtReusable(pos, true, &node)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		keyCopy := make([]byte, len(node.key))
		copy(keyCopy, node.key)
		out = append(out, keyCopy)

		pos += int64(bytesRead)
	}

	return out, nil
}

func (t *DiskTreeCompressed) Size() int {
	return len(t.data)
}

func (t *DiskTreeCompressed) Data() []byte {
	return t.data
}

func (t *DiskTreeCompressed) Next(key []byte) (Node, error) {
	return t.Get(key)
}
