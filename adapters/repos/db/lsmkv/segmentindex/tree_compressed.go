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
	"io"
	"math"

	"github.com/pkg/errors"
)

const (
	compressedTreeHeaderSize = 6 // version(1) + flags(1) + keySize(4)
)

func getNecessaryBytes(number uint64) int {
	if number == 0 {
		return 0
	}
	bytes := 0
	for number > 0 {
		bytes++
		number >>= 8
	}
	return bytes
}

func (n *Node) compressedSize(fixedKeySize bool, offsetSizes int, hasLeft, hasRight bool) int {
	if n == nil {
		return 0
	}
	size := 2 // header
	if !fixedKeySize {
		size += getNecessaryBytes(uint64(len(n.Key)))
	}
	size += len(n.Key)
	size += getNecessaryBytes(n.Start)
	size += getNecessaryBytes(n.End - n.Start)
	if hasLeft {
		size += offsetSizes // int64 pointer left child
	}
	if hasRight {
		size += offsetSizes // int64 pointer right child
	}
	return size
}

// returns individual offsets and total size, nil nodes are skipped
func (t *Tree) calculateCompressedDiskOffsets(current uint64) ([]uint64, uint64, int, bool) {
	maxTreeSize := 0
	nodeOffsetBytesFirstPass := 8
	isKeyFixedSize := true
	keySize := 0

	for i, node := range t.nodes {
		if node == nil {
			continue
		}
		if i == 0 {
			keySize = len(node.Key)
		}
		if i > 0 && len(node.Key) != keySize {
			isKeyFixedSize = false
			break
		}
	}

	countChildren := 0
	for i, node := range t.nodes {
		if node == nil {
			continue
		}

		hasLeft := t.exists(t.left(i))
		hasRight := t.exists(t.right(i))
		if hasLeft {
			countChildren++
		}
		if hasRight {
			countChildren++
		}
		maxTreeSize += node.compressedSize(i != 0 && isKeyFixedSize, nodeOffsetBytesFirstPass, hasLeft, hasRight)
	}
	offsets := make([]uint64, len(t.nodes))
	nodeOffsetBytesSecondPass := getNecessaryBytes(uint64(maxTreeSize))

	for nodeOffsetBytesFirstPass != nodeOffsetBytesSecondPass {
		maxTreeSize -= int(nodeOffsetBytesFirstPass-nodeOffsetBytesSecondPass) * countChildren
		nodeOffsetBytesFirstPass = nodeOffsetBytesSecondPass
		nodeOffsetBytesSecondPass = getNecessaryBytes(uint64(maxTreeSize))
	}

	offset := current
	for i, node := range t.nodes {
		if node == nil {
			continue
		}
		hasLeft := t.exists(t.left(i))
		hasRight := t.exists(t.right(i))
		size := node.compressedSize(isKeyFixedSize, nodeOffsetBytesFirstPass, hasLeft, hasRight)
		offsets[i] = uint64(offset)
		offset += uint64(size)
	}

	return offsets, uint64(offset), nodeOffsetBytesFirstPass, isKeyFixedSize
}

func (t *Tree) getCompressedHeader(isKeyFixedSize bool) []byte {
	// version(1) + flags(1) + keySize(4)
	buf := make([]byte, 6)

	// Version
	buf[0] = 1

	// Flags
	var flags byte
	if isKeyFixedSize {
		flags |= 0x01
	}
	buf[1] = flags

	// Key size
	if isKeyFixedSize && len(t.nodes) > 0 {
		keySize := uint32(len(t.nodes[0].Key))
		binary.LittleEndian.PutUint32(buf[2:6], keySize)
	}

	return buf
}

func (n *Node) serializeCompressed(fixedKeySize bool, leftOffset, rightOffset int, offsetBytes int) []byte {
	key := n.Key
	nodeStart := n.Start
	nodeSize := n.End - n.Start
	var entry []byte

	// Calculate necessary bytes
	lenKeyBytes := 0
	if !fixedKeySize {
		lenKeyBytes = getNecessaryBytes(uint64(len(key)))
	}
	nodeStartBytes := getNecessaryBytes(nodeStart)
	nodeSizeBytes := getNecessaryBytes(nodeSize)
	leftOffsetBytes := 0
	if leftOffset > 0 {
		leftOffsetBytes = offsetBytes
	}
	rightOffsetBytes := 0
	if rightOffset > 0 {
		rightOffsetBytes = offsetBytes
	}

	// Create header
	headerInt := (lenKeyBytes & 0b11) |
		((nodeStartBytes & 0b111) << 2) |
		((nodeSizeBytes & 0b111) << 5) |
		((leftOffsetBytes & 0b111) << 8) |
		((rightOffsetBytes & 0b111) << 11)

	headerBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(headerBytes, uint16(headerInt))
	entry = append(entry, headerBytes...)

	// Add key length if variable
	if lenKeyBytes > 0 {
		lenKeyData := make([]byte, lenKeyBytes)
		for i := 0; i < lenKeyBytes; i++ {
			lenKeyData[i] = byte(len(key) >> (8 * i))
		}
		entry = append(entry, lenKeyData...)
	}

	// Add key
	entry = append(entry, key...)

	// Add node start
	if nodeStartBytes > 0 {
		nodeStartData := make([]byte, nodeStartBytes)
		for i := 0; i < nodeStartBytes; i++ {
			nodeStartData[i] = byte(nodeStart >> (8 * i))
		}
		entry = append(entry, nodeStartData...)
	}

	// Add node size
	if nodeSizeBytes > 0 {
		nodeSizeData := make([]byte, nodeSizeBytes)
		for i := 0; i < nodeSizeBytes; i++ {
			nodeSizeData[i] = byte(nodeSize >> (8 * i))
		}
		entry = append(entry, nodeSizeData...)
	}

	// Add left offset
	if leftOffsetBytes > 0 {
		leftOffsetData := make([]byte, leftOffsetBytes)
		for i := 0; i < leftOffsetBytes; i++ {
			leftOffsetData[i] = byte(leftOffset >> (8 * i))
		}
		entry = append(entry, leftOffsetData...)
	}

	// Add right offset
	if rightOffsetBytes > 0 {
		rightOffsetData := make([]byte, rightOffsetBytes)
		for i := 0; i < rightOffsetBytes; i++ {
			rightOffsetData[i] = byte(rightOffset >> (8 * i))
		}
		entry = append(entry, rightOffsetData...)
	}

	return entry
}

func (t *Tree) MarshalBinaryCompressed() ([]byte, error) {
	offsets, size, nodeOffsetBytes, isKeyFixedSize := t.calculateCompressedDiskOffsets(compressedTreeHeaderSize)
	header := t.getCompressedHeader(isKeyFixedSize)

	buf := bytes.NewBuffer(nil)

	if _, err := buf.Write(header); err != nil {
		return nil, err
	}

	for i, node := range t.nodes {
		if node == nil {
			continue
		}
		myOffset := offsets[i]

		var leftOffset int64
		var rightOffset int64

		if t.exists(t.left(i)) {
			leftOffset = int64(offsets[t.left(i)] - myOffset)
		} else {
			leftOffset = 0
		}

		if t.exists(t.right(i)) {
			if t.exists(t.left(i)) {
				rightOffset = int64(offsets[t.right(i)] - offsets[t.left(i)])
			} else {
				rightOffset = int64(offsets[t.right(i)] - myOffset)
			}
		} else {
			rightOffset = 0
		}

		if len(node.Key) > math.MaxUint32 {
			return nil, errors.Errorf("max key size is %d", math.MaxUint32)
		}

		entry := node.serializeCompressed(isKeyFixedSize, int(leftOffset), int(rightOffset), nodeOffsetBytes)
		if _, err := buf.Write(entry); err != nil {
			return nil, err
		}
	}
	bytes := buf.Bytes()
	if size != uint64(len(bytes)) {
		return nil, errors.Errorf("corrupt: wrote %d bytes with target %d", len(bytes), size)
	}

	return bytes, nil
}

func (t *Tree) MarshalBinaryCompressedInto(w io.Writer) (int64, error) {
	offsets, size := t.calculateDiskOffsets()

	// create buf just once and reuse for each iteration, each iteration
	// overwrites every single byte of the buffer, so no initializing or
	// resetting after a round is required.
	buf := make([]byte, 36) // 1x uint32 + 4x uint64

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
