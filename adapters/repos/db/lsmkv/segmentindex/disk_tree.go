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

package segmentindex

import (
	"bytes"
	"errors"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/contentReader"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// DiskTree is a read-only wrapper around a marshalled index search tree, which
// can be used for reading, but cannot change the underlying structure. It is
// thus perfectly suited as an index for an (immutable) LSM disk segment, but
// pretty much useless for anything else
type DiskTree struct {
	contentReader contentReader.ContentReader
}

type dtNode struct {
	key        []byte
	startPos   uint64
	endPos     uint64
	leftChild  int64
	rightChild int64
}

func NewDiskTree(contentReader contentReader.ContentReader) *DiskTree {
	return &DiskTree{
		contentReader: contentReader,
	}
}

func (t *DiskTree) Get(key []byte) (Node, error) {
	if t.contentReader.Length() == 0 {
		return Node{}, lsmkv.NotFound
	}
	var out Node
	var buffer []byte
	var keyLen uint32
	offset := uint64(0)

	// jump to the buffer until the node with _key_ is found or return a NotFound error.
	// This function avoids allocations by reusing the same buffer for all keys and avoids memory reads by only
	// extracting the necessary pieces of information while skipping the rest
	for {
		// detect if there is no node with the wanted key.
		if offset+4 > t.contentReader.Length() || offset+4 < 4 {
			return out, lsmkv.NotFound
		}

		keyLen, offset = t.contentReader.ReadUint32(offset)

		buffer, offset = t.contentReader.CopyRange(offset, uint64(keyLen))

		keyEqual := bytes.Compare(key, buffer)
		if keyEqual == 0 {
			out.Key = buffer
			out.Start, offset = t.contentReader.ReadUint64(offset)
			out.End, offset = t.contentReader.ReadUint64(offset)
			return out, nil
		} else if keyEqual < 0 {
			offset += 2 * 8                                // jump over start+end position
			offset, _ = t.contentReader.ReadUint64(offset) // left child
		} else {
			offset += 3 * 8                                // jump over start+end position and left child
			offset, _ = t.contentReader.ReadUint64(offset) // right child
		}
	}
}

func (t *DiskTree) readNodeAt(offset int64) (dtNode, error) {
	retNode, _, err := t.readNode(uint64(offset))
	return retNode, err
}

func (t *DiskTree) readNode(offsetRead uint64) (dtNode, int, error) {
	var out dtNode

	contReader, err := t.contentReader.NewWithOffsetStart(offsetRead)
	if err != nil {
		return out, 0, err
	}

	// in buffer needs at least 36 bytes of data:
	// 4bytes for key length, 32bytes for position and children
	if contReader.Length() < 36 {
		return out, 0, io.EOF
	}

	keyLen, offset := contReader.ReadUint32(0)
	copiedBytes, offset := contReader.CopyRange(offset, uint64(keyLen))

	out.key = copiedBytes

	out.startPos, offset = contReader.ReadUint64(offset)
	out.endPos, offset = contReader.ReadUint64(offset)
	leftChild, offset := contReader.ReadUint64(offset)
	rightChild, offset := contReader.ReadUint64(offset)
	out.leftChild = int64(leftChild)
	out.rightChild = int64(rightChild)
	return out, int(offset), nil
}

func (t *DiskTree) Seek(key []byte) (Node, error) {
	if t.contentReader.Length() == 0 {
		return Node{}, lsmkv.NotFound
	}

	return t.seekAt(0, key)
}

func (t *DiskTree) seekAt(offset int64, key []byte) (Node, error) {
	node, err := t.readNodeAt(offset)
	if err != nil {
		return Node{}, err
	}

	self := Node{
		Key:   node.key,
		Start: node.startPos,
		End:   node.endPos,
	}

	if bytes.Equal(key, node.key) {
		return self, nil
	}

	if bytes.Compare(key, node.key) < 0 {
		if node.leftChild < 0 {
			return self, nil
		}

		left, err := t.seekAt(node.leftChild, key)
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

		return t.seekAt(node.rightChild, key)
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
		node, readLength, err := t.readNode(uint64(bufferPos))
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
	return int(t.contentReader.Length())
}
