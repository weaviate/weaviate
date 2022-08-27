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

package segmentindex

import (
	"bytes"
	"io"

	"github.com/semi-technologies/weaviate/usecases/byte_operations"

	"github.com/pkg/errors"
)

var NotFound = errors.Errorf("not found")

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

func (t *DiskTree) Get(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, NotFound
	}
	var out Node
	byteOps := byte_operations.ByteOperations{Buffer: t.data}

	// jump to the buffer until the node with _key_ is found or return a NotFound error.
	// This function avoids allocations by reusing the same buffer for all keys and avoids memory reads by only
	// extracting the necessary pieces of information while skipping the rest
	NodeKeyBuffer := make([]byte, len(key))
	for {
		// detect if there is no node with the wanted key.
		if byteOps.Position+4 > uint64(len(t.data)) || byteOps.Position+4 < 4 {
			return out, NotFound
		}

		keyLen := byteOps.ReadUint32()
		if int(keyLen) > len(NodeKeyBuffer) {
			NodeKeyBuffer = make([]byte, int(keyLen))
		} else if int(keyLen) < len(NodeKeyBuffer) {
			NodeKeyBuffer = NodeKeyBuffer[:keyLen]
		}
		_, err := byteOps.CopyBytesFromBuffer(uint64(keyLen), NodeKeyBuffer)
		if err != nil {
			return out, errors.Wrap(err, "Could not copy node key")
		}

		keyEqual := bytes.Compare(key, NodeKeyBuffer)
		if keyEqual == 0 {
			out.Key = NodeKeyBuffer
			out.Start = byteOps.ReadUint64()
			out.End = byteOps.ReadUint64()
			return out, nil
		} else if keyEqual < 0 {
			byteOps.MoveBufferPositionForward(2 * 8) // jump over start+end position
			byteOps.Position = byteOps.ReadUint64()  // left child
		} else {
			byteOps.MoveBufferPositionForward(3 * 8) // jump over start+end position and left child
			byteOps.Position = byteOps.ReadUint64()  // right child
		}
	}
}

func (t *DiskTree) readNodeAt(offset int64) (dtNode, error) {
	retNode, _, err := t.readNode(t.data[offset:])
	return retNode, err
}

func (t *DiskTree) readNode(in []byte) (dtNode, int, error) {
	var out dtNode
	// in buffer needs at least 36 bytes of data:
	// 4bytes for key length, 32bytes for position and children
	if len(in) < 36 {
		return out, 0, io.EOF
	}

	byteOps := byte_operations.ByteOperations{Buffer: in}

	keyLen := uint64(byteOps.ReadUint32())
	copiedBytes, err := byteOps.CopyBytesFromBuffer(keyLen, nil)
	if err != nil {
		return out, int(byteOps.Position), errors.Wrap(err, "Could not copy node key")
	}
	out.key = copiedBytes

	out.startPos = byteOps.ReadUint64()
	out.endPos = byteOps.ReadUint64()
	out.leftChild = int64(byteOps.ReadUint64())
	out.rightChild = int64(byteOps.ReadUint64())
	return out, int(byteOps.Position), nil
}

func (t *DiskTree) Seek(key []byte) (Node, error) {
	if len(t.data) == 0 {
		return Node{}, NotFound
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

		if err == NotFound {
			return self, nil
		}

		return Node{}, err
	} else {
		if node.rightChild < 0 {
			return Node{}, NotFound
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
		node, readLength, err := t.readNode(t.data[bufferPos:])
		bufferPos += readLength
		if err == io.EOF {
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
