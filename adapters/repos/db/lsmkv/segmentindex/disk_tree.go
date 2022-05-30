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
	"encoding/binary"
	"io"

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

	return t.getAt(0, key)
}

func (t *DiskTree) getAt(offset int64, key []byte) (Node, error) {
	node, err := t.readNodeAt(offset)
	if err != nil {
		return Node{}, err
	}

	if bytes.Equal(node.key, key) {
		return Node{
			Key:   node.key,
			Start: node.startPos,
			End:   node.endPos,
		}, nil
	}

	if bytes.Compare(key, node.key) < 0 {
		if node.leftChild < 0 {
			return Node{}, NotFound
		}

		return t.getAt(node.leftChild, key)
	} else {
		if node.rightChild < 0 {
			return Node{}, NotFound
		}

		return t.getAt(node.rightChild, key)
	}
}

func (t *DiskTree) readNodeAt(offset int64) (dtNode, error) {
	r := bytes.NewReader(t.data)
	r.Seek(offset, io.SeekStart)

	return t.readNode(r)
}

func (t *DiskTree) readNode(r io.Reader) (dtNode, error) {
	var out dtNode
	tmpBuf := make([]byte, 32) // 32 bytes (4x uint64 is the most we'll ever read at the same time)

	if _, err := r.Read(tmpBuf[0:4]); err != nil {
		return out, err
	}

	keyLen := binary.LittleEndian.Uint32(tmpBuf[0:4])
	out.key = make([]byte, keyLen)
	if _, err := r.Read(out.key); err != nil {
		return out, err
	}

	// read the next four 8 byte numbers at once
	if _, err := r.Read(tmpBuf[0:32]); err != nil {
		return out, err
	}

	out.startPos = binary.LittleEndian.Uint64(tmpBuf[0:8])
	out.endPos = binary.LittleEndian.Uint64(tmpBuf[8:16])
	out.leftChild = int64(binary.LittleEndian.Uint64(tmpBuf[16:24]))
	out.rightChild = int64(binary.LittleEndian.Uint64(tmpBuf[24:32]))
	return out, nil
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
	r := bytes.NewReader(t.data)
	var out [][]byte
	for {
		node, err := t.readNode(r)
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
