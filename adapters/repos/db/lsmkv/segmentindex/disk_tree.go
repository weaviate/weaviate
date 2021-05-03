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
	node, err := t.readNode(offset)
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

func (t *DiskTree) readNode(offset int64) (dtNode, error) {
	r := bytes.NewReader(t.data)
	r.Seek(offset, io.SeekStart)

	var out dtNode

	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return out, err
	}

	out.key = make([]byte, keyLen)
	if _, err := r.Read(out.key); err != nil {
		return out, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.startPos); err != nil {
		return out, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.endPos); err != nil {
		return out, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.leftChild); err != nil {
		return out, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.rightChild); err != nil {
		return out, err
	}

	return out, nil
}
