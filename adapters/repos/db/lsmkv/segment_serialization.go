//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// a single node of strategy "replace"
type segmentReplaceNode struct {
	tombstone           bool
	value               []byte
	primaryKey          []byte
	secondaryIndexCount uint16
	secondaryKeys       [][]byte
	offset              int
}

func (s *segmentReplaceNode) KeyIndexAndWriteTo(w io.Writer) (segmentindex.Key, error) {
	out := segmentindex.Key{}
	written := 0

	buf := make([]byte, 9)
	if s.tombstone {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	valueLength := uint64(len(s.value))
	binary.LittleEndian.PutUint64(buf[1:9], valueLength)
	if _, err := w.Write(buf); err != nil {
		return out, err
	}

	written += 9

	n, err := w.Write(s.value)
	if err != nil {
		return out, errors.Wrapf(err, "write node value")
	}
	written += n

	keyLength := uint32(len(s.primaryKey))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := w.Write(buf[0:4]); err != nil {
		return out, err
	}
	written += 4

	n, err = w.Write(s.primaryKey)
	if err != nil {
		return out, errors.Wrapf(err, "write node key")
	}
	written += n

	for j := 0; j < int(s.secondaryIndexCount); j++ {
		var secondaryKeyLength uint32
		if j < len(s.secondaryKeys) {
			secondaryKeyLength = uint32(len(s.secondaryKeys[j]))
		}

		// write the key length in any case
		binary.LittleEndian.PutUint32(buf[0:4], secondaryKeyLength)
		if _, err := w.Write(buf[0:4]); err != nil {
			return out, err
		}
		written += 4

		if secondaryKeyLength == 0 {
			// we're done here
			continue
		}

		// only write the key if it exists
		n, err = w.Write(s.secondaryKeys[j])
		if err != nil {
			return out, errors.Wrapf(err, "write secondary key %d", j)
		}
		written += n
	}

	return segmentindex.Key{
		ValueStart:    s.offset,
		ValueEnd:      s.offset + written,
		Key:           s.primaryKey,
		SecondaryKeys: s.secondaryKeys,
	}, nil
}

func ParseReplaceNode(r io.Reader, secondaryIndexCount uint16) (segmentReplaceNode, error) {
	out := segmentReplaceNode{}

	// 9 bytes is the most we can ever read uninterrupted, i.e. without a dynamic
	// read in between.
	tmpBuf := make([]byte, 9)
	if n, err := io.ReadFull(r, tmpBuf); err != nil {
		return out, errors.Wrap(err, "read tombstone and value length")
	} else {
		out.offset += n
	}

	out.tombstone = tmpBuf[0] == 0x1
	valueLength := binary.LittleEndian.Uint64(tmpBuf[1:9])
	out.value = make([]byte, valueLength)
	if n, err := io.ReadFull(r, out.value); err != nil {
		return out, errors.Wrap(err, "read value")
	} else {
		out.offset += n
	}

	if n, err := io.ReadFull(r, tmpBuf[0:4]); err != nil {
		return out, errors.Wrap(err, "read key length encoding")
	} else {
		out.offset += n
	}

	keyLength := binary.LittleEndian.Uint32(tmpBuf[0:4])
	out.primaryKey = make([]byte, keyLength)
	if n, err := io.ReadFull(r, out.primaryKey); err != nil {
		return out, errors.Wrap(err, "read key")
	} else {
		out.offset += n
	}

	if secondaryIndexCount > 0 {
		out.secondaryKeys = make([][]byte, secondaryIndexCount)
	}

	for j := 0; j < int(secondaryIndexCount); j++ {
		if n, err := io.ReadFull(r, tmpBuf[0:4]); err != nil {
			return out, errors.Wrap(err, "read secondary key length encoding")
		} else {
			out.offset += n
		}
		secKeyLen := binary.LittleEndian.Uint32(tmpBuf[0:4])
		if secKeyLen == 0 {
			continue
		}

		out.secondaryKeys[j] = make([]byte, secKeyLen)
		if n, err := io.ReadFull(r, out.secondaryKeys[j]); err != nil {
			return out, errors.Wrap(err, "read secondary key")
		} else {
			out.offset += n
		}
	}

	return out, nil
}

func ParseReplaceNodeInto(r io.Reader, secondaryIndexCount uint16, out *segmentReplaceNode) error {
	out.offset = 0

	if err := binary.Read(r, binary.LittleEndian, &out.tombstone); err != nil {
		return errors.Wrap(err, "read tombstone")
	}
	out.offset += 1

	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return errors.Wrap(err, "read value length encoding")
	}
	out.offset += 8

	if int(valueLength) > cap(out.value) {
		out.value = make([]byte, valueLength)
	} else {
		out.value = out.value[:valueLength]
	}

	if n, err := r.Read(out.value); err != nil {
		return errors.Wrap(err, "read value")
	} else {
		out.offset += n
	}

	var keyLength uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLength); err != nil {
		return errors.Wrap(err, "read key length encoding")
	}
	out.offset += 4

	out.primaryKey = make([]byte, keyLength)
	if n, err := r.Read(out.primaryKey); err != nil {
		return errors.Wrap(err, "read key")
	} else {
		out.offset += n
	}

	if secondaryIndexCount > 0 {
		out.secondaryKeys = make([][]byte, secondaryIndexCount)
	}

	for j := 0; j < int(secondaryIndexCount); j++ {
		var secKeyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &secKeyLen); err != nil {
			return errors.Wrap(err, "read secondary key length encoding")
		}
		out.offset += 4

		if secKeyLen == 0 {
			continue
		}

		out.secondaryKeys[j] = make([]byte, secKeyLen)
		if n, err := r.Read(out.secondaryKeys[j]); err != nil {
			return errors.Wrap(err, "read secondary key")
		} else {
			out.offset += n
		}
	}

	return nil
}

// collection strategy does not support secondary keys at this time
type segmentCollectionNode struct {
	values     []value
	primaryKey []byte
	offset     int
}

func (s segmentCollectionNode) KeyIndexAndWriteTo(w io.Writer) (segmentindex.Key, error) {
	out := segmentindex.Key{}
	written := 0
	valueLen := uint64(len(s.values))
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint64(buf, valueLen)
	if _, err := w.Write(buf[0:8]); err != nil {
		return out, errors.Wrapf(err, "write values len for node")
	}
	written += 8

	for i, value := range s.values {
		if value.tombstone {
			buf[0] = 0x01
		} else {
			buf[0] = 0x00
		}

		valueLen := uint64(len(value.value))
		binary.LittleEndian.PutUint64(buf[1:9], valueLen)
		if _, err := w.Write(buf[0:9]); err != nil {
			return out, errors.Wrapf(err, "write len of value %d", i)
		}
		written += 9

		n, err := w.Write(value.value)
		if err != nil {
			return out, errors.Wrapf(err, "write value %d", i)
		}
		written += n
	}

	keyLength := uint32(len(s.primaryKey))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := w.Write(buf[0:4]); err != nil {
		return out, errors.Wrapf(err, "write key length encoding for node")
	}
	written += 4

	n, err := w.Write(s.primaryKey)
	if err != nil {
		return out, errors.Wrapf(err, "write node")
	}
	written += n

	out = segmentindex.Key{
		ValueStart: s.offset,
		ValueEnd:   s.offset + written,
		Key:        s.primaryKey,
	}

	return out, nil
}

func ParseCollectionNode(r io.Reader) (segmentCollectionNode, error) {
	out := segmentCollectionNode{}
	// 9 bytes is the most we can ever read uninterrupted, i.e. without a dynamic
	// read in between.
	tmpBuf := make([]byte, 9)

	if n, err := io.ReadFull(r, tmpBuf[0:8]); err != nil {
		return out, errors.Wrap(err, "read values len")
	} else {
		out.offset += n
	}

	valuesLen := binary.LittleEndian.Uint64(tmpBuf[0:8])
	out.values = make([]value, valuesLen)
	for i := range out.values {
		if n, err := io.ReadFull(r, tmpBuf[0:9]); err != nil {
			return out, errors.Wrap(err, "read value tombston and len")
		} else {
			out.offset += n
		}
		out.values[i].tombstone = tmpBuf[0] == 0x1
		valueLen := binary.LittleEndian.Uint64(tmpBuf[1:9])
		out.values[i].value = make([]byte, valueLen)
		n, err := io.ReadFull(r, out.values[i].value)
		if err != nil {
			return out, errors.Wrap(err, "read value")
		}
		out.offset += n
	}

	if n, err := io.ReadFull(r, tmpBuf[0:4]); err != nil {
		return out, errors.Wrap(err, "read key len")
	} else {
		out.offset += n
	}
	keyLen := binary.LittleEndian.Uint32(tmpBuf[0:4])
	out.primaryKey = make([]byte, keyLen)
	n, err := io.ReadFull(r, out.primaryKey)
	if err != nil {
		return out, errors.Wrap(err, "read key")
	}
	out.offset += n

	return out, nil
}

// ParseCollectionNodeInto takes the []byte slice and parses it into the
// specified node. It does not perform any copies and the caller must be aware
// that memory may be shared between the two. As a result, the caller must make
// sure that they do not modify "in" while "node" is still in use. A safer
// alternative is to use ParseCollectionNode.
//
// The primary intention of this function is to provide a way to reuse buffers
// when the lifetime is controlled tightly, for example in cursors used within
// compactions. Use at your own risk!
//
// If the buffers of the provided node have enough capacity they will be
// reused. Only if the capacity is not enough, will an allocation occur. This
// allocation uses 25% overhead to avoid future allocations for nodes of
// similar size.
//
// As a result calling this method only makes sense if you plan on calling it
// multiple times. Calling it just once on an unitialized node does not have
// major advantages over calling ParseCollectionNode.
func ParseCollectionNodeInto(in []byte, node *segmentCollectionNode) error {
	// offset is only the local offset relative to "in". In the end we need to
	// update the global offset.
	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	offset += 8

	resizeValuesOfCollectionNode(node, valuesLen)
	for i := range node.values {
		node.values[i].tombstone = in[offset] == 0x1
		offset += 1

		valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
		offset += 8

		resizeValueOfCollectionNodeAtPos(node, i, valueLen)
		node.values[i].value = in[offset : offset+int(valueLen)]
		offset += int(valueLen)
	}

	keyLen := binary.LittleEndian.Uint32(in[offset : offset+4])
	offset += 4

	resizeKeyOfCollectionNode(node, keyLen)
	node.primaryKey = in[offset : offset+int(keyLen)]
	offset += int(keyLen)

	node.offset = offset
	return nil
}

func resizeValuesOfCollectionNode(node *segmentCollectionNode, size uint64) {
	if cap(node.values) >= int(size) {
		node.values = node.values[:size]
	} else {
		// Allocate with 25% overhead to reduce chance of having to do multiple
		// allocations sequentially.
		node.values = make([]value, size, int(float64(size)*1.25))
	}
}

func resizeValueOfCollectionNodeAtPos(node *segmentCollectionNode, pos int,
	size uint64,
) {
	if cap(node.values[pos].value) >= int(size) {
		node.values[pos].value = node.values[pos].value[:size]
	} else {
		// Allocate with 25% overhead to reduce chance of having to do multiple
		// allocations sequentially.
		node.values[pos].value = make([]byte, size, int(float64(size)*1.25))
	}
}

func resizeKeyOfCollectionNode(node *segmentCollectionNode, size uint32) {
	if cap(node.primaryKey) >= int(size) {
		node.primaryKey = node.primaryKey[:size]
	} else {
		// Allocate with 25% overhead to reduce chance of having to do multiple
		// allocations sequentially.
		node.primaryKey = make([]byte, size, int(float64(size)*1.25))
	}
}
