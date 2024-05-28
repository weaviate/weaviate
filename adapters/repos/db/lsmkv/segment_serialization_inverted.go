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

package lsmkv

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// a single node of strategy "inverted"
type segmentInvertedNode struct {
	values     []value
	primaryKey []byte
	offset     int
}

var payloadLen = 16

func (s segmentInvertedNode) KeyIndexAndWriteTo(w io.Writer) (segmentindex.Key, error) {
	out := segmentindex.Key{}
	written := 0
	valueLen := uint64(len(s.values))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, valueLen)
	if _, err := w.Write(buf[0:8]); err != nil {
		return out, errors.Wrapf(err, "write values len for node")
	}
	written += 8

	for i, v := range s.values {
		n, err := w.Write(v.value)
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

// ParseInvertedNode reads from r and parses the Inverted values into a segmentInvertedNode
//
// When only given an offset, r is constructed as a *bufio.Reader to avoid first reading the
// entire segment (could be GBs). Each consecutive read will be buffered to avoid excessive
// syscalls.
//
// When we already have a finite and manageable []byte (i.e. when we have already seeked to an
// lsmkv node and have start+end offset), r should be constructed as a *bytes.Reader, since the
// contents have already been `pread` from the segment contentFile.
func ParseInvertedNode(r io.Reader) (segmentInvertedNode, error) {
	out := segmentInvertedNode{}
	// 8 bytes is the most we can ever read uninterrupted, i.e. without a dynamic
	// read in between.
	tmpBuf := make([]byte, 8)

	if n, err := io.ReadFull(r, tmpBuf[0:8]); err != nil {
		return out, errors.Wrap(err, "read values len")
	} else {
		out.offset += n
	}

	valuesLen := binary.LittleEndian.Uint64(tmpBuf[0:8])
	out.values = make([]value, valuesLen)
	for i := range out.values {
		out.values[i].value = make([]byte, payloadLen)
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

// ParseInvertedNodeInto takes the []byte slice and parses it into the
// specified node. It does not perform any copies and the caller must be aware
// that memory may be shared between the two. As a result, the caller must make
// sure that they do not modify "in" while "node" is still in use. A safer
// alternative is to use ParseInvertedNode.
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
// multiple times. Calling it just once on an uninitialized node does not have
// major advantages over calling ParseInvertedNode.
func ParseInvertedNodeInto(r io.Reader, node *segmentInvertedNode) error {
	// offset is only the local offset relative to "in". In the end we need to
	// update the global offset.
	offset := 0

	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf[0:8])
	if err != nil {
		return fmt.Errorf("read values len: %w", err)
	}

	valuesLen := binary.LittleEndian.Uint64(buf[0:8])
	offset += 8

	resizeValuesOfInvertedNode(node, valuesLen)
	for i := range node.values {
		_, err = io.ReadFull(r, node.values[i].value)
		if err != nil {
			return fmt.Errorf("read node value: %w", err)
		}

		offset += int(payloadLen)
	}

	_, err = io.ReadFull(r, buf[0:4])
	if err != nil {
		return fmt.Errorf("read values len: %w", err)
	}
	keyLen := binary.LittleEndian.Uint32(buf)
	offset += 4

	resizeKeyOfInvertedNode(node, keyLen)
	_, err = io.ReadFull(r, node.primaryKey)
	if err != nil {
		return fmt.Errorf("read primary key: %w", err)
	}
	offset += int(keyLen)

	node.offset = offset
	return nil
}

func resizeValuesOfInvertedNode(node *segmentInvertedNode, size uint64) {
	if cap(node.values) >= int(size) {
		node.values = node.values[:size]
	} else {
		// Allocate with 25% overhead to reduce chance of having to do multiple
		// allocations sequentially.
		node.values = make([]value, size, int(float64(size)*1.25))
	}
	for i := range node.values {
		node.values[i].value = make([]byte, payloadLen)
	}
}

func resizeKeyOfInvertedNode(node *segmentInvertedNode, size uint32) {
	if cap(node.primaryKey) >= int(size) {
		node.primaryKey = node.primaryKey[:size]
	} else {
		// Allocate with 25% overhead to reduce chance of having to do multiple
		// allocations sequentially.
		node.primaryKey = make([]byte, size, int(float64(size)*1.25))
	}
}
