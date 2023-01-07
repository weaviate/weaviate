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
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type segmentHeader struct {
	level            uint16
	version          uint16
	secondaryIndices uint16
	strategy         SegmentStrategy
	indexStart       uint64
}

func (h *segmentHeader) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.LittleEndian, &h.level); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.version); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.secondaryIndices); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, h.strategy); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.indexStart); err != nil {
		return -1, err
	}

	return int64(SegmentHeaderSize), nil
}

func (h *segmentHeader) PrimaryIndex(source []byte) ([]byte, error) {
	if h.secondaryIndices == 0 {
		return source[h.indexStart:], nil
	}

	offsets, err := h.parseSecondaryIndexOffsets(
		source[h.indexStart:h.secondaryIndexOffsetsEnd()])
	if err != nil {
		return nil, err
	}

	// the beginning of the first secondary is also the end of the primary
	end := offsets[0]
	return source[h.secondaryIndexOffsetsEnd():end], nil
}

func (h *segmentHeader) secondaryIndexOffsetsEnd() uint64 {
	return h.indexStart + (uint64(h.secondaryIndices) * 8)
}

func (h *segmentHeader) parseSecondaryIndexOffsets(source []byte) ([]uint64, error) {
	r := bytes.NewReader(source)

	offsets := make([]uint64, h.secondaryIndices)
	if err := binary.Read(r, binary.LittleEndian, &offsets); err != nil {
		return nil, err
	}

	return offsets, nil
}

func (h *segmentHeader) SecondaryIndex(source []byte, indexID uint16) ([]byte, error) {
	if indexID >= h.secondaryIndices {
		return nil, errors.Errorf("retrieve index %d with len %d",
			indexID, h.secondaryIndices)
	}

	offsets, err := h.parseSecondaryIndexOffsets(
		source[h.indexStart:h.secondaryIndexOffsetsEnd()])
	if err != nil {
		return nil, err
	}

	start := offsets[indexID]
	if indexID == h.secondaryIndices-1 {
		// this is the last index, return until EOF
		return source[start:], nil
	}

	end := offsets[indexID+1]
	return source[start:end], nil
}

func parseSegmentHeader(r io.Reader) (*segmentHeader, error) {
	out := &segmentHeader{}

	if err := binary.Read(r, binary.LittleEndian, &out.level); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.version); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.secondaryIndices); err != nil {
		return nil, err
	}

	if out.version != 0 {
		return nil, errors.Errorf("unsupported version %d", out.version)
	}

	if err := binary.Read(r, binary.LittleEndian, &out.strategy); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.indexStart); err != nil {
		return nil, err
	}

	return out, nil
}

type segmentIndices struct {
	keys                []segmentindex.Key
	secondaryIndexCount uint16
	scratchSpacePath    string
}

func (s segmentIndices) WriteTo(w io.Writer) (int64, error) {
	currentOffset := uint64(s.keys[len(s.keys)-1].ValueEnd)
	var written int64

	if _, err := os.Stat(s.scratchSpacePath); err == nil {
		// exists, we need to delete
		// This could be the case if Weaviate shut down unexpectedly (i.e. crashed)
		// while a compaction was running. We can safely discard the contents of
		// the scratch space.

		if err := os.RemoveAll(s.scratchSpacePath); err != nil {
			return written, errors.Wrap(err, "clean up previous scratch space")
		}
	} else if os.IsNotExist(err) {
		// does not exist yet, nothing to - will be created in the next step
	} else {
		return written, errors.Wrap(err, "check for scratch space directory")
	}

	if err := os.Mkdir(s.scratchSpacePath, 0o777); err != nil {
		return written, errors.Wrap(err, "create scratch space")
	}

	primaryFileName := filepath.Join(s.scratchSpacePath, "primary")
	primaryFD, err := os.Create(primaryFileName)
	if err != nil {
		return written, err
	}

	primaryFDBuffered := bufio.NewWriter(primaryFD)

	n, err := s.buildAndMarshalPrimary(primaryFDBuffered, s.keys)
	if err != nil {
		return written, err
	}

	if err := primaryFDBuffered.Flush(); err != nil {
		return written, err
	}

	primaryFD.Seek(0, io.SeekStart)

	// pretend that primary index was already written, then also account for the
	// additional offset pointers (one for each secondary index)
	currentOffset = currentOffset + uint64(n) +
		uint64(s.secondaryIndexCount)*8

	// secondaryIndicesBytes := bytes.NewBuffer(nil)
	secondaryFileName := filepath.Join(s.scratchSpacePath, "secondary")
	secondaryFD, err := os.Create(secondaryFileName)
	if err != nil {
		return written, err
	}

	secondaryFDBuffered := bufio.NewWriter(secondaryFD)

	if s.secondaryIndexCount > 0 {
		offsets := make([]uint64, s.secondaryIndexCount)
		for pos := range offsets {
			n, err := s.buildAndMarshalSecondary(secondaryFDBuffered, pos, s.keys)
			if err != nil {
				return written, err
			} else {
				written += int64(n)
			}

			offsets[pos] = currentOffset
			currentOffset = offsets[pos] + uint64(n)
		}

		if err := binary.Write(w, binary.LittleEndian, &offsets); err != nil {
			return written, err
		}

		written += int64(len(offsets)) * 8
	}

	if err := secondaryFDBuffered.Flush(); err != nil {
		return written, err
	}

	secondaryFD.Seek(0, io.SeekStart)

	if n, err := io.Copy(w, primaryFD); err != nil {
		return written, err
	} else {
		written += int64(n)
	}

	if n, err := io.Copy(w, secondaryFD); err != nil {
		return written, err
	} else {
		written += int64(n)
	}

	if err := primaryFD.Close(); err != nil {
		return written, err
	}

	if err := secondaryFD.Close(); err != nil {
		return written, err
	}

	if err := os.RemoveAll(s.scratchSpacePath); err != nil {
		return written, err
	}

	return written, nil
}

// pos indicates the position of a secondary index, assumes unsorted keys and
// sorts them
func (s *segmentIndices) buildAndMarshalSecondary(w io.Writer, pos int,
	keys []segmentindex.Key,
) (int64, error) {
	keyNodes := make([]segmentindex.Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = segmentindex.Node{
			Key:   key.SecondaryKeys[pos],
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
		i++
	}

	keyNodes = keyNodes[:i]

	sort.Slice(keyNodes, func(a, b int) bool {
		return bytes.Compare(keyNodes[a].Key, keyNodes[b].Key) < 0
	})

	index := segmentindex.NewBalanced(keyNodes)
	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// assumes sorted keys and does NOT sort them again
func (s *segmentIndices) buildAndMarshalPrimary(w io.Writer, keys []segmentindex.Key) (int64, error) {
	keyNodes := make([]segmentindex.Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = segmentindex.Node{
			Key:   key.Key,
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
	}
	index := segmentindex.NewBalanced(keyNodes)

	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return -1, err
	}

	return n, nil
}

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
