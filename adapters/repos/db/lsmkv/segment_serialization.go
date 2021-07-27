//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
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
	keys                []keyIndex
	secondaryIndexCount uint16
	scratchSpacePath    string
}

func (s segmentIndices) WriteTo(w io.Writer) (int64, error) {
	currentOffset := uint64(s.keys[len(s.keys)-1].valueEnd)
	var written int64

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
	keys []keyIndex) (int64, error) {
	keyNodes := make([]segmentindex.Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.secondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = segmentindex.Node{
			Key:   key.secondaryKeys[pos],
			Start: uint64(key.valueStart),
			End:   uint64(key.valueEnd),
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
func (s *segmentIndices) buildAndMarshalPrimary(w io.Writer, keys []keyIndex) (int64, error) {
	keyNodes := make([]segmentindex.Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = segmentindex.Node{
			Key:   key.key,
			Start: uint64(key.valueStart),
			End:   uint64(key.valueEnd),
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

func (s *segmentReplaceNode) KeyIndexAndWriteTo(w io.Writer) (keyIndex, error) {
	out := keyIndex{}
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

	return keyIndex{
		valueStart:    s.offset,
		valueEnd:      s.offset + written,
		key:           s.primaryKey,
		secondaryKeys: s.secondaryKeys,
	}, nil
}

func ParseReplaceNode(r io.Reader, secondaryIndexCount uint16) (segmentReplaceNode, error) {
	out := segmentReplaceNode{}

	if err := binary.Read(r, binary.LittleEndian, &out.tombstone); err != nil {
		return out, errors.Wrap(err, "read tombstone")
	}
	out.offset += 1

	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return out, errors.Wrap(err, "read value length encoding")
	}
	out.offset += 8

	out.value = make([]byte, valueLength)
	if n, err := r.Read(out.value); err != nil {
		return out, errors.Wrap(err, "read value")
	} else {
		out.offset += n
	}

	var keyLength uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLength); err != nil {
		return out, errors.Wrap(err, "read key length encoding")
	}
	out.offset += 4

	out.primaryKey = make([]byte, keyLength)
	if n, err := r.Read(out.primaryKey); err != nil {
		return out, errors.Wrap(err, "read key")
	} else {
		out.offset += n
	}

	if secondaryIndexCount > 0 {
		out.secondaryKeys = make([][]byte, secondaryIndexCount)
	}

	for j := 0; j < int(secondaryIndexCount); j++ {
		var secKeyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &secKeyLen); err != nil {
			return out, errors.Wrap(err, "read secondary key length encoding")
		}
		out.offset += 4

		if secKeyLen == 0 {
			continue
		}

		out.secondaryKeys[j] = make([]byte, secKeyLen)
		if n, err := r.Read(out.secondaryKeys[j]); err != nil {
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

func (s segmentCollectionNode) KeyIndexAndWriteTo(w io.Writer) (keyIndex, error) {
	out := keyIndex{}
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

	out = keyIndex{
		valueStart: s.offset,
		valueEnd:   s.offset + written,
		key:        s.primaryKey,
	}

	return out, nil
}

func ParseCollectionNode(r io.Reader) (segmentCollectionNode, error) {
	out := segmentCollectionNode{}
	var valuesLen uint64
	if err := binary.Read(r, binary.LittleEndian, &valuesLen); err != nil {
		return out, errors.Wrap(err, "read values len")
	}
	out.offset += 8

	out.values = make([]value, valuesLen)
	for i := range out.values {
		if err := binary.Read(r, binary.LittleEndian, &out.values[i].tombstone); err != nil {
			return out, errors.Wrap(err, "read value tombstone")
		}
		out.offset += 1

		var valueLen uint64
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return out, errors.Wrap(err, "read value len")
		}
		out.offset += 8

		out.values[i].value = make([]byte, valueLen)
		n, err := r.Read(out.values[i].value)
		if err != nil {
			return out, errors.Wrap(err, "read value")
		}
		out.offset += n
	}

	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return out, errors.Wrap(err, "read key len")
	}
	out.offset += 4

	out.primaryKey = make([]byte, keyLen)
	n, err := r.Read(out.primaryKey)
	if err != nil {
		return out, errors.Wrap(err, "read key")
	}
	out.offset += n

	return out, nil
}
