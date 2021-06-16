package lsmkv

import (
	"bytes"
	"encoding/binary"
	"io"
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
}

func (s *segmentIndices) WriteTo(w io.Writer) (int64, error) {
	currentOffset := uint64(s.keys[len(s.keys)-1].valueEnd)
	var written int64
	indexBytes, err := s.buildAndMarshalPrimary(s.keys)
	if err != nil {
		return written, err
	}

	// pretend that primary index was already written, then also account for the
	// additional offset pointers (one for each secondary index)
	currentOffset = currentOffset + uint64(len(indexBytes)) +
		uint64(s.secondaryIndexCount)*8

	secondaryIndicesBytes := bytes.NewBuffer(nil)

	if s.secondaryIndexCount > 0 {
		offsets := make([]uint64, s.secondaryIndexCount)
		for pos := range offsets {
			secondaryBytes, err := s.buildAndMarshalSecondary(pos, s.keys)
			if err != nil {
				return written, err
			}

			if n, err := secondaryIndicesBytes.Write(secondaryBytes); err != nil {
				return written, err
			} else {
				written += int64(n)
			}

			offsets[pos] = currentOffset
			currentOffset = offsets[pos] + uint64(len(secondaryBytes))
		}

		if err := binary.Write(w, binary.LittleEndian, &offsets); err != nil {
			return written, err
		}

		written += int64(len(offsets)) * 8
	}

	// write primary index
	if n, err := w.Write(indexBytes); err != nil {
		return written, err
	} else {
		written += int64(n)
	}

	// write secondary indices
	if n, err := secondaryIndicesBytes.WriteTo(w); err != nil {
		return written, err
	} else {
		written += n
	}

	return written, nil
}

// pos indicates the position of a secondary index, assumes unsorted keys and
// sorts them
func (s *segmentIndices) buildAndMarshalSecondary(pos int,
	keys []keyIndex) ([]byte, error) {
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
	indexBytes, err := index.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return indexBytes, nil
}

// assumes sorted keys and does NOT sort them again
func (s *segmentIndices) buildAndMarshalPrimary(keys []keyIndex) ([]byte, error) {
	keyNodes := make([]segmentindex.Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = segmentindex.Node{
			Key:   key.key,
			Start: uint64(key.valueStart),
			End:   uint64(key.valueEnd),
		}
	}
	index := segmentindex.NewBalanced(keyNodes)

	indexBytes, err := index.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return indexBytes, nil
}
