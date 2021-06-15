package lsmkv

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
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
