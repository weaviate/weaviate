package lsmkv

import (
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

func (h *segmentHeader) WriteTo(w io.Writer) (int, error) {
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

	return SegmentHeaderSize, nil
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
