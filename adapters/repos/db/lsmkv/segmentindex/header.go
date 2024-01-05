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

package segmentindex

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// HeaderSize describes the general offset in a segment until the data
// starts, it is composed of 2 bytes for level, 2 bytes for version,
// 2 bytes for secondary index count, 2 bytes for strategy, 8 bytes
// for the pointer to the index part
const HeaderSize = 16

type Header struct {
	Level            uint16
	Version          uint16
	SecondaryIndices uint16
	Strategy         Strategy
	IndexStart       uint64
}

func (h *Header) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.LittleEndian, &h.Level); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.Version); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.SecondaryIndices); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Strategy); err != nil {
		return -1, err
	}
	if err := binary.Write(w, binary.LittleEndian, &h.IndexStart); err != nil {
		return -1, err
	}

	return int64(HeaderSize), nil
}

func (h *Header) PrimaryIndex(source []byte) ([]byte, error) {
	if h.SecondaryIndices == 0 {
		return source[h.IndexStart:], nil
	}

	offsets, err := h.parseSecondaryIndexOffsets(
		source[h.IndexStart:h.secondaryIndexOffsetsEnd()])
	if err != nil {
		return nil, err
	}

	// the beginning of the first secondary is also the end of the primary
	end := offsets[0]
	return source[h.secondaryIndexOffsetsEnd():end], nil
}

func (h *Header) secondaryIndexOffsetsEnd() uint64 {
	return h.IndexStart + (uint64(h.SecondaryIndices) * 8)
}

func (h *Header) parseSecondaryIndexOffsets(source []byte) ([]uint64, error) {
	r := bufio.NewReader(bytes.NewReader(source))

	offsets := make([]uint64, h.SecondaryIndices)
	if err := binary.Read(r, binary.LittleEndian, &offsets); err != nil {
		return nil, err
	}

	return offsets, nil
}

func (h *Header) SecondaryIndex(source []byte, indexID uint16) ([]byte, error) {
	if indexID >= h.SecondaryIndices {
		return nil, fmt.Errorf("retrieve index %d with len %d",
			indexID, h.SecondaryIndices)
	}

	offsets, err := h.parseSecondaryIndexOffsets(
		source[h.IndexStart:h.secondaryIndexOffsetsEnd()])
	if err != nil {
		return nil, err
	}

	start := offsets[indexID]
	if indexID == h.SecondaryIndices-1 {
		// this is the last index, return until EOF
		return source[start:], nil
	}

	end := offsets[indexID+1]
	return source[start:end], nil
}

func ParseHeader(r io.Reader) (*Header, error) {
	out := &Header{}

	if err := binary.Read(r, binary.LittleEndian, &out.Level); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.Version); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.SecondaryIndices); err != nil {
		return nil, err
	}

	if out.Version != 0 {
		return nil, fmt.Errorf("unsupported version %d", out.Version)
	}

	if err := binary.Read(r, binary.LittleEndian, &out.Strategy); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.IndexStart); err != nil {
		return nil, err
	}

	return out, nil
}
