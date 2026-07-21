//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	// HeaderSize describes the general offset in a segment until the data
	// starts, it is composed of 2 bytes for level, 2 bytes for version,
	// 2 bytes for secondary index count, 2 bytes for strategy, 8 bytes
	// for the pointer to the index part
	HeaderSize = 16

	// ChecksumSize describes the length of the segment file checksum.
	// This is currently based on the CRC32 hashing algorithm.
	ChecksumSize = 4
)

type Header struct {
	Level            uint16
	Version          uint16
	SecondaryIndices uint16
	Strategy         Strategy
	IndexStart       uint64
}

func (h *Header) WriteTo(w io.Writer) (int64, error) {
	data := make([]byte, HeaderSize)
	rw := byteops.NewReadWriter(data)
	rw.WriteUint16(h.Level)
	rw.WriteUint16(h.Version)
	rw.WriteUint16(h.SecondaryIndices)
	rw.WriteUint16(uint16(h.Strategy))
	rw.WriteUint64(h.IndexStart)

	write, err := w.Write(data)
	if err != nil {
		return 0, err
	}
	if write != HeaderSize {
		return 0, fmt.Errorf("expected to write %d bytes, got %d", HeaderSize, write)
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

func ParseHeader(data []byte) (*Header, error) {
	if len(data) != HeaderSize {
		return nil, fmt.Errorf("expected %d bytes, got %d", HeaderSize, len(data))
	}
	rw := byteops.NewReadWriter(data)
	out := &Header{}
	out.Level = rw.ReadUint16()
	out.Version = rw.ReadUint16()
	out.SecondaryIndices = rw.ReadUint16()
	out.Strategy = Strategy(rw.ReadUint16())
	out.IndexStart = rw.ReadUint64()

	if out.Version > CurrentSegmentVersion {
		return nil, fmt.Errorf("unsupported version %d", out.Version)
	}

	// IndexStart < HeaderSize only happens on a corrupted/truncated/zeroed
	// file; left undetected, readers panic slicing contents[HeaderSize:IndexStart]
	// or silently treat the segment as empty (data loss). Reject it here, the
	// one choke point every segment open goes through.
	if out.IndexStart < uint64(HeaderSize) {
		return nil, fmt.Errorf(
			"corrupt segment header: index start %d is before the header end (%d); "+
				"segment file is truncated, zeroed, or otherwise corrupted",
			out.IndexStart, HeaderSize,
		)
	}

	return out, nil
}

// ValidateIndexBounds checks that IndexStart and (if set) the secondary
// index offset table fall within the segment's actual byte length.
// ParseHeader can't do this itself since it never sees the file, only the
// fixed-size header; skipping it panics on the first slice read.
func (h *Header) ValidateIndexBounds(contentsLen uint64) error {
	if h.IndexStart > contentsLen {
		return fmt.Errorf(
			"corrupt segment header: index start %d is past the segment end (%d); "+
				"segment file is truncated or otherwise corrupted",
			h.IndexStart, contentsLen,
		)
	}
	if h.SecondaryIndices > 0 {
		if end := h.secondaryIndexOffsetsEnd(); end > contentsLen {
			return fmt.Errorf(
				"corrupt segment header: secondary index table end %d is past the segment "+
					"end (%d); segment file is truncated or otherwise corrupted",
				end, contentsLen,
			)
		}
	}
	return nil
}

// ValidateNonEmptyIndex rejects an empty primary index when the header
// claims the segment holds data (IndexStart > HeaderSize), for strategies
// whose object-keyed primary DiskTree always keeps at least one entry once
// IndexStart moves past the header - even an all-tombstone flush indexes
// its tombstone marker under the object's key. An empty index at that
// point is only reachable via corruption for those strategies - most
// commonly an off-by-one where IndexStart == the segment's actual length:
// that passes ValidateIndexBounds (which uses '>', not '>='), but leaves a
// zero-byte primary index that would otherwise open clean and silently
// serve empty reads for data that is still intact in the data region.
//
// StrategyRoaringSetRange and StrategyInverted are excluded: neither
// indexes tombstones (or, for RoaringSetRange, anything at all) via the
// primary DiskTree. flushDataRoaringSetRange always returns zero index
// keys - it has no primary DiskTree by design. flushDataInverted returns
// zero keys whenever every value in the flush is a tombstone (inverted
// tombstones live in a separate bitmap, not as per-key index entries), so
// an empty index with IndexStart > HeaderSize is normal, by-design state
// for both strategies whenever the segment holds any data - not a
// corruption signal.
func (h *Header) ValidateNonEmptyIndex(primaryIndexLen int) error {
	if h.Strategy == StrategyRoaringSetRange || h.Strategy == StrategyInverted {
		return nil
	}
	if h.IndexStart > uint64(HeaderSize) && primaryIndexLen == 0 {
		return fmt.Errorf(
			"corrupt segment header: index start %d is past the header end (%d) but the "+
				"primary index is empty; segment file is truncated or otherwise corrupted",
			h.IndexStart, HeaderSize,
		)
	}
	return nil
}
