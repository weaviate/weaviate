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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

var (
	SegmentInvertedDefaultHeaderSize = 27
	SegmentInvertedDefaultBlockSize  = terms.BLOCK_SIZE
	SegmentInvertedDefaultFieldCount = 2
)

const HeaderInvertedSize = 29 // 27 + 2 bytes for data field count

type HeaderInverted struct {
	KeysOffset            uint64
	TombstoneOffset       uint64
	PropertyLengthsOffset uint64
	Version               uint8
	BlockSize             uint8
	DataFieldCount        uint8
	DataFields            []varenc.VarEncDataType
}

func LoadHeaderInverted(headerBytes []byte) (*HeaderInverted, error) {
	header := &HeaderInverted{}

	header.KeysOffset = binary.LittleEndian.Uint64(headerBytes[0:8])
	header.TombstoneOffset = binary.LittleEndian.Uint64(headerBytes[8:16])
	header.PropertyLengthsOffset = binary.LittleEndian.Uint64(headerBytes[16:24])
	header.Version = headerBytes[24]
	header.BlockSize = headerBytes[25]
	header.DataFieldCount = headerBytes[26]

	header.DataFields = make([]varenc.VarEncDataType, header.DataFieldCount)
	for i, b := range headerBytes[27:] {
		header.DataFields[i] = varenc.VarEncDataType(b)
	}

	return header, nil
}

func (h *HeaderInverted) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.LittleEndian, h.KeysOffset); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, h.TombstoneOffset); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, h.PropertyLengthsOffset); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, h.Version); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, h.BlockSize); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, h.DataFieldCount); err != nil {
		return 0, err
	}

	for _, df := range h.DataFields {
		if err := binary.Write(w, binary.LittleEndian, df); err != nil {
			return 0, err
		}
	}

	return int64(SegmentInvertedDefaultHeaderSize + len(h.DataFields)), nil
}

func ParseHeaderInverted(r io.Reader) (*HeaderInverted, error) {
	out := &HeaderInverted{}

	if err := binary.Read(r, binary.LittleEndian, &out.KeysOffset); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.TombstoneOffset); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.PropertyLengthsOffset); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.Version); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.BlockSize); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &out.DataFieldCount); err != nil {
		return nil, err
	}

	out.DataFields = make([]varenc.VarEncDataType, out.DataFieldCount)

	for i := 0; i < int(out.DataFieldCount); i++ {
		var b byte
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return nil, err
		}

		out.DataFields[i] = varenc.VarEncDataType(b)
	}

	return out, nil
}

// ValidateOffsetsInBounds checks that KeysOffset, TombstoneOffset, and
// PropertyLengthsOffset all fall within the segment's actual byte length.
// LoadHeaderInverted only ever sees the fixed-size inverted header, not the
// file, so a corrupt offset left unchecked panics the first time a
// lazily-loaded reader slices contents[offset:offset+n] with no recover
// barrier (background compaction, flush, bloom-rebuild).
func (h *HeaderInverted) ValidateOffsetsInBounds(contentsLen uint64) error {
	offsets := []struct {
		name   string
		offset uint64
	}{
		{"keys offset", h.KeysOffset},
		{"tombstone offset", h.TombstoneOffset},
		{"property lengths offset", h.PropertyLengthsOffset},
	}
	for _, o := range offsets {
		if o.offset > contentsLen {
			return fmt.Errorf(
				"corrupt inverted segment header: %s %d is past the segment end (%d); "+
					"segment file is truncated or otherwise corrupted",
				o.name, o.offset, contentsLen,
			)
		}
	}
	return nil
}

// ValidateNonEmptyIndex rejects an empty primary index for a
// StrategyInverted segment that claims to hold data (indexStart >
// HeaderSize), unless propLengthCount (a uint64 at PropertyLengthsOffset+8;
// see flushDataInverted) proves every flushed value was a tombstone.
// propLengthCount increments once per docID with a non-tombstone value, so
// it is zero exactly when the empty index is legitimate rather than
// corrupt.
func (h *HeaderInverted) ValidateNonEmptyIndex(contents []byte, primaryIndexLen int, indexStart uint64) error {
	if primaryIndexLen != 0 || indexStart <= uint64(HeaderSize) {
		return nil
	}
	countStart := h.PropertyLengthsOffset + 8
	countEnd := countStart + 8
	if countEnd > uint64(len(contents)) {
		return fmt.Errorf(
			"corrupt inverted segment header: property lengths count field [%d,%d) is past "+
				"the segment end (%d); segment file is truncated or otherwise corrupted",
			countStart, countEnd, len(contents),
		)
	}
	propLengthCount := binary.LittleEndian.Uint64(contents[countStart:countEnd])
	if propLengthCount > 0 {
		return fmt.Errorf(
			"corrupt segment header: index start %d is past the header end (%d) but the "+
				"primary index is empty even though %d real (non-tombstone) entries were "+
				"flushed; segment file is truncated or otherwise corrupted",
			indexStart, HeaderSize, propLengthCount,
		)
	}
	return nil
}
