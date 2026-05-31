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

// SegmentInvertedVersionV2 is the inverted-segment format version that stores
// per-document property lengths as a flat sorted SoA section (sorted []uint64
// docID column + parallel []uint32 length column) read in place from the mmap'd
// segment, instead of the legacy V0 gob-encoded map[uint64]uint32.
//
// Version 0 is the legacy gob-map format (still written today; the V2 writer is
// a separate change). validateInvertedVersion rejects any Version above this
// sentinel so an old binary loudly forward-rejects a segment written by a newer
// binary rather than silently mis-decoding it (which would yield wrong BM25
// scores the replication hashtree cannot detect). Legacy Version 0 always
// passes.
const SegmentInvertedVersionV2 = uint8(1)

// validateInvertedVersion is the G4 reject-unknown-version guard. It rejects any
// inverted-segment Version above the highest this binary knows how to read
// (SegmentInvertedVersionV2), with an operator-runbook error. The error names
// the offending version and the max supported so an operator mid-rolling-upgrade
// knows the node binary is too old to read the segment and must be upgraded
// before it can serve it. Legacy Version 0 and the supported V2 sentinel pass.
//
// The segment path is not available at this layer; the single call site wraps
// the returned error with the path so the end-to-end message reads
// "unsupported inverted segment version N (max M) ...; segment=PATH".
func validateInvertedVersion(version uint8) error {
	if version > SegmentInvertedVersionV2 {
		return fmt.Errorf("unsupported inverted segment version %d (max supported %d); "+
			"this binary is too old to read this segment, upgrade the node binary "+
			"to the release that introduced version %d (or newer) before serving it; "+
			"do not downgrade a node below the version that wrote this segment",
			version, SegmentInvertedVersionV2, version)
	}
	return nil
}

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

	if err := validateInvertedVersion(header.Version); err != nil {
		return nil, err
	}

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

	if err := validateInvertedVersion(out.Version); err != nil {
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
