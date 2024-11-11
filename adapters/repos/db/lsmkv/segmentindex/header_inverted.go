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
	"encoding/binary"
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
