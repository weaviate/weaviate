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

package columnar

import "fmt"

// EncodingID identifies the on-disk encoding of a column's data pages.
// Values are part of the on-disk format and must never be reused.
type EncodingID uint8

const (
	// EncodingRawFixedWidth stores values as contiguous fixed-width
	// little-endian payloads, with no compression. Scalars (8B) and single
	// vectors (Dims×4B) share it unchanged; compressed encodings (delta,
	// FOR, dict) slot in beside it without a format break.
	EncodingRawFixedWidth EncodingID = 0
	// EncodingOffsetValues stores variable-width rows as a row-offset table
	// followed by the concatenated payloads:
	//
	//	[offsets: (rows+1) × uint32]   (byte offsets into the payload)
	//	[padding to payloadAlign]
	//	[payload: concatenated row bytes]
	//
	// Used by multi-vector columns, where each row is a token matrix of
	// variable token count. The offset table is relative to the payload
	// start; row i spans payload[offsets[i]:offsets[i+1]].
	EncodingOffsetValues EncodingID = 1
)

// Encoding translates between the in-memory representation of a column page
// (contiguous fixed-width little-endian values) and its on-disk bytes.
//
// All encodings must be lossless: the columnar store's contract is that it
// can serve as a faithful source for the property values it holds.
type Encoding interface {
	ID() EncodingID
	// Encode appends the on-disk representation of page (rows fixed-width
	// values of the given width) to dst and returns the extended slice.
	Encode(dst, page []byte, width, rows int) []byte
	// Decode returns the in-memory representation of the encoded page.
	// For zero-copy encodings the returned slice may alias encoded.
	Decode(encoded []byte, width, rows int) ([]byte, error)
	// EncodedSize returns the exact on-disk size of a page before encoding
	// it. Required so writers can lay out block offsets without buffering.
	EncodedSize(width, rows int) int
}

type rawFixedWidth struct{}

func (rawFixedWidth) ID() EncodingID { return EncodingRawFixedWidth }

func (rawFixedWidth) Encode(dst, page []byte, width, rows int) []byte {
	return append(dst, page[:width*rows]...)
}

func (rawFixedWidth) Decode(encoded []byte, width, rows int) ([]byte, error) {
	if len(encoded) < width*rows {
		return nil, fmt.Errorf("raw page too short: %d < %d", len(encoded), width*rows)
	}
	return encoded[:width*rows], nil
}

func (rawFixedWidth) EncodedSize(width, rows int) int { return width * rows }

var encodings = map[EncodingID]Encoding{
	EncodingRawFixedWidth: rawFixedWidth{},
}

// EncodingByID resolves fixed-width encodings. EncodingOffsetValues is
// variable-width and handled structurally by the block writer/reader, so it
// is valid in schemas but not resolvable here.
func EncodingByID(id EncodingID) (Encoding, error) {
	if id == EncodingOffsetValues {
		return nil, fmt.Errorf("encoding %d is variable-width and has no fixed-width codec", id)
	}
	e, ok := encodings[id]
	if !ok {
		return nil, fmt.Errorf("unknown column encoding %d", id)
	}
	return e, nil
}

// ValidEncoding reports whether id is a known encoding (fixed or variable).
func ValidEncoding(id EncodingID) bool {
	if id == EncodingOffsetValues {
		return true
	}
	_, ok := encodings[id]
	return ok
}

// payloadAlign is the byte alignment of vector payload starts within a
// block. 64 bytes satisfies float32 access and AVX/SVE-width loads, keeping
// the zero-copy door open (the brainstorm's alignment question: cheap to
// design in, expensive to retrofit).
const payloadAlign = 64

func alignUp(n, align int) int {
	return (n + align - 1) &^ (align - 1)
}
