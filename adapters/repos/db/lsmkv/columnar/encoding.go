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
	// little-endian payloads, with no compression. It is deliberately a
	// first-class encoding (not an implicit default): future column kinds
	// with larger fixed widths — single- and multi-vector payloads — reuse
	// this encoding unchanged, and compressed encodings (delta, FOR, dict)
	// slot in beside it without a format break.
	EncodingRawFixedWidth EncodingID = 0
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

func EncodingByID(id EncodingID) (Encoding, error) {
	e, ok := encodings[id]
	if !ok {
		return nil, fmt.Errorf("unknown column encoding %d", id)
	}
	return e, nil
}
