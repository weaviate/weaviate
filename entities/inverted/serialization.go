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

package inverted

import (
	"encoding/binary"
	"fmt"
	"math"
)

// LexicographicallySortableFloat64 transforms a conversion to a
// lexicographically sortable byte slice. In general, for lexicographical
// sorting big endian notatino is required. Additionally  the sign needs to be
// flipped in any case, but additionally each remaining byte also needs to be
// flipped if the number is negative
//
// NaN is unsupported: it does not reliably round-trip (math.NaN() decodes to a
// finite value) and has no meaningful sort position. Callers must not pass NaN;
// its equality and range semantics through this encoding are undefined.
func LexicographicallySortableFloat64(in float64) ([]byte, error) {
	// Normalize negative zero (-0.0) to positive zero (0.0). IEEE 754 defines
	// -0.0 == 0.0, but their bit representations differ. Without this
	// normalization -0.0 would encode to a byte sequence that sorts before all
	// negative numbers, breaking equality and range filters.
	if in == 0 && math.Signbit(in) {
		in = 0
	}

	bits := math.Float64bits(in)
	if in >= 0 {
		// on positive numbers only flip the sign
		bits ^= 1 << 63
	} else {
		// on negative numbers flip every bit
		bits = ^bits
	}

	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, bits)
	return out, nil
}

// ParseLexicographicallySortableFloat64 reverses the changes in
// LexicographicallySortableFloat64
func ParseLexicographicallySortableFloat64(in []byte) (float64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("float64 must be 8 bytes long, got: %d", len(in))
	}

	bits := binary.BigEndian.Uint64(in)
	if in[0]&0x80 == 0x80 {
		// encoded as negative means it was originally positive, so we only need to
		// flip the sign
		bits ^= 1 << 63
	} else {
		// encoded as positive means it was originally negative, so we need to flip
		// everything
		bits = ^bits
	}

	return math.Float64frombits(bits), nil
}

// LexicographicallySortableInt64 performs a conversion to a lexicographically
// sortable byte slice. For this, big endian notation is required and the sign
// must be flipped
func LexicographicallySortableInt64(in int64) ([]byte, error) {
	out := make([]byte, 8)
	// flip the sign
	binary.BigEndian.PutUint64(out, uint64(in)^(1<<63))
	return out, nil
}

// ParseLexicographicallySortableInt64 reverses the changes in
// LexicographicallySortableInt64
func ParseLexicographicallySortableInt64(in []byte) (int64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("int64 must be 8 bytes long, got: %d", len(in))
	}

	// flip the sign back
	return int64(binary.BigEndian.Uint64(in) ^ (1 << 63)), nil
}

// LexicographicallySortableUint64 performs a conversion to a lexicographically
// sortable byte slice. For this, big endian notation is required.
func LexicographicallySortableUint64(in uint64) ([]byte, error) {
	// no signs to flip as this is a uint
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, in)
	return out, nil
}

// ParseLexicographicallySortableUint64 reverses the changes in
// LexicographicallySortableUint64
func ParseLexicographicallySortableUint64(in []byte) (uint64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("uint64 must be 8 bytes long, got: %d", len(in))
	}

	return binary.BigEndian.Uint64(in), nil
}
