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

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pkg/errors"
)

// LexicographicallySortableFloat64 transforms a conversion to a
// lexicographically sortable byte slice. In general, for lexicographical
// sorting big endian notatino is required. Additionally  the sign needs to be
// flipped in any case, but additionally each remaining byte also needs to be
// flipped if the number is negative
func LexicographicallySortableFloat64(in float64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.BigEndian, in)
	if err != nil {
		return nil, errors.Wrap(err, "serialize float64 value as big endian")
	}

	var out []byte
	if in >= 0 {
		// on positive numbers only flip the sign
		out = buf.Bytes()
		firstByte := out[0] ^ 0x80
		out = append([]byte{firstByte}, out[1:]...)
	} else {
		// on negative numbers flip every bit
		out = make([]byte, 8)
		for i, b := range buf.Bytes() {
			out[i] = b ^ 0xFF
		}
	}

	return out, nil
}

// ParseLexicographicallySortableFloat64 reverses the changes in
// LexicographicallySortableFloat64
func ParseLexicographicallySortableFloat64(in []byte) (float64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("float64 must be 8 bytes long, got: %d", len(in))
	}

	flipped := make([]byte, 8)
	if in[0]&0x80 == 0x80 {
		// encoded as negative means it was originally positive, so we only need to
		// flip the sign
		flipped[0] = in[0] ^ 0x80

		// the remainder can be copied
		for i := 1; i < 8; i++ {
			flipped[i] = in[i]
		}
	} else {
		// encoded as positive means it was originally negative, so we need to flip
		// everything
		for i := 0; i < 8; i++ {
			flipped[i] = in[i] ^ 0xFF
		}
	}

	r := bytes.NewReader(flipped)
	var value float64

	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, errors.Wrap(err, "deserialize float64 value as big endian")
	}

	return value, nil
}

// LexicographicallySortableInt64 performs a conversion to a lexicographically
// sortable byte slice. For this, big endian notation is required and the sign
// must be flipped
func LexicographicallySortableInt64(in int64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	asInt64 := int64(in)

	// flip the sign
	asInt64 = asInt64 ^ math.MinInt64

	err := binary.Write(buf, binary.BigEndian, asInt64)
	if err != nil {
		return nil, errors.Wrap(err, "serialize int value as big endian")
	}

	return buf.Bytes(), nil
}

// ParseLexicographicallySortableInt64 reverses the changes in
// LexicographicallySortableInt64
func ParseLexicographicallySortableInt64(in []byte) (int64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("int64 must be 8 bytes long, got: %d", len(in))
	}

	r := bytes.NewReader(in)
	var value int64

	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, errors.Wrap(err, "deserialize int64 value as big endian")
	}

	return value ^ math.MinInt64, nil
}

// LexicographicallySortableUint64 performs a conversion to a lexicographically
// sortable byte slice. For this, big endian notation is required.
func LexicographicallySortableUint64(in uint64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// no signs to flip as this is a uint
	err := binary.Write(buf, binary.BigEndian, in)
	if err != nil {
		return nil, errors.Wrap(err, "serialize int value as big endian")
	}

	return buf.Bytes(), nil
}

// ParseLexicographicallySortableUint64 reverses the changes in
// LexicographicallySortableUint64
func ParseLexicographicallySortableUint64(in []byte) (uint64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("uint64 must be 8 bytes long, got: %d", len(in))
	}

	r := bytes.NewReader(in)
	var value uint64

	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, errors.Wrap(err, "deserialize uint64 value as big endian")
	}

	return value, nil
}
