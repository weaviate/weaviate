//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"encoding/binary"
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

// LexicographicallySortableInt64 performs a conversion to a lexicographically
// sortable byte slice. Fro this, big endian notation is required and the sign
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

// LexicographicallySortableUint32 performs a conversion to a lexicographically
// sortable byte slice. Fro this, big endian notation is required and the sign
// must be flipped
func LexicographicallySortableUint32(in uint32) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// no signs to flip as this is a uint
	err := binary.Write(buf, binary.BigEndian, in)
	if err != nil {
		return nil, errors.Wrap(err, "serialize int value as big endian")
	}

	return buf.Bytes(), nil
}
