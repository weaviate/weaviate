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

// Package gobenc provides allocation-efficient encoding and decoding of
// map[uint64]uint32. Decoding/gob's reflection machinery causes thousands of heap
// allocations per call which is a serious problem in hot paths.
// This package produces the identical wire format so old and new segments are
// fully interchangeable (backward and forward compatible during rolling
// upgrades), while cutting allocations to near zero.
package gobenc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
)

// gobMapPreamble is the fixed gob type-definition message for map[uint64]uint32.
// Every fresh gob.Encoder emits these 14 bytes before the first data message.
var gobMapPreamble = []byte{13, 127, 4, 1, 2, 255, 128, 0, 1, 6, 1, 6, 0, 0}

// dataPrefix is the fixed 3-byte sequence at the start of every data message
// body: type id 64 encoded as gob int (0xFF 0x80) + singleton delta (0x00).
var dataPrefix = []byte{255, 128, 0}

// Encode serializes a map[uint64]uint32 into gob-compatible wire format.
// The output is byte-identical to what encoding/gob produces for this type
// (modulo map iteration order of entries).
func Encode(m map[uint64]uint32) []byte {
	// Pre-compute data body size: 3 (dataPrefix) + count + entries
	bodySize := len(dataPrefix) + gobUintSize(uint64(len(m)))
	for k, v := range m {
		bodySize += gobUintSize(k) + gobUintSize(uint64(v))
	}

	// Total: preamble + msg length + body
	totalSize := len(gobMapPreamble) + gobUintSize(uint64(bodySize)) + bodySize
	buf := make([]byte, 0, totalSize)

	// Write preamble
	buf = append(buf, gobMapPreamble...)

	// Write message length
	buf = appendGobUint(buf, uint64(bodySize))

	// Write data prefix (type id + singleton delta)
	buf = append(buf, dataPrefix...)

	// Write map count
	buf = appendGobUint(buf, uint64(len(m)))

	// Write entries
	for k, v := range m {
		buf = appendGobUint(buf, k)
		buf = appendGobUint(buf, uint64(v))
	}

	return buf
}

// Decode deserializes gob-encoded bytes back into a map[uint64]uint32.
// It first attempts a fast manual decode. If the preamble doesn't match
// (e.g. data written by a different gob encoder configuration), it falls
// back to encoding/gob.
func Decode(data []byte) (map[uint64]uint32, error) {
	m, err := decodeFast(data)
	if err == nil {
		return m, nil
	}

	// Fallback to stdlib gob for unexpected formats
	dec := gob.NewDecoder(bytes.NewReader(data))
	m = map[uint64]uint32{}
	if gobErr := dec.Decode(&m); gobErr != nil {
		return nil, fmt.Errorf("gobenc: fast decode failed (%w), gob fallback also failed: %w", err, gobErr)
	}
	return m, nil
}

func decodeFast(data []byte) (map[uint64]uint32, error) {
	if len(data) < len(gobMapPreamble)+1 {
		return nil, fmt.Errorf("data too short (%d bytes)", len(data))
	}

	if !isGobMap(data) {
		return nil, fmt.Errorf("preamble mismatch")
	}

	pos := len(gobMapPreamble)

	// Read message length
	msgLen, n, err := readGobUint(data, pos)
	if err != nil {
		return nil, fmt.Errorf("read message length: %w", err)
	}
	pos += n

	if msgLen > uint64(len(data)-pos) {
		return nil, fmt.Errorf("message length %d exceeds data (%d bytes remaining)", msgLen, len(data)-pos)
	}
	msgEnd := pos + int(msgLen)

	// Verify and skip data prefix (type id + singleton delta)
	if pos+len(dataPrefix) > msgEnd {
		return nil, fmt.Errorf("data prefix truncated")
	}
	if data[pos] != dataPrefix[0] || data[pos+1] != dataPrefix[1] || data[pos+2] != dataPrefix[2] {
		return nil, fmt.Errorf("data prefix mismatch")
	}
	pos += len(dataPrefix)

	// Read map count
	count, n, err := readGobUint(data, pos)
	if err != nil {
		return nil, fmt.Errorf("read map count: %w", err)
	}
	pos += n

	// Each entry is at least 2 bytes (1 byte key + 1 byte value).
	if count > uint64(msgEnd-pos)/2 {
		return nil, fmt.Errorf("count %d too large for remaining %d bytes", count, msgEnd-pos)
	}

	m := make(map[uint64]uint32, count)

	for i := range count {
		key, n, err := readGobUint(data, pos)
		if err != nil {
			return nil, fmt.Errorf("read key %d: %w", i, err)
		}
		pos += n

		val, n, err := readGobUint(data, pos)
		if err != nil {
			return nil, fmt.Errorf("read value %d: %w", i, err)
		}
		pos += n

		if val > math.MaxUint32 {
			return nil, fmt.Errorf("value %d at index %d overflows uint32", val, i)
		}
		m[key] = uint32(val)
	}

	if pos != msgEnd {
		return nil, fmt.Errorf("trailing bytes: decoded to offset %d, expected %d", pos, msgEnd)
	}

	return m, nil
}

// isGobMap reports whether data begins with the gob preamble for map[uint64]uint32.
func isGobMap(data []byte) bool {
	if len(data) < len(gobMapPreamble) {
		return false
	}
	for i, b := range gobMapPreamble {
		if data[i] != b {
			return false
		}
	}
	return true
}

// gobUintSize returns the number of bytes needed to encode x as a gob uint.
func gobUintSize(x uint64) int {
	if x <= 127 {
		return 1
	}
	n := 0
	for v := x; v > 0; v >>= 8 {
		n++
	}
	return 1 + n // 1 byte for count + n bytes for value
}

// appendGobUint appends the gob-encoded uint to buf and returns the extended slice.
func appendGobUint(buf []byte, x uint64) []byte {
	if x <= 127 {
		return append(buf, byte(x))
	}
	// Encode into a temporary big-endian buffer
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], x)
	// Find the first non-zero byte
	i := 0
	for i < 7 && tmp[i] == 0 {
		i++
	}
	n := 8 - i // number of significant bytes
	buf = append(buf, byte(256-n))
	buf = append(buf, tmp[i:]...)
	return buf
}

// readGobUint reads a gob-encoded uint from data at the given offset.
// Returns the value, the number of bytes consumed, and any error.
func readGobUint(data []byte, pos int) (uint64, int, error) {
	if pos >= len(data) {
		return 0, 0, fmt.Errorf("unexpected end of data at offset %d", pos)
	}
	b := data[pos]
	if b <= 127 {
		return uint64(b), 1, nil
	}
	n := int(256 - int(b)) // number of value bytes
	if n > 8 {
		return 0, 0, fmt.Errorf("invalid gob uint length %d at offset %d", n, pos)
	}
	if pos+1+n > len(data) {
		return 0, 0, fmt.Errorf("need %d bytes at offset %d, have %d", n, pos+1, len(data)-pos-1)
	}
	var val uint64
	for i := range n {
		val = (val << 8) | uint64(data[pos+1+i])
	}
	return val, 1 + n, nil
}
