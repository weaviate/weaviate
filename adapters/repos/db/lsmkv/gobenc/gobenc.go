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
// map[uint64]uint32. The encoding/gob package's reflection machinery causes
// thousands of heap allocations per call which is a serious problem in hot paths.
// This package produces the identical wire format so old and new segments are
// fully interchangeable (backward and forward compatible during rolling
// upgrades), while cutting allocations to near zero.
//
// The package relies on the gob wire format for maps being stable across Go
// versions. If a future Go version changes the format, init() will panic at
// startup. TestPreambleStability and TestCrossCompatGobToCustom catch this in
// CI before any release — if they fail after a Go upgrade, the preamble
// parsing logic in this package needs updating.
package gobenc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
)

// gobMapSuffix is the Go-version-independent tail of every gob preamble for
// map[uint64]uint32. Different Go versions assign different internal type IDs
// which changes the first few bytes of the preamble, but this 7-byte suffix is
// always identical:
//
//	end-commonType(00) key-delta(01) key-type-uint(06) elem-delta(01)
//	elem-type-uint(06) end-mapType(00) end-wireType(00)
var gobMapSuffix = []byte{0x00, 0x01, 0x06, 0x01, 0x06, 0x00, 0x00}

// gobMapInfix is the constant bytes after the negative type ID in the preamble
// body: field-delta-4=MapT(04) field-delta-1=CommonType(01) field-delta-2=Id(02).
var gobMapInfix = []byte{0x04, 0x01, 0x02}

// gobMapPreamble is the gob type-definition message for map[uint64]uint32,
// captured from the current Go version's gob.Encoder at init time.
// dataPrefix is the type-ID + singleton-delta bytes that precede every data
// message body. Both are derived from the runtime so they are always correct.
//
// Encode uses these globals directly. Decode cannot use them directly — it
// must parse the type ID from the preamble in the data, which may have been
// written by a different Go version with a different type ID.
var (
	gobMapPreamble []byte
	dataPrefix     []byte
)

func init() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(map[uint64]uint32{}); err != nil {
		panic("gobenc: failed to encode reference map: " + err.Error())
	}

	preambleLen, typeID, err := parseGobMapPreamble(buf.Bytes())
	if err != nil {
		panic("gobenc: preamble produced by this Go version is unparseable: " + err.Error())
	}

	gobMapPreamble = make([]byte, preambleLen)
	copy(gobMapPreamble, buf.Bytes()[:preambleLen])

	dataPrefix = appendGobInt(nil, typeID)
	dataPrefix = append(dataPrefix, 0x00)
}

// Encode serializes a map[uint64]uint32 into gob-compatible wire format.
// The output is byte-identical to what encoding/gob produces for this type
// (modulo map iteration order of entries).
func Encode(m map[uint64]uint32) ([]byte, error) {
	// Pre-compute data body size: dataPrefix + count + entries
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

	if len(buf) != totalSize {
		return nil, fmt.Errorf("gobenc: size bookkeeping bug: predicted %d bytes, wrote %d", totalSize, len(buf))
	}

	return buf, nil
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
	preambleLen, typeID, err := parseGobMapPreamble(data)
	if err != nil {
		return nil, err
	}

	pos := preambleLen

	// Read the data message byte count (follows the preamble in the gob stream).
	msgLen, n, err := readGobUint(data, pos)
	if err != nil {
		return nil, fmt.Errorf("read message length: %w", err)
	}
	pos += n

	if msgLen > uint64(len(data)-pos) {
		return nil, fmt.Errorf("message length %d exceeds data (%d bytes remaining)", msgLen, len(data)-pos)
	}
	msgEnd := pos + int(msgLen)

	// Verify data prefix: type ID (gob int) + singleton delta (0x00).
	// Parse in-place to avoid allocating the expected bytes.
	prefixID, n, err := readGobInt(data, pos)
	if err != nil || pos+n >= msgEnd {
		return nil, fmt.Errorf("data prefix truncated")
	}
	if prefixID != typeID {
		return nil, fmt.Errorf("data prefix mismatch")
	}
	pos += n
	if data[pos] != 0x00 {
		return nil, fmt.Errorf("data prefix mismatch")
	}
	pos++

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

// parseGobMapPreamble validates that data begins with a gob type-definition
// for map[uint64]uint32 and returns the preamble length and the type ID.
//
// The preamble layout is:
//
//	[msg_length] [neg_type_id] [infix: 04 01 02] [pos_type_id] [suffix: 00 01 06 01 06 00 00]
//	 variable     variable      fixed              variable      fixed
//
// The type ID is assigned by the Go runtime and varies across versions, so we
// check the fixed infix/suffix against constants and only require the two type
// ID occurrences to be structurally valid and mutually consistent.
func parseGobMapPreamble(data []byte) (preambleLen int, typeID int64, err error) {
	// Minimum preamble: 1(msgLen) + 1(negID) + 3(infix) + 1(posID) + 7(suffix) = 13
	if len(data) < 13 {
		return 0, 0, fmt.Errorf("data too short (%d bytes)", len(data))
	}

	pos := 0

	// Read preamble message length (gob uint).
	msgLen, n, err := readGobUint(data, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	pos += n

	if msgLen > uint64(len(data)-pos) {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	preambleLen = pos + int(msgLen)

	// Verify suffix: the last 7 bytes of the preamble encode
	// "key=uint, elem=uint, end mapType, end wireType" — stable across Go versions.
	suffixStart := preambleLen - len(gobMapSuffix)
	if suffixStart < pos {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	for i, b := range gobMapSuffix {
		if data[suffixStart+i] != b {
			return 0, 0, fmt.Errorf("preamble mismatch")
		}
	}

	// Read type ID from the preamble header. Gob encodes it as a negative int
	// to distinguish type-definition messages from data messages.
	negID, n, err := readGobInt(data, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	pos += n

	if negID >= 0 {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	typeID = -negID

	// Verify infix: 04 01 02 (MapT / CommonType / Id field deltas).
	if pos+len(gobMapInfix) > suffixStart {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	for i, b := range gobMapInfix {
		if data[pos+i] != b {
			return 0, 0, fmt.Errorf("preamble mismatch")
		}
	}
	pos += len(gobMapInfix)

	// Read positive type ID in CommonType — must match the header.
	posID, n, err := readGobInt(data, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}
	pos += n

	if posID != typeID {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}

	// pos should land exactly at the suffix.
	if pos != suffixStart {
		return 0, 0, fmt.Errorf("preamble mismatch")
	}

	return preambleLen, typeID, nil
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

// appendGobInt appends a gob-encoded signed integer to buf.
func appendGobInt(buf []byte, x int64) []byte {
	var u uint64
	if x < 0 {
		u = uint64(^x<<1) | 1
	} else {
		u = uint64(x << 1)
	}
	return appendGobUint(buf, u)
}

// readGobInt reads a gob-encoded signed integer from data at pos.
func readGobInt(data []byte, pos int) (int64, int, error) {
	u, n, err := readGobUint(data, pos)
	if err != nil {
		return 0, 0, err
	}
	if u&1 == 1 {
		return ^int64(u >> 1), n, nil
	}
	return int64(u >> 1), n, nil
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
