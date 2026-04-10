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

package gobenc

import (
	"bytes"
	"encoding/gob"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreambleStability(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode(map[uint64]uint32{}))

	// The preamble produced by the current Go version must be parseable.
	// If this fails after a Go upgrade, the gob wire format for maps has changed
	// and parseGobMapPreamble (gobenc.go) needs updating. The init() function
	// will also panic at startup, so this must be fixed before releasing.
	preambleLen, typeID, err := parseGobMapPreamble(buf.Bytes())
	require.NoError(t, err, "preamble produced by current Go version must be parseable — "+
		"if this fails after a Go upgrade, update parseGobMapPreamble in gobenc.go")
	assert.True(t, typeID > 0, "type ID must be positive")

	// The version-independent suffix must appear at the end of the preamble.
	got := buf.Bytes()[preambleLen-len(gobMapSuffix) : preambleLen]
	assert.Equal(t, gobMapSuffix, got, "suffix must match")
}

// This tests that the suffix check correctly rejects types that are NOT a map
// of unsigned integers. Note: map[uint64]uint64, map[uint32]uint32, etc. are
// wire-identical to map[uint64]uint32 because gob uses a single "uint" type
// for all unsigned integer widths, so they correctly match our suffix check.
func TestPreambleUniqueness(t *testing.T) {
	t.Parallel()
	otherTypes := []struct {
		name string
		val  any
	}{
		{"[]uint64", []uint64{1}},
		{"[]uint32", []uint32{1, 2}},
		{"[]int64", []int64{-1, 0, 1}},
		{"[]byte", []byte{1, 2, 3}},
		{"[]string", []string{"a", "b"}},
		{"[]float64", []float64{1.5, 2.5}},
		{"string", "hello"},
		{"int", 42},
		{"uint64", uint64(99)},
		{"float64", 3.14},
		{"bool", true},
		{"struct", struct{ X int }{42}},
		{"map[string]uint32", map[string]uint32{"a": 1}},
		{"map[uint64]string", map[uint64]string{1: "a"}},
		{"map[int64]int32", map[int64]int32{1: 2}},
		{"map[string]string", map[string]string{"a": "b"}},
	}

	for _, tc := range otherTypes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			require.NoError(t, enc.Encode(tc.val))

			_, _, err := parseGobMapPreamble(buf.Bytes())
			assert.Error(t, err,
				"preamble for %s must not parse as map[uint64]uint32", tc.name)
		})
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		m    map[uint64]uint32
	}{
		{"empty", map[uint64]uint32{}},
		{"single", map[uint64]uint32{1: 2}},
		{"zero_key_value", map[uint64]uint32{0: 0}},
		{"small_values", map[uint64]uint32{1: 2, 3: 4, 5: 6}},
		{"large_keys", map[uint64]uint32{
			1<<32 - 1: 100,
			1<<48 - 1: 200,
			1<<56 - 1: 300,
		}},
		{"max_values", map[uint64]uint32{
			math.MaxUint64: math.MaxUint32,
		}},
		{"boundary_127", map[uint64]uint32{127: 127}},
		{"boundary_128", map[uint64]uint32{128: 128}},
		{"thousand_entries", makeMap(1000)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := Encode(tc.m)
			require.NoError(t, err)
			decoded, err := Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.m, decoded)
		})
	}
}

func TestCrossCompatGobToCustom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		m    map[uint64]uint32
	}{
		{"empty", map[uint64]uint32{}},
		{"single", map[uint64]uint32{42: 100}},
		{"multi", map[uint64]uint32{1: 2, 100: 200, 1000: 2000}},
		{"max", map[uint64]uint32{math.MaxUint64: math.MaxUint32}},
		{"thousand", makeMap(1000)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Encode with stdlib gob
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			require.NoError(t, enc.Encode(tc.m))

			// Decode with our decoder
			decoded, err := Decode(buf.Bytes())
			require.NoError(t, err)
			assert.Equal(t, tc.m, decoded)
		})
	}
}

func TestCrossCompatCustomToGob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		m    map[uint64]uint32
	}{
		{"empty", map[uint64]uint32{}},
		{"single", map[uint64]uint32{42: 100}},
		{"multi", map[uint64]uint32{1: 2, 100: 200, 1000: 2000}},
		{"max", map[uint64]uint32{math.MaxUint64: math.MaxUint32}},
		{"thousand", makeMap(1000)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Encode with our encoder
			encoded, err := Encode(tc.m)
			require.NoError(t, err)

			// Decode with stdlib gob
			dec := gob.NewDecoder(bytes.NewReader(encoded))
			result := map[uint64]uint32{}
			require.NoError(t, dec.Decode(&result))
			assert.Equal(t, tc.m, result)
		})
	}
}

func Test_parseGobMapPreamble(t *testing.T) {
	t.Parallel()

	// Valid preambles from our encoder (type ID is Go-version-dependent)
	enc1, err := Encode(map[uint64]uint32{})
	require.NoError(t, err)
	preambleLen, typeID, err := parseGobMapPreamble(enc1)
	require.NoError(t, err)
	assert.Equal(t, len(gobMapPreamble), preambleLen)
	assert.True(t, typeID > 0)

	enc2, err := Encode(map[uint64]uint32{1: 2})
	require.NoError(t, err)
	preambleLen, typeID, err = parseGobMapPreamble(enc2)
	require.NoError(t, err)
	assert.Equal(t, len(gobMapPreamble), preambleLen)
	assert.True(t, typeID > 0)

	// Invalid data
	_, _, err = parseGobMapPreamble(nil)
	assert.Error(t, err)

	_, _, err = parseGobMapPreamble([]byte{})
	assert.Error(t, err)

	_, _, err = parseGobMapPreamble([]byte{0, 1, 2})
	assert.Error(t, err)

	_, _, err = parseGobMapPreamble([]byte("hello world, this is not gob data"))
	assert.Error(t, err)

	// Other gob types should not match
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode([]uint64{1, 2, 3}))
	_, _, err = parseGobMapPreamble(buf.Bytes())
	assert.Error(t, err)
}

// TestKnownGoPreambles verifies the exact preamble bytes observed in CI from
// different Go versions are correctly parsed.
func TestKnownGoPreambles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bytes  []byte
		typeID int64
	}{
		{
			name:   "go1.26_typeID_64",
			bytes:  []byte{0x0d, 0x7f, 0x04, 0x01, 0x02, 0xff, 0x80, 0x00, 0x01, 0x06, 0x01, 0x06, 0x00, 0x00},
			typeID: 64,
		},
		{
			name:   "go1.25_typeID_65",
			bytes:  []byte{0x0e, 0xff, 0x81, 0x04, 0x01, 0x02, 0xff, 0x82, 0x00, 0x01, 0x06, 0x01, 0x06, 0x00, 0x00},
			typeID: 65,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			preambleLen, typeID, err := parseGobMapPreamble(tc.bytes)
			require.NoError(t, err)
			assert.Equal(t, len(tc.bytes), preambleLen)
			assert.Equal(t, tc.typeID, typeID)
		})
	}
}

// TestCrossVersionDecode verifies that data encoded with different type IDs
// (as produced by different Go versions) can be decoded correctly.
func TestCrossVersionDecode(t *testing.T) {
	t.Parallel()

	typeIDs := []struct {
		name   string
		typeID int64
	}{
		{"id64_go1.26", 64},
		{"id65_go1.25", 65},
		{"id32_hypothetical", 32},
		{"id128_hypothetical", 128},
	}

	maps := []struct {
		name string
		m    map[uint64]uint32
	}{
		{"empty", map[uint64]uint32{}},
		{"single", map[uint64]uint32{1: 2}},
		{"multi", map[uint64]uint32{1: 2, 100: 200, 1000: 2000}},
		{"max", map[uint64]uint32{math.MaxUint64: math.MaxUint32}},
		{"thousand", makeMap(1000)},
	}

	for _, tid := range typeIDs {
		for _, tc := range maps {
			t.Run(tid.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				data := buildGobStream(tid.typeID, tc.m)
				decoded, err := Decode(data)
				require.NoError(t, err)
				assert.Equal(t, tc.m, decoded)
			})
		}
	}
}

// buildGobStream constructs a complete gob wire-format encoding of m using the
// given type ID. This simulates what different Go versions produce when they
// assign different internal type IDs to map[uint64]uint32.
func buildGobStream(typeID int64, m map[uint64]uint32) []byte {
	// Build preamble body: [neg_type_id] [infix] [pos_type_id] [suffix]
	pBody := appendGobInt(nil, -typeID)
	pBody = append(pBody, gobMapInfix...)
	pBody = appendGobInt(pBody, typeID)
	pBody = append(pBody, gobMapSuffix...)

	// Preamble: [msg_length] [body]
	preamble := appendGobUint(nil, uint64(len(pBody)))
	preamble = append(preamble, pBody...)

	// Data prefix: type ID as gob int + singleton delta (0x00)
	dp := appendGobInt(nil, typeID)
	dp = append(dp, 0x00)

	// Data body: [data_prefix] [count] [entries...]
	body := make([]byte, 0, len(dp)+gobUintSize(uint64(len(m)))+len(m)*18)
	body = append(body, dp...)
	body = appendGobUint(body, uint64(len(m)))
	for k, v := range m {
		body = appendGobUint(body, k)
		body = appendGobUint(body, uint64(v))
	}

	// Full stream: [preamble] [data_msg_length] [data_body]
	result := make([]byte, 0, len(preamble)+gobUintSize(uint64(len(body)))+len(body))
	result = append(result, preamble...)
	result = appendGobUint(result, uint64(len(body)))
	result = append(result, body...)

	return result
}

// TestDecodeFallbackToGob verifies that Decode falls back to stdlib gob when
// decodeFast rejects the preamble. This covers the safety-net path for data
// that is valid gob but has a preamble our fast parser doesn't handle (e.g.
// a CommonType with a Name field, which changes the infix byte sequence).
func TestDecodeFallbackToGob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		m    map[uint64]uint32
	}{
		{"empty", map[uint64]uint32{}},
		{"single", map[uint64]uint32{42: 100}},
		{"multi", map[uint64]uint32{1: 2, 100: 200, 1000: 2000}},
		{"max", map[uint64]uint32{math.MaxUint64: math.MaxUint32}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Build a gob stream whose preamble includes a CommonType Name
			// field. This changes the infix from [04 01 02] to [04 01 01 ...]
			// causing decodeFast to reject, while stdlib gob decodes it fine.
			data := buildGobStreamWithName(64, "mymap", tc.m)

			// Fast path must reject this preamble.
			_, err := decodeFast(data)
			require.Error(t, err, "decodeFast should reject preamble with Name field")

			// Full Decode must succeed via gob fallback.
			decoded, err := Decode(data)
			require.NoError(t, err)
			assert.Equal(t, tc.m, decoded)
		})
	}
}

// buildGobStreamWithName is like buildGobStream but includes a CommonType Name
// field in the preamble, producing valid gob that decodeFast cannot parse.
func buildGobStreamWithName(typeID int64, name string, m map[uint64]uint32) []byte {
	// Preamble body:
	//   [neg_type_id] [04=MapT] [01=CommonType] [01=Name delta] [gob_string]
	//   [01=Id delta] [type_id] [00=end CommonType] [suffix]
	pBody := appendGobInt(nil, -typeID)
	pBody = append(pBody, 0x04) // wireType delta 4: MapT
	pBody = append(pBody, 0x01) // mapType delta 1: CommonType
	pBody = append(pBody, 0x01) // CommonType delta 1: Name field
	pBody = appendGobUint(pBody, uint64(len(name)))
	pBody = append(pBody, []byte(name)...)
	pBody = append(pBody, 0x01) // CommonType delta 1: Id field (delta from Name)
	pBody = appendGobInt(pBody, typeID)
	pBody = append(pBody, gobMapSuffix...) // end CommonType + key/elem/end

	preamble := appendGobUint(nil, uint64(len(pBody)))
	preamble = append(preamble, pBody...)

	// Data prefix: type ID as gob int + singleton delta (0x00)
	dp := appendGobInt(nil, typeID)
	dp = append(dp, 0x00)

	// Data body: [data_prefix] [count] [entries...]
	body := make([]byte, 0, len(dp)+gobUintSize(uint64(len(m)))+len(m)*18)
	body = append(body, dp...)
	body = appendGobUint(body, uint64(len(m)))
	for k, v := range m {
		body = appendGobUint(body, k)
		body = appendGobUint(body, uint64(v))
	}

	// Full stream: [preamble] [data_msg_length] [data_body]
	result := make([]byte, 0, len(preamble)+gobUintSize(uint64(len(body)))+len(body))
	result = append(result, preamble...)
	result = appendGobUint(result, uint64(len(body)))
	result = append(result, body...)

	return result
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()

	encoded3, err := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})
	require.NoError(t, err)

	tests := []struct {
		name   string
		data   func() []byte
		errMsg string
	}{
		{
			name:   "nil",
			data:   func() []byte { return nil },
			errMsg: "too short",
		},
		{
			name:   "too_short",
			data:   func() []byte { return []byte{1, 2, 3} },
			errMsg: "too short",
		},
		{
			name:   "bad_preamble",
			data:   func() []byte { return make([]byte, 20) },
			errMsg: "preamble mismatch",
		},
		{
			name: "truncated_at_message_length",
			data: func() []byte {
				return encoded3[:len(gobMapPreamble)+1]
			},
			errMsg: "exceeds data",
		},
		{
			name: "corrupted_data_prefix",
			data: func() []byte {
				d := make([]byte, len(encoded3))
				copy(d, encoded3)
				// The data prefix starts right after preamble + message length.
				// Corrupt the first byte of the data prefix.
				pos := len(gobMapPreamble)
				_, n, _ := readGobUint(d, pos) // skip message length field
				pos += n
				d[pos] ^= 0xFF
				return d
			},
			errMsg: "data prefix mismatch",
		},
		{
			name: "truncated_after_first_key",
			data: func() []byte {
				// Encode a 3-entry map, then shorten the message body so it
				// cuts off mid-stream (after the first key-value pair).
				full, _ := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})
				// Find where the data body starts (after preamble + msg length)
				pos := len(gobMapPreamble)
				_, n, _ := readGobUint(full, pos) // msg length
				pos += n
				// pos is now at the data body start; skip prefix + count + one KV pair
				pos += len(dataPrefix)
				_, n, _ = readGobUint(full, pos) // count
				pos += n
				_, n, _ = readGobUint(full, pos) // key 0
				pos += n
				_, n, _ = readGobUint(full, pos) // val 0
				pos += n
				// Truncate right after first entry
				return full[:pos]
			},
			errMsg: "exceeds data",
		},
		{
			name: "truncated_mid_entry_after_key",
			data: func() []byte {
				full, _ := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})
				pos := len(gobMapPreamble)
				_, n, _ := readGobUint(full, pos) // msg length
				pos += n
				pos += len(dataPrefix)
				_, n, _ = readGobUint(full, pos) // count
				pos += n
				_, n, _ = readGobUint(full, pos) // key 0
				pos += n
				_, n, _ = readGobUint(full, pos) // val 0
				pos += n
				_, n, _ = readGobUint(full, pos) // key 1
				pos += n
				// Truncate after second key, before its value
				return full[:pos]
			},
			errMsg: "exceeds data",
		},
		{
			name: "count_exceeds_remaining_data",
			data: func() []byte {
				// Encode an empty map, then overwrite the count to a huge value.
				d, _ := Encode(map[uint64]uint32{})
				pos := len(gobMapPreamble)
				_, n, _ := readGobUint(d, pos) // msg length
				pos += n
				pos += len(dataPrefix)
				// pos is now at the count byte; the empty map has count=0.
				// Replace with a large count that exceeds remaining bytes.
				d[pos] = 127 // 127 entries, but 0 bytes of data left
				return d
			},
			errMsg: "too large",
		},
		{
			name: "value_overflows_uint32",
			data: func() []byte {
				// Encode a valid single-entry map, then patch the value to be > MaxUint32.
				// We build the payload manually: preamble + msg length + dataPrefix + count(1) + key(1) + value(MaxUint32+1)
				val := uint64(math.MaxUint32) + 1
				bodySize := len(dataPrefix) + gobUintSize(1) + gobUintSize(1) + gobUintSize(val)
				buf := make([]byte, 0, len(gobMapPreamble)+gobUintSize(uint64(bodySize))+bodySize)
				buf = append(buf, gobMapPreamble...)
				buf = appendGobUint(buf, uint64(bodySize))
				buf = append(buf, dataPrefix...)
				buf = appendGobUint(buf, 1) // count
				buf = appendGobUint(buf, 1) // key
				buf = appendGobUint(buf, val)
				return buf
			},
			errMsg: "overflows uint32",
		},
		{
			name: "invalid_gob_uint_length_byte",
			data: func() []byte {
				// Craft data where readGobUint encounters a length byte implying n > 8.
				// Byte 128 means n = 256-128 = 128, which is invalid.
				// Place it where the map count is read.
				bodySize := len(dataPrefix) + 1 // dataPrefix + one byte for the bad count
				buf := make([]byte, 0, len(gobMapPreamble)+gobUintSize(uint64(bodySize))+bodySize)
				buf = append(buf, gobMapPreamble...)
				buf = appendGobUint(buf, uint64(bodySize))
				buf = append(buf, dataPrefix...)
				buf = append(buf, 128) // length byte: n = 256-128 = 128 > 8
				return buf
			},
			errMsg: "invalid gob uint length",
		},
		{
			name: "trailing_bytes_after_valid_entries",
			data: func() []byte {
				d, _ := Encode(map[uint64]uint32{1: 2})
				// Append extra bytes within the message by extending the slice
				// and patching the message length.
				extra := append(d, 0xFF, 0xFF)
				// Patch message length: it's right after the preamble
				pos := len(gobMapPreamble)
				origLen, n, _ := readGobUint(extra, pos)
				// Rebuild with larger message length
				buf := make([]byte, 0, len(extra)+2)
				buf = append(buf, gobMapPreamble...)
				buf = appendGobUint(buf, origLen+2)
				buf = append(buf, extra[pos+n:]...)
				return buf
			},
			errMsg: "trailing bytes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeFast(tc.data())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	m := makeMap(1000)
	for b.Loop() {
		_, _ = Encode(m)
	}
}

func BenchmarkDecode(b *testing.B) {
	m := makeMap(1000)
	data, _ := Encode(m)
	for b.Loop() {
		_, _ = Decode(data)
	}
}

func BenchmarkGobEncode(b *testing.B) {
	m := makeMap(1000)
	for b.Loop() {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(m)
	}
}

func BenchmarkGobDecode(b *testing.B) {
	m := makeMap(1000)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(m)
	data := buf.Bytes()
	for b.Loop() {
		dec := gob.NewDecoder(bytes.NewReader(data))
		result := map[uint64]uint32{}
		dec.Decode(&result)
	}
}

func makeMap(n int) map[uint64]uint32 {
	m := make(map[uint64]uint32, n)
	for i := range n {
		m[uint64(i)*7+3] = uint32(i*13 + 5)
	}
	return m
}
