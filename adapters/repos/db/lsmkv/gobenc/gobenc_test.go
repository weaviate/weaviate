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

	// The first 14 bytes are the type-definition preamble
	require.True(t, len(buf.Bytes()) >= len(gobMapPreamble),
		"gob output shorter than expected preamble")
	assert.Equal(t, gobMapPreamble, buf.Bytes()[:len(gobMapPreamble)],
		"hardcoded preamble must match what encoding/gob produces")
}

// This tests that the gob-"magic bytes" we use to detect that it is a map[uint64]uint32 are in fact unique for that
// type
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
		{"map[uint64]uint64", map[uint64]uint64{1: 2}},
		{"map[uint32]uint32", map[uint32]uint32{1: 2}},
		{"map[uint32]uint64", map[uint32]uint64{1: 2}},
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

			assert.False(t, isGobMap(buf.Bytes()),
				"preamble for %s must differ from map[uint64]uint32", tc.name)
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

			encoded := Encode(tc.m)
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
			encoded := Encode(tc.m)

			// Decode with stdlib gob
			dec := gob.NewDecoder(bytes.NewReader(encoded))
			result := map[uint64]uint32{}
			require.NoError(t, dec.Decode(&result))
			assert.Equal(t, tc.m, result)
		})
	}
}

func Test_isGobMap(t *testing.T) {
	t.Parallel()

	assert.True(t, isGobMap(Encode(map[uint64]uint32{})))
	assert.True(t, isGobMap(Encode(map[uint64]uint32{1: 2})))

	assert.False(t, isGobMap(nil))
	assert.False(t, isGobMap([]byte{}))
	assert.False(t, isGobMap([]byte{0, 1, 2}))
	assert.False(t, isGobMap([]byte("hello world, this is not gob data")))

	// Other gob types should not match
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode([]uint64{1, 2, 3}))
	assert.False(t, isGobMap(buf.Bytes()))
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()

	encoded3 := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})

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
				prefixOffset := len(gobMapPreamble) + gobUintSize(uint64(len(encoded3)-len(gobMapPreamble)-1))
				d[prefixOffset] ^= 0xFF
				return d
			},
			errMsg: "data prefix mismatch",
		},
		{
			name: "truncated_after_first_key",
			data: func() []byte {
				// Encode a 3-entry map, then shorten the message body so it
				// cuts off mid-stream (after the first key-value pair).
				full := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})
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
				full := Encode(map[uint64]uint32{1: 2, 100: 200, 1000: 2000})
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
				d := Encode(map[uint64]uint32{})
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
				d := Encode(map[uint64]uint32{1: 2})
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
		Encode(m)
	}
}

func BenchmarkDecode(b *testing.B) {
	m := makeMap(1000)
	data := Encode(m)
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
