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
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode(map[uint64]uint32{}))

	// The first 14 bytes are the type-definition preamble
	require.True(t, len(buf.Bytes()) >= len(GobMapPreamble),
		"gob output shorter than expected preamble")
	assert.Equal(t, GobMapPreamble, buf.Bytes()[:len(GobMapPreamble)],
		"hardcoded preamble must match what encoding/gob produces")
}

func TestPreambleUniqueness(t *testing.T) {
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
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			require.NoError(t, enc.Encode(tc.val))

			data := buf.Bytes()
			if len(data) >= len(GobMapPreamble) {
				assert.NotEqual(t, GobMapPreamble, data[:len(GobMapPreamble)],
					"preamble for %s must differ from map[uint64]uint32", tc.name)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
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
			encoded := Encode(tc.m)
			decoded, err := Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.m, decoded)
		})
	}
}

func TestCrossCompatGobToCustom(t *testing.T) {
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

func TestIsGobMap(t *testing.T) {
	assert.True(t, IsGobMap(Encode(map[uint64]uint32{})))
	assert.True(t, IsGobMap(Encode(map[uint64]uint32{1: 2})))

	assert.False(t, IsGobMap(nil))
	assert.False(t, IsGobMap([]byte{}))
	assert.False(t, IsGobMap([]byte{0, 1, 2}))
	assert.False(t, IsGobMap([]byte("hello world, this is not gob data")))

	// Other gob types should not match
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	require.NoError(t, enc.Encode([]uint64{1, 2, 3}))
	assert.False(t, IsGobMap(buf.Bytes()))
}

func TestDecodeErrors(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		_, err := Decode(nil)
		assert.Error(t, err)
	})

	t.Run("too_short", func(t *testing.T) {
		_, err := Decode([]byte{1, 2, 3})
		assert.Error(t, err)
	})

	t.Run("bad_preamble", func(t *testing.T) {
		bad := make([]byte, 20)
		_, err := Decode(bad)
		assert.Error(t, err)
	})

	t.Run("truncated_message", func(t *testing.T) {
		encoded := Encode(map[uint64]uint32{1: 2})
		// Truncate data message
		_, err := Decode(encoded[:len(GobMapPreamble)+2])
		assert.Error(t, err)
	})
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
