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
	"bytes"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ent "github.com/weaviate/weaviate/entities/inverted"
)

// TestExtractBoolValue pins the filter-side bool encoding byte-for-byte and
// its parity with the indexed representation written by Analyzer.Bool: a
// filter key that drifts from the written key silently matches nothing.
func TestExtractBoolValue(t *testing.T) {
	s := &Searcher{}
	a := NewAnalyzer(nil, "Test")

	tests := []struct {
		in   bool
		want []byte
	}{
		{false, []byte{0}},
		{true, []byte{1}},
	}

	for _, tt := range tests {
		got, err := s.extractBoolValue(tt.in)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)

		indexed, err := a.Bool(tt.in)
		require.NoError(t, err)
		require.Len(t, indexed, 1)
		assert.Equal(t, indexed[0].Data, got, "filter key must match indexed key for %v", tt.in)
	}

	_, err := s.extractBoolValue("not a bool")
	assert.ErrorContains(t, err, "expected value to be bool")
}

// TestEncodeKeys pins the batched slab encoders: each key equals the
// single-value encoding, every key's capacity is capped to its own end so an
// append cannot clobber the next, and a bad value reports its position.
func TestEncodeKeys(t *testing.T) {
	s := &Searcher{}

	// assertKeysMatch checks the encoded keys against the single-value encoder
	// and that each key's capacity is capped to keyLen, so appending to one
	// cannot reach the next.
	//
	// The expectation is built from every input value and then ordered and
	// deduplicated the way the builders do, and its length is asserted. Sizing
	// it from keys.Len() instead would make it agree with whatever the encoder
	// produced — including nothing at all, since the range body would simply
	// not run.
	assertKeysMatch := func(t *testing.T, keys ent.SortedKeys, keyLen, numValues int, want func(i int) []byte) {
		t.Helper()
		expected := make([][]byte, numValues)
		for i := range expected {
			expected[i] = want(i)
		}
		slices.SortFunc(expected, bytes.Compare)
		expected = slices.CompactFunc(expected, bytes.Equal)

		require.Equal(t, len(expected), keys.Len(), "one key per distinct value")
		for i, key := range keys.All() {
			assert.Equalf(t, expected[i], key, "key %d bytes", i)
			assert.Equalf(t, keyLen, cap(key), "key %d capacity must be capped to its own %d-byte end", i, keyLen)
		}
	}

	t.Run("int", func(t *testing.T) {
		// Deliberately not in ascending order: a fixture that is already sorted
		// passes whether or not the encoder orders anything.
		values := []int{0, 42, -1_000_000, 1, -1}
		keys, err := encodeIntKeys(values)
		require.NoError(t, err)
		assertKeysMatch(t, keys, 8, len(values), func(i int) []byte {
			b, err := s.extractIntValue(values[i])
			require.NoError(t, err)
			return b
		})
	})

	t.Run("number", func(t *testing.T) {
		values := []float64{0.5, math.MaxFloat64, -math.Pi, 0}
		keys, err := encodeNumberKeys(values)
		require.NoError(t, err)
		assertKeysMatch(t, keys, 8, len(values), func(i int) []byte {
			b, err := s.extractNumberValue(values[i])
			require.NoError(t, err)
			return b
		})
	})

	t.Run("bool", func(t *testing.T) {
		// Three values, two distinct keys: the shape where dedup shrinks the list.
		values := []bool{true, false, true}
		keys, err := encodeBoolKeys(values)
		require.NoError(t, err)
		assertKeysMatch(t, keys, 1, len(values), func(i int) []byte {
			b, err := s.extractBoolValue(values[i])
			require.NoError(t, err)
			return b
		})
	})

	t.Run("date", func(t *testing.T) {
		// The second value is the earlier instant (19:00Z the previous day) but
		// the later string, so a sort over the values rather than their encodings
		// would order these the other way round.
		values := []string{"2020-12-31T23:00:00Z", "2021-01-01T00:00:00+05:00"}
		keys, err := encodeDateKeys(values)
		require.NoError(t, err)
		assertKeysMatch(t, keys, 8, len(values), func(i int) []byte {
			b, err := s.extractDateValue(values[i])
			require.NoError(t, err)
			return b
		})
	})

	t.Run("uuid", func(t *testing.T) {
		values := []string{
			"ffffffff-ffff-ffff-ffff-ffffffffffff",
			"00000000-0000-0000-0000-000000000001",
		}
		keys, err := encodeUUIDKeys(values)
		require.NoError(t, err)
		assertKeysMatch(t, keys, 16, len(values), func(i int) []byte {
			b, err := s.extractUUIDValue(values[i])
			require.NoError(t, err)
			return b
		})
	})

	t.Run("keys are independent", func(t *testing.T) {
		keys, err := encodeIntKeys([]int{1, 2, 3})
		require.NoError(t, err)
		neighbor := append([]byte(nil), keys.At(1)...)
		// no spare capacity, so this reallocates and cannot reach key 1
		grown := append(keys.At(0), 0xff, 0xff, 0xff, 0xff)
		require.Len(t, grown, len(keys.At(0))+4)
		assert.Equal(t, neighbor, keys.At(1), "append to one key must not clobber the next")
	})

	t.Run("empty and single", func(t *testing.T) {
		empty, err := encodeIntKeys(nil)
		require.NoError(t, err)
		assert.Zero(t, empty.Len())

		one, err := encodeIntKeys([]int{7})
		require.NoError(t, err)
		require.Equal(t, 1, one.Len())
	})

	t.Run("encode error reports the failing index", func(t *testing.T) {
		_, err := encodeDateKeys([]string{"2020-01-02T03:04:05Z", "not-a-date"})
		require.ErrorContains(t, err, "value 1")

		_, err = encodeUUIDKeys([]string{"00000000-0000-0000-0000-000000000001", "not-a-uuid"})
		require.ErrorContains(t, err, "value 1")
	})
}
