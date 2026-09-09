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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLexicographicallySortableFloat64NegativeZero verifies that -0.0 and 0.0
// produce identical byte representations, satisfying IEEE 754 equality.
func TestLexicographicallySortableFloat64NegativeZero(t *testing.T) {
	negZeroBytes, err := LexicographicallySortableFloat64(math.Copysign(0, -1))
	require.Nil(t, err)

	posZeroBytes, err := LexicographicallySortableFloat64(0.0)
	require.Nil(t, err)

	assert.Equal(t, posZeroBytes, negZeroBytes, "-0.0 and 0.0 must encode identically")

	// Also verify ordering: -1000000.0 must sort below 0.0
	negBigBytes, err := LexicographicallySortableFloat64(-1000000.0)
	require.Nil(t, err)

	// negBigBytes < posZeroBytes lexicographically
	assert.True(t, string(negBigBytes) < string(posZeroBytes),
		"-1000000.0 must sort before 0.0")
	// negZeroBytes must not sort before negBigBytes
	assert.False(t, string(negZeroBytes) < string(negBigBytes),
		"-0.0 must not sort before -1000000.0")
}

// TestSerialization makes sure that writing and reading into the
// lexicographically sortable types byte slices ends up with the same values as
// original. There is no focus on the sortability itself, as that is already
// tested extensively in analyzer_test.go
func TestSerialization(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		subjects := []float64{
			math.SmallestNonzeroFloat64,
			-400.0001,
			-21,
			0,
			21,
			400.0001,
			math.MaxFloat64,
		}

		for _, sub := range subjects {
			t.Run(fmt.Sprintf("with %f", sub), func(t *testing.T) {
				bytes, err := LexicographicallySortableFloat64(sub)
				require.Nil(t, err)

				parsed, err := ParseLexicographicallySortableFloat64(bytes)
				require.Nil(t, err)

				assert.Equal(t, sub, parsed, "before and after must match")
			})
		}
	})

	t.Run("int64", func(t *testing.T) {
		subjects := []int64{
			math.MinInt64,
			-400,
			-21,
			0,
			21,
			400,
			math.MaxInt64,
		}

		for _, sub := range subjects {
			t.Run(fmt.Sprintf("with %d", sub), func(t *testing.T) {
				bytes, err := LexicographicallySortableInt64(sub)
				require.Nil(t, err)

				parsed, err := ParseLexicographicallySortableInt64(bytes)
				require.Nil(t, err)

				assert.Equal(t, sub, parsed, "before and after must match")
			})
		}
	})

	t.Run("uint64", func(t *testing.T) {
		subjects := []uint64{
			0,
			21,
			400,
			math.MaxUint64,
		}

		for _, sub := range subjects {
			t.Run(fmt.Sprintf("with %d", sub), func(t *testing.T) {
				bytes, err := LexicographicallySortableUint64(sub)
				require.Nil(t, err)

				parsed, err := ParseLexicographicallySortableUint64(bytes)
				require.Nil(t, err)

				assert.Equal(t, sub, parsed, "before and after must match")
			})
		}
	})
}

// The tests below pin the exact byte encoding produced by the
// LexicographicallySortable* functions. The encoding is an on-disk format:
// segment files store keys encoded this way, so any change to it breaks
// reads of previously written data. Round-trip tests alone cannot catch
// such a change, as both directions would change together. Entries are
// ordered ascending by value so the encodings can also be checked to sort
// in value order, which is the property the format exists for.

func TestLexicographicallySortableInt64Golden(t *testing.T) {
	tests := []struct {
		in   int64
		want []byte
	}{
		{math.MinInt64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MinInt64 + 1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{-1_000_000_000_000_000_000, []byte{0x72, 0x1f, 0x49, 0x4c, 0x58, 0x9c, 0x00, 0x00}},
		{-4_294_967_296, []byte{0x7f, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00}},
		{-1_000_000, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xf0, 0xbd, 0xc0}},
		{-256, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}},
		{-2, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{-1, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{0, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{2, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}},
		{255, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff}},
		{1_000_000, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40}},
		{math.MaxInt64 - 1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{math.MaxInt64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			got, err := LexicographicallySortableInt64(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			parsed, err := ParseLexicographicallySortableInt64(got)
			require.NoError(t, err)
			assert.Equal(t, tt.in, parsed)
		})
	}

	for i := 1; i < len(tests); i++ {
		assert.Negative(t, bytes.Compare(tests[i-1].want, tests[i].want),
			"encoding of %d must sort before encoding of %d", tests[i-1].in, tests[i].in)
	}
}

func TestLexicographicallySortableFloat64Golden(t *testing.T) {
	tests := []struct {
		in   float64
		want []byte
	}{
		{math.Inf(-1), []byte{0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{-math.MaxFloat64, []byte{0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{-1e10, []byte{0x3d, 0xfd, 0x5f, 0xa0, 0xdf, 0xff, 0xff, 0xff}},
		{-math.Pi, []byte{0x3f, 0xf6, 0xde, 0x04, 0xab, 0xbb, 0xd2, 0xe7}},
		{-1, []byte{0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{-0.5, []byte{0x40, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{-math.SmallestNonzeroFloat64, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{math.Copysign(0, -1), []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{0, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.SmallestNonzeroFloat64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{0.5, []byte{0xbf, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.Pi, []byte{0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}},
		{math.MaxFloat64, []byte{0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.Inf(1), []byte{0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.in), func(t *testing.T) {
			got, err := LexicographicallySortableFloat64(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			parsed, err := ParseLexicographicallySortableFloat64(got)
			require.NoError(t, err)
			// compare bit patterns so ±0 stay distinguishable; -0.0 is
			// normalized on encoding and round-trips to +0.0
			wantParsed := tt.in
			if tt.in == 0 {
				wantParsed = 0
			}
			assert.Equal(t, math.Float64bits(wantParsed), math.Float64bits(parsed))
		})
	}

	for i := 1; i < len(tests); i++ {
		cmp := bytes.Compare(tests[i-1].want, tests[i].want)
		if tests[i-1].in < tests[i].in {
			assert.Negative(t, cmp,
				"encoding of %v must sort before encoding of %v", tests[i-1].in, tests[i].in)
		} else {
			// equal values (-0.0 and 0.0) must encode identically
			assert.Zero(t, cmp,
				"encodings of equal values %v and %v must match", tests[i-1].in, tests[i].in)
		}
	}
}

func TestLexicographicallySortableUint64Golden(t *testing.T) {
	tests := []struct {
		in   uint64
		want []byte
	}{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{2, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}},
		{255, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff}},
		{256, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}},
		{65_535, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff}},
		{65_536, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}},
		{1 << 20, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00}},
		{4_294_967_295, []byte{0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff}},
		{4_294_967_296, []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
		{1_000_000_000_000_000_000, []byte{0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00}},
		{math.MaxUint64 / 2, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1 << 63, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MaxUint64 - 1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			got, err := LexicographicallySortableUint64(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			parsed, err := ParseLexicographicallySortableUint64(got)
			require.NoError(t, err)
			assert.Equal(t, tt.in, parsed)
		})
	}

	for i := 1; i < len(tests); i++ {
		assert.Negative(t, bytes.Compare(tests[i-1].want, tests[i].want),
			"encoding of %d must sort before encoding of %d", tests[i-1].in, tests[i].in)
	}
}
