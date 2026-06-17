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
