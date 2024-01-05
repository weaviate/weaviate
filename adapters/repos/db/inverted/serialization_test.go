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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
