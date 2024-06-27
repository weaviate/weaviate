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

package lsmkv

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestReaderRoaringSetRange(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("with empty CursorRoaringSetRange", func(t *testing.T) {
		values := []uint64{0, 1, 4, 5, 6, 12, 13, 14, 12345678901234567890, math.MaxUint64}
		operators := []filters.Operator{
			filters.OperatorGreaterThanEqual,
			filters.OperatorGreaterThan,
			filters.OperatorLessThanEqual,
			filters.OperatorLessThan,
			filters.OperatorEqual,
			filters.OperatorNotEqual,
		}

		reader := NewBucketReaderRoaringSetRange(func() CursorRoaringSetRange {
			return newFakeCursorRoaringSetRange(map[uint64]uint64{})
		}, logger)

		for _, operator := range operators {
			t.Run(operator.Name(), func(t *testing.T) {
				for _, value := range values {
					t.Run(fmt.Sprintf("value %d", value), func(t *testing.T) {
						bm, err := reader.Read(context.Background(), value, operator)

						assert.NoError(t, err)
						require.NotNil(t, bm)
						assert.True(t, bm.IsEmpty())
					})
				}
			})
		}
	})
	t.Run("with populated CursorRoaringSetRange", func(t *testing.T) {
		type testCase struct {
			operator filters.Operator
			value    uint64
			expected []uint64
		}

		testCases := []testCase{
			// greater than equal
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    0,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    1,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    4,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    5,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    6,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    12,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    13,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    12345678901234567890,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64,
				expected: []uint64{},
			},
			// greater than
			{
				operator: filters.OperatorGreaterThan,
				value:    0,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    1,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    4,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    5,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    6,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    12,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    13,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    12345678901234567890,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64,
				expected: []uint64{},
			},
			// less than equal
			{
				operator: filters.OperatorLessThanEqual,
				value:    0,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    1,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    4,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    5,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    6,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    12,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    13,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    12345678901234567890,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			// less than
			{
				operator: filters.OperatorLessThan,
				value:    0,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThan,
				value:    1,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThan,
				value:    4,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThan,
				value:    5,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorLessThan,
				value:    6,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThan,
				value:    12,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThan,
				value:    13,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorLessThan,
				value:    14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    12345678901234567890,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			// equal
			{
				operator: filters.OperatorEqual,
				value:    0,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorEqual,
				value:    1,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    4,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    5,
				expected: []uint64{15, 25},
			},
			{
				operator: filters.OperatorEqual,
				value:    6,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    12,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    13,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorEqual,
				value:    14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    12345678901234567890,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64,
				expected: []uint64{},
			},
			// not equal
			{
				operator: filters.OperatorNotEqual,
				value:    0,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    1,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    4,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    5,
				expected: []uint64{10, 20, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    6,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    12,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    13,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    12345678901234567890,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
		}

		reader := NewBucketReaderRoaringSetRange(func() CursorRoaringSetRange {
			return newFakeCursorRoaringSetRange(map[uint64]uint64{
				113: 13, // 1101
				213: 13, // 1101
				15:  5,  // 0101
				25:  5,  // 0101
				10:  0,  // 0000
				20:  0,  // 0000
			})
		}, logger)

		for _, tc := range testCases {
			t.Run(tc.operator.Name(), func(t *testing.T) {
				t.Run(fmt.Sprintf("value %d", tc.value), func(t *testing.T) {
					bm, err := reader.Read(context.Background(), tc.value, tc.operator)

					assert.NoError(t, err)
					require.NotNil(t, bm)
					assert.ElementsMatch(t, tc.expected, bm.ToArray())
				})
			})
		}
	})

	t.Run("with populated CursorRoaringSetRange (high numbers)", func(t *testing.T) {
		type testCase struct {
			operator filters.Operator
			value    uint64
			expected []uint64
		}

		testCases := []testCase{
			// greater than equal
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    0,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    12345,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 13,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 12,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 6,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 5,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 4,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64 - 1,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorGreaterThanEqual,
				value:    math.MaxUint64,
				expected: []uint64{10, 20},
			},
			// greater than
			{
				operator: filters.OperatorGreaterThan,
				value:    0,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    12345,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 13,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 12,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 6,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 5,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 4,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64 - 1,
				expected: []uint64{10, 20},
			},
			{
				operator: filters.OperatorGreaterThan,
				value:    math.MaxUint64,
				expected: []uint64{},
			},
			// less than equal
			{
				operator: filters.OperatorLessThanEqual,
				value:    0,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    12345,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 13,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 12,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 6,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 5,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 4,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64 - 1,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThanEqual,
				value:    math.MaxUint64,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			// less than
			{
				operator: filters.OperatorLessThan,
				value:    0,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThan,
				value:    12345,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 13,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 12,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 6,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 5,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 4,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64 - 1,
				expected: []uint64{15, 25, 113, 213},
			},
			{
				operator: filters.OperatorLessThan,
				value:    math.MaxUint64,
				expected: []uint64{15, 25, 113, 213},
			},
			// equal
			{
				operator: filters.OperatorEqual,
				value:    0,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    12345,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 14,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 13,
				expected: []uint64{113, 213},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 12,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 6,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 5,
				expected: []uint64{15, 25},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 4,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64 - 1,
				expected: []uint64{},
			},
			{
				operator: filters.OperatorEqual,
				value:    math.MaxUint64,
				expected: []uint64{10, 20},
			},
			// not equal
			{
				operator: filters.OperatorNotEqual,
				value:    0,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    12345,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 14,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 13,
				expected: []uint64{10, 20, 15, 25},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 12,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 6,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 5,
				expected: []uint64{10, 20, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 4,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64 - 1,
				expected: []uint64{10, 20, 15, 25, 113, 213},
			},
			{
				operator: filters.OperatorNotEqual,
				value:    math.MaxUint64,
				expected: []uint64{15, 25, 113, 213},
			},
		}

		reader := NewBucketReaderRoaringSetRange(func() CursorRoaringSetRange {
			return newFakeCursorRoaringSetRange(map[uint64]uint64{
				113: math.MaxUint64 - 13, // 1111..0010
				213: math.MaxUint64 - 13, // 1111..0010
				15:  math.MaxUint64 - 5,  // 1111..1010
				25:  math.MaxUint64 - 5,  // 1111..1010
				10:  math.MaxUint64,      // 1111..1111
				20:  math.MaxUint64,      // 1111..1111
			})
		}, logger)

		for _, tc := range testCases {
			t.Run(tc.operator.Name(), func(t *testing.T) {
				t.Run(fmt.Sprintf("value %d", tc.value), func(t *testing.T) {
					bm, err := reader.Read(context.Background(), tc.value, tc.operator)

					assert.NoError(t, err)
					require.NotNil(t, bm)
					assert.ElementsMatch(t, tc.expected, bm.ToArray())
				})
			})
		}
	})
}

func TestNoGapsCursor(t *testing.T) {
	t.Run("with empty CursorRoaringSetRange", func(t *testing.T) {
		c := &noGapsCursor{cursor: newFakeCursorRoaringSetRange(map[uint64]uint64{})}

		k, v, ok := c.first()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.Nil(t, v)

		for i := uint8(1); i < 65; i++ {
			k, v, ok = c.next()
			require.Equal(t, i, k)
			require.True(t, ok)
			assert.Nil(t, v)
		}

		k, v, ok = c.next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("with populated CursorRoaringSetRange", func(t *testing.T) {
		c := &noGapsCursor{cursor: newFakeCursorRoaringSetRange(map[uint64]uint64{
			113: 13, // 1101
			213: 13, // 1101
			15:  5,  // 0101
			25:  5,  // 0101
			10:  0,  // 0000
			20:  0,  // 0000
		})}

		k, v, ok := c.first()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.ToArray())

		expected := map[uint8][]uint64{
			1: {15, 25, 113, 213},
			3: {15, 25, 113, 213},
			4: {113, 213},
		}

		for i := uint8(1); i < 65; i++ {
			k, v, ok := c.next()
			require.Equal(t, i, k)
			require.True(t, ok)

			if expectedV, ok := expected[i]; ok {
				require.NotNil(t, v)
				assert.ElementsMatch(t, expectedV, v.ToArray())
			} else {
				assert.Nil(t, v)
			}
		}

		k, v, ok = c.next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})
}

func TestFakeCursorRoaringSetRange(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		c := newFakeCursorRoaringSetRange(map[uint64]uint64{})

		k, v, ok := c.First()
		assert.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("populated", func(t *testing.T) {
		c := newFakeCursorRoaringSetRange(map[uint64]uint64{
			113: 13, // 1101
			213: 13, // 1101
			15:  5,  // 0101
			25:  5,  // 0101
			10:  0,  // 0000
			20:  0,  // 0000
		})

		k, v, ok := c.First()
		assert.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(1), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(3), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(4), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})
}

type fakeCursorRoaringSetRange struct {
	bitmaps map[uint8]*sroar.Bitmap
	bits    []uint8
	pos     int
}

func newFakeCursorRoaringSetRange(docId2Val map[uint64]uint64) *fakeCursorRoaringSetRange {
	bitmaps := make(map[uint8]*sroar.Bitmap, 65)

	for docId, val := range docId2Val {
		if bitmaps[0] == nil {
			bitmaps[0] = sroar.NewBitmap()
		}
		bitmaps[0].Set(docId)

		for i := uint8(0); i < 64; i++ {
			if val&(1<<i) != 0 {
				if bitmaps[i+1] == nil {
					bitmaps[i+1] = sroar.NewBitmap()
				}
				bitmaps[i+1].Set(docId)
			}
		}
	}

	bits := make([]uint8, 0, len(bitmaps))
	for bit := range bitmaps {
		bits = append(bits, bit)
	}
	sort.Slice(bits, func(i, j int) bool { return bits[i] < bits[j] })

	return &fakeCursorRoaringSetRange{
		bitmaps: bitmaps,
		bits:    bits,
		pos:     0,
	}
}

func (c *fakeCursorRoaringSetRange) First() (uint8, *sroar.Bitmap, bool) {
	c.pos = 0
	return c.Next()
}

func (c *fakeCursorRoaringSetRange) Next() (uint8, *sroar.Bitmap, bool) {
	if c.pos >= len(c.bits) {
		return 0, nil, false
	}

	bit := c.bits[c.pos]
	c.pos++
	return bit, c.bitmaps[bit], true
}

func (c *fakeCursorRoaringSetRange) Close() {}
