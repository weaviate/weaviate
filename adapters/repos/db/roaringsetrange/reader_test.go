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

package roaringsetrange

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestReader(t *testing.T) {
	logger, _ := test.NewNullLogger()

	memSeg1 := NewMemtable(logger)
	memSeg1.Insert(6, []uint64{16, 26})    // 000110
	memSeg1.Insert(19, []uint64{119, 219}) // 010011
	memSeg1.Insert(25, []uint64{113, 213}) // overwriten
	memSeg1.Delete(8, []uint64{10, 20})

	memSeg2 := NewMemtable(logger)
	memSeg2.Insert(4, []uint64{14, 24})    // 000100
	memSeg2.Insert(17, []uint64{117, 217}) // 010001
	memSeg2.Insert(22, []uint64{15, 25})   // overwritten
	memSeg2.Delete(1, []uint64{16, 26})

	mem := NewMemtable(logger)
	mem.Insert(0, []uint64{10, 20})    // 000000
	mem.Insert(5, []uint64{15, 25})    // 000101
	mem.Insert(13, []uint64{113, 213}) // 001101
	mem.Delete(21, []uint64{121, 221})

	// 0 -> 10, 20
	// 4 -> 14, 24
	// 5 -> 15, 25
	// 13 -> 113, 213
	// 17 -> 117, 217
	// 19 -> 119, 229

	seg1Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(memSeg1)))
	seg2Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(memSeg2)))
	memReader := NewMemtableReader(mem)

	reader := NewCombinedReader([]InnerReader{seg1Reader, seg2Reader, memReader}, func() {}, 4, logger)

	type testCase struct {
		value    uint64
		operator filters.Operator
		expected []uint64
	}

	testCases := []testCase{
		{
			value:    0,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    0,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    0,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20},
		},
		{
			value:    0,
			operator: filters.OperatorLessThan,
			expected: []uint64{},
		},
		{
			value:    0,
			operator: filters.OperatorEqual,
			expected: []uint64{10, 20},
		},
		{
			value:    0,
			operator: filters.OperatorNotEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			value:    4,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    4,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    4,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			value:    4,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20},
		},
		{
			value:    4,
			operator: filters.OperatorEqual,
			expected: []uint64{14, 24},
		},
		{
			value:    4,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			value:    5,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    5,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			value:    5,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			value:    5,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			value:    5,
			operator: filters.OperatorEqual,
			expected: []uint64{15, 25},
		},
		{
			value:    5,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 113, 213, 117, 217, 119, 219},
		},

		{
			value:    13,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			value:    13,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			value:    13,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			value:    13,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			value:    13,
			operator: filters.OperatorEqual,
			expected: []uint64{113, 213},
		},
		{
			value:    13,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 117, 217, 119, 219},
		},

		{
			value:    17,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			value:    17,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{119, 219},
		},
		{
			value:    17,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			value:    17,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			value:    17,
			operator: filters.OperatorEqual,
			expected: []uint64{117, 217},
		},
		{
			value:    17,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 119, 219},
		},

		{
			value:    19,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{119, 219},
		},
		{
			value:    19,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{},
		},
		{
			value:    19,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			value:    19,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			value:    19,
			operator: filters.OperatorEqual,
			expected: []uint64{119, 219},
		},
		{
			value:    19,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
	}

	for _, tc := range testCases {
		t.Run("read", func(t *testing.T) {
			bm, err := reader.Read(context.Background(), tc.value, tc.operator)
			assert.NoError(t, err)
			assert.NotNil(t, bm)
			assert.ElementsMatch(t, bm.ToArray(), tc.expected)
		})
	}
}
