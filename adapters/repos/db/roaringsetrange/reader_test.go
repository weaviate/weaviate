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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestCombinedReader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	testCases := []struct {
		name     string
		value    uint64
		operator filters.Operator
		expected []uint64
	}{
		{
			name:     "greater than equal 0",
			value:    0,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 0",
			value:    0,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 0",
			value:    0,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20},
		},
		{
			name:     "less than 0",
			value:    0,
			operator: filters.OperatorLessThan,
			expected: []uint64{},
		},
		{
			name:     "equal 0",
			value:    0,
			operator: filters.OperatorEqual,
			expected: []uint64{10, 20},
		},
		{
			name:     "not equal 0",
			value:    0,
			operator: filters.OperatorNotEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 4",
			value:    4,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 4",
			value:    4,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 4",
			value:    4,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			name:     "less than 4",
			value:    4,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20},
		},
		{
			name:     "equal 4",
			value:    4,
			operator: filters.OperatorEqual,
			expected: []uint64{14, 24},
		},
		{
			name:     "not equal 4",
			value:    4,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 5",
			value:    5,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 5",
			value:    5,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 5",
			value:    5,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			name:     "less than 5",
			value:    5,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			name:     "equal 5",
			value:    5,
			operator: filters.OperatorEqual,
			expected: []uint64{15, 25},
		},
		{
			name:     "not equal 5",
			value:    5,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 13",
			value:    13,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 13",
			value:    13,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			name:     "less than equal 13",
			value:    13,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			name:     "less than 13",
			value:    13,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			name:     "equal 13",
			value:    13,
			operator: filters.OperatorEqual,
			expected: []uint64{113, 213},
		},
		{
			name:     "not equal 13",
			value:    13,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 17",
			value:    17,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			name:     "greater than 17",
			value:    17,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{119, 219},
		},
		{
			name:     "less than equal 17",
			value:    17,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			name:     "less than 17",
			value:    17,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			name:     "equal 17",
			value:    17,
			operator: filters.OperatorEqual,
			expected: []uint64{117, 217},
		},
		{
			name:     "not equal 17",
			value:    17,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 119, 219},
		},

		{
			name:     "greater than equal 19",
			value:    19,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{119, 219},
		},
		{
			name:     "greater than 19",
			value:    19,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{},
		},
		{
			name:     "less than equal 19",
			value:    19,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than 19",
			value:    19,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			name:     "equal 19",
			value:    19,
			operator: filters.OperatorEqual,
			expected: []uint64{119, 219},
		},
		{
			name:     "not equal 19",
			value:    19,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
	}

	t.Run("segments + memtable readers", func(t *testing.T) {
		seg1Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(mt1)))
		seg2Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(mt2)))
		mtReader := NewMemtableReader(mt3)

		reader := NewCombinedReader([]InnerReader{seg1Reader, seg2Reader, mtReader}, func() {}, 4, logger)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				assert.NoError(t, err)
				defer release()

				assert.NotNil(t, bm)
				assert.ElementsMatch(t, bm.ToArray(), tc.expected)
			})
		}
	})

	t.Run("segment-in-memory + memtable readers", func(t *testing.T) {
		s := NewSegmentInMemory()
		s.MergeMemtable(mt1)
		s.MergeMemtable(mt2)

		segInMemoReader, release := NewSegmentInMemoryReader(s, roaringset.NewBitmapBufPoolNoop())
		mtReader := NewMemtableReader(mt3)

		reader := NewCombinedReader([]InnerReader{segInMemoReader, mtReader}, release, 4, logger)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				assert.NoError(t, err)
				defer release()

				assert.NotNil(t, bm)
				assert.ElementsMatch(t, bm.ToArray(), tc.expected)
			})
		}
	})
}

func TestCombinedReaderInnerReaders(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("all but 1st inner readers' results are released", func(t *testing.T) {
		expected := []uint64{1, 3, 5, 6}

		innerReader1 := newFakeInnerReader(roaringset.NewBitmap(1, 2), nil, nil)
		innerReader2 := newFakeInnerReader(roaringset.NewBitmap(3, 4), roaringset.NewBitmap(2), nil)
		innerReader3 := newFakeInnerReader(roaringset.NewBitmap(5, 6), roaringset.NewBitmap(4), nil)
		reader := NewCombinedReader([]InnerReader{innerReader1, innerReader2, innerReader3}, func() {}, 4, logger)

		bm, release, err := reader.Read(context.Background(), 0, filters.OperatorGreaterThanEqual)
		require.NoError(t, err)

		assert.ElementsMatch(t, expected, bm.ToArray())
		assert.Equal(t, 1, innerReader1.InUseCounter())
		assert.Equal(t, 0, innerReader2.InUseCounter())
		assert.Equal(t, 0, innerReader3.InUseCounter())
		release()
		assert.Equal(t, 0, innerReader1.InUseCounter())
	})

	t.Run("all inner readers' results are released on error", func(t *testing.T) {
		innerReader1 := newFakeInnerReader(nil, nil, fmt.Errorf("error1"))
		innerReader2 := newFakeInnerReader(roaringset.NewBitmap(3, 4), roaringset.NewBitmap(2), nil)
		innerReader3 := newFakeInnerReader(nil, nil, fmt.Errorf("error3"))
		reader := NewCombinedReader([]InnerReader{innerReader1, innerReader2, innerReader3}, func() {}, 4, logger)

		bm, _, err := reader.Read(context.Background(), 0, filters.OperatorGreaterThanEqual)
		require.Error(t, err)

		assert.Nil(t, bm)
		assert.ErrorContains(t, err, "error1")
		assert.ErrorContains(t, err, "error3")
		assert.Equal(t, 0, innerReader1.InUseCounter())
		assert.Equal(t, 0, innerReader2.InUseCounter())
		assert.Equal(t, 0, innerReader3.InUseCounter())
	})
}

func createTestMemtables(logger logrus.FieldLogger) (*Memtable, *Memtable, *Memtable) {
	mt1 := NewMemtable(logger)
	mt1.Insert(6, []uint64{16, 26})    // deleted
	mt1.Insert(19, []uint64{119, 219}) // 010011
	mt1.Insert(25, []uint64{113, 213}) // overwriten
	mt1.Delete(8, []uint64{10, 20})

	mt2 := NewMemtable(logger)
	mt2.Insert(4, []uint64{14, 24})    // 000100
	mt2.Insert(17, []uint64{117, 217}) // 010001
	mt2.Insert(22, []uint64{15, 25})   // overwritten
	mt2.Delete(1, []uint64{16, 26})

	mt3 := NewMemtable(logger)
	mt3.Insert(0, []uint64{10, 20})    // 000000
	mt3.Insert(5, []uint64{15, 25})    // 000101
	mt3.Insert(13, []uint64{113, 213}) // 001101
	mt3.Delete(21, []uint64{121, 221})

	// 0 -> 10, 20
	// 4 -> 14, 24
	// 5 -> 15, 25
	// 13 -> 113, 213
	// 17 -> 117, 217
	// 19 -> 119, 219

	return mt1, mt2, mt3
}

type fakeInnerReader struct {
	inUseCounter int
	additions    *sroar.Bitmap
	deletions    *sroar.Bitmap
	err          error
}

func newFakeInnerReader(additions, deletions *sroar.Bitmap, err error) *fakeInnerReader {
	return &fakeInnerReader{
		inUseCounter: 0,
		additions:    additions,
		deletions:    deletions,
		err:          err,
	}
}

func (r *fakeInnerReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (layer roaringset.BitmapLayer, release func(), err error) {
	r.inUseCounter++
	return roaringset.BitmapLayer{Additions: r.additions, Deletions: r.deletions},
		func() { r.inUseCounter-- }, r.err
}

func (r *fakeInnerReader) InUseCounter() int {
	return r.inUseCounter
}
