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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestSegmentInMemory(t *testing.T) {
	t.Run("bitmaps are initialized and empty", func(t *testing.T) {
		s := NewSegmentInMemory()

		for i := range s.bitmaps {
			assert.NotNil(t, s.bitmaps[i])
			assert.True(t, s.bitmaps[i].IsEmpty())
		}
	})

	t.Run("size is sum of bitmap sizes", func(t *testing.T) {
		bmSize := sroar.NewBitmap().LenInBytes()

		s := NewSegmentInMemory()
		assert.Equal(t, bmSize*65, s.Size())
	})

	t.Run("merging", func(t *testing.T) {
		logger, _ := test.NewNullLogger()

		mt1, mt2, mt3 := createTestMemtables(logger)
		expectedElemsByBit := map[int][]uint64{
			0: {10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
			1: {119, 219, 117, 217, 15, 25, 113, 213},
			2: {119, 219},
			3: {14, 24, 15, 25, 113, 213},
			4: {113, 213},
			5: {119, 219, 117, 217},
		}

		t.Run("segments", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)
			cur3 := newFakeSegmentCursor(mt3)

			s := NewSegmentInMemory()
			s.MergeSegmentByCursor(cur1)
			s.MergeSegmentByCursor(cur2)
			s.MergeSegmentByCursor(cur3)

			assertElemsByBit(t, s, expectedElemsByBit)
		})

		t.Run("memtables", func(t *testing.T) {
			s := NewSegmentInMemory()
			s.MergeMemtable(mt1)
			s.MergeMemtable(mt2)
			s.MergeMemtable(mt3)

			assertElemsByBit(t, s, expectedElemsByBit)
		})

		t.Run("segments + memtable", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)

			s := NewSegmentInMemory()
			s.MergeSegmentByCursor(cur1)
			s.MergeSegmentByCursor(cur2)
			s.MergeMemtable(mt3)

			assertElemsByBit(t, s, expectedElemsByBit)
		})
	})
}

func TestSegmentInMemoryReader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	s := NewSegmentInMemory()
	s.MergeMemtable(mt1)
	s.MergeMemtable(mt2)
	s.MergeMemtable(mt3)

	bufPool := roaringset.NewBitmapBufPoolNoop()
	reader, release := NewSegmentInMemoryReader(s, bufPool)
	defer release()

	t.Run("read valid operators", func(t *testing.T) {
		testCases := []struct {
			name     string
			value    uint64
			operator filters.Operator
			expected []uint64
		}{
			{
				name:     "equal 0",
				value:    0,
				operator: filters.OperatorEqual,
				expected: []uint64{10, 20},
			},
			{
				name:     "equal 13",
				value:    13,
				operator: filters.OperatorEqual,
				expected: []uint64{113, 213},
			},
			{
				name:     "equal 8",
				value:    8,
				operator: filters.OperatorEqual,
				expected: []uint64{},
			},
			{
				name:     "not equal 0",
				value:    0,
				operator: filters.OperatorNotEqual,
				expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
			},
			{
				name:     "not equal 13",
				value:    13,
				operator: filters.OperatorNotEqual,
				expected: []uint64{10, 20, 14, 24, 15, 25, 117, 217, 119, 219},
			},
			{
				name:     "not equal 8",
				value:    8,
				operator: filters.OperatorNotEqual,
				expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
			},
			{
				name:     "greater than equal 0",
				value:    0,
				operator: filters.OperatorGreaterThanEqual,
				expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
			},
			{
				name:     "greater than equal 13",
				value:    13,
				operator: filters.OperatorGreaterThanEqual,
				expected: []uint64{113, 213, 117, 217, 119, 219},
			},
			{
				name:     "greater than 0",
				value:    0,
				operator: filters.OperatorGreaterThan,
				expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
			},
			{
				name:     "greater than 13",
				value:    13,
				operator: filters.OperatorGreaterThan,
				expected: []uint64{117, 217, 119, 219},
			},
			{
				name:     "less than equal 0",
				value:    0,
				operator: filters.OperatorLessThanEqual,
				expected: []uint64{10, 20},
			},
			{
				name:     "less than equal 13",
				value:    13,
				operator: filters.OperatorLessThanEqual,
				expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
			},
			{
				name:     "less than 0",
				value:    0,
				operator: filters.OperatorLessThan,
				expected: []uint64{},
			},
			{
				name:     "less than 13",
				value:    13,
				operator: filters.OperatorLessThan,
				expected: []uint64{10, 20, 14, 24, 15, 25},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				layer, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				require.NoError(t, err)
				defer release()

				assert.ElementsMatch(t, tc.expected, layer.Additions.ToArray())
				assert.Nil(t, layer.Deletions)
			})
		}
	})

	t.Run("read invalid opeators", func(t *testing.T) {
		testCases := []struct {
			name     string
			operator filters.Operator
		}{
			{
				name:     "like",
				operator: filters.OperatorLike,
			},
			{
				name:     "is null",
				operator: filters.OperatorIsNull,
			},
			{
				name:     "and",
				operator: filters.OperatorAnd,
			},
			{
				name:     "or",
				operator: filters.OperatorOr,
			},
			{
				name:     "within geo range",
				operator: filters.OperatorWithinGeoRange,
			},
			{
				name:     "contains any",
				operator: filters.ContainsAny,
			},
			{
				name:     "contains all",
				operator: filters.ContainsAll,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				layer, _, err := reader.Read(context.Background(), 0, tc.operator)
				assert.ErrorContains(t, err, "not supported for segment-in-memory")
				assert.Nil(t, layer.Additions)
				assert.Nil(t, layer.Deletions)
			})
		}
	})

	t.Run("read expired context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		layer, _, err := reader.Read(ctx, 0, filters.OperatorGreaterThanEqual)
		assert.ErrorContains(t, err, ctx.Err().Error())
		assert.Nil(t, layer.Additions)
		assert.Nil(t, layer.Deletions)
	})
}

func TestSegmentInMemoryReaderBufPool(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	s := NewSegmentInMemory()
	s.MergeMemtable(mt1)
	s.MergeMemtable(mt2)
	s.MergeMemtable(mt3)

	bufPool := newBitmapBufPoolWithCounter()
	reader, release := NewSegmentInMemoryReader(s, bufPool)
	defer release()

	t.Run("all but one bufs are returned to the pull on read", func(t *testing.T) {
		testCases := []struct {
			name     string
			value    uint64
			operator filters.Operator
		}{
			{
				name:     "equal 0",
				value:    0,
				operator: filters.OperatorEqual,
			},
			{
				name:     "equal 13",
				value:    13,
				operator: filters.OperatorEqual,
			},
			{
				name:     "equal 8",
				value:    8,
				operator: filters.OperatorEqual,
			},
			{
				name:     "not equal 0",
				value:    0,
				operator: filters.OperatorNotEqual,
			},
			{
				name:     "not equal 13",
				value:    13,
				operator: filters.OperatorNotEqual,
			},
			{
				name:     "not equal 8",
				value:    8,
				operator: filters.OperatorNotEqual,
			},
			{
				name:     "greater than equal 0",
				value:    0,
				operator: filters.OperatorGreaterThanEqual,
			},
			{
				name:     "greater than equal 13",
				value:    13,
				operator: filters.OperatorGreaterThanEqual,
			},
			{
				name:     "greater than 0",
				value:    0,
				operator: filters.OperatorGreaterThan,
			},
			{
				name:     "greater than 13",
				value:    13,
				operator: filters.OperatorGreaterThan,
			},
			{
				name:     "less than equal 0",
				value:    0,
				operator: filters.OperatorLessThanEqual,
			},
			{
				name:     "less than equal 13",
				value:    13,
				operator: filters.OperatorLessThanEqual,
			},
			{
				name:     "less than 0",
				value:    0,
				operator: filters.OperatorLessThan,
			},
			{
				name:     "less than 13",
				value:    13,
				operator: filters.OperatorLessThan,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				require.NoError(t, err)

				assert.GreaterOrEqual(t, 1, bufPool.InUseCounter())
				release()
				assert.Equal(t, 0, bufPool.InUseCounter())
			})
		}
	})
}

func assertElemsByBit(t *testing.T, s *SegmentInMemory, expectedElemsByBit map[int][]uint64) {
	for bit := 0; bit < 65; bit++ {
		if elems, ok := expectedElemsByBit[bit]; ok {
			assert.ElementsMatch(t, elems, s.bitmaps[bit].ToArray())
		} else {
			assert.True(t, s.bitmaps[bit].IsEmpty())
		}
	}
}

type bitmapBufPoolWithCounter struct {
	inUseCounter int
}

func newBitmapBufPoolWithCounter() *bitmapBufPoolWithCounter {
	return &bitmapBufPoolWithCounter{inUseCounter: 0}
}

func (p *bitmapBufPoolWithCounter) Get(minCap int) (buf []byte, put func()) {
	p.inUseCounter++
	return make([]byte, 0, minCap), func() { p.inUseCounter-- }
}

func (p *bitmapBufPoolWithCounter) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := p.Get(bm.LenInBytes())
	cloned = bm.CloneToBuf(buf)
	return
}

func (p *bitmapBufPoolWithCounter) InUseCounter() int {
	return p.inUseCounter
}
