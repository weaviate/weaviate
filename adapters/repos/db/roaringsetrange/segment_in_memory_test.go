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

package roaringsetrange

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestSegmentInMemory(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("bitmaps are initialized and empty", func(t *testing.T) {
		s := NewSegmentInMemory(logger)

		for i := range s.bitmaps {
			assert.NotNil(t, s.bitmaps[i])
			assert.True(t, s.bitmaps[i].IsEmpty())
		}
	})

	t.Run("size is sum of bitmap sizes", func(t *testing.T) {
		bmSize := sroar.NewBitmap().LenInBytes()

		s := NewSegmentInMemory(logger)
		assert.Equal(t, bmSize*65, s.Size())
	})

	t.Run("size counts headroom", func(t *testing.T) {
		s := newSegmentInMemoryWithDeadSpace(t, 4, 1024)
		s.Compact()

		capacity, length := 0, 0
		for i := range s.bitmaps {
			capacity += s.bitmaps[i].CapInBytes()
			length += s.bitmaps[i].LenInBytes()
		}
		require.Greater(t, capacity, length, "headroom must be spare capacity or the case tests nothing")
		assert.Equal(t, capacity, s.Size())
	})

	t.Run("merging", func(t *testing.T) {
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

			seg := NewSegmentInMemory(logger)
			seg.MergeSegmentByCursor(cur1)
			seg.MergeSegmentByCursor(cur2)
			seg.MergeSegmentByCursor(cur3)

			assertElemsByBit(t, seg, expectedElemsByBit)
		})

		t.Run("memtables", func(t *testing.T) {
			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(mt1)
			seg.MergeMemtableEventually(mt2)
			seg.MergeMemtableEventually(mt3)

			waitUntilMemtablesMerged(t, seg)
			assertElemsByBit(t, seg, expectedElemsByBit)
		})

		t.Run("segments + memtable", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)

			seg := NewSegmentInMemory(logger)
			seg.MergeSegmentByCursor(cur1)
			seg.MergeSegmentByCursor(cur2)
			seg.MergeMemtableEventually(mt3)

			waitUntilMemtablesMerged(t, seg)
			assertElemsByBit(t, seg, expectedElemsByBit)
		})
	})

	t.Run("simultaneous read & write", func(t *testing.T) {
		mt1, mt2, mt3 := createTestMemtables(logger)
		bufPool := roaringset.NewBitmapBufPoolNoop()

		createReader := func(s *SegmentInMemory) *CombinedReader {
			readers, release := s.Readers(bufPool)
			return NewCombinedReader(readers, release, 1, logger)
		}

		assertResult := func(t *testing.T, creader *CombinedReader, value uint64, operator filters.Operator, expected []uint64) {
			t.Helper()

			bm, release, err := creader.Read(context.Background(), value, operator)
			require.NoError(t, err)

			defer release()
			assert.ElementsMatch(t, expected, bm.ToArray())
		}

		assertGreaterThanEqual13 := func(t *testing.T, creader *CombinedReader) {
			assertResult(t, creader, 13, filters.OperatorGreaterThanEqual, []uint64{113, 213, 117, 217, 119, 219})
		}

		t.Run("multiple readers used", func(t *testing.T) {
			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(mt1)
			seg.MergeMemtableEventually(mt2)
			seg.MergeMemtableEventually(mt3)

			t.Run("same results before merge", func(t *testing.T) {
				creader1 := createReader(seg)
				creader2 := createReader(seg)
				creader3 := createReader(seg)
				defer creader1.Close()
				defer creader2.Close()
				defer creader3.Close()

				assertGreaterThanEqual13(t, creader1)
				assertGreaterThanEqual13(t, creader2)
				assertGreaterThanEqual13(t, creader3)
			})

			waitUntilMemtablesMerged(t, seg)

			t.Run("same results after merge", func(t *testing.T) {
				creader1 := createReader(seg)
				creader2 := createReader(seg)
				creader3 := createReader(seg)
				defer creader1.Close()
				defer creader2.Close()
				defer creader3.Close()

				assertGreaterThanEqual13(t, creader1)
				assertGreaterThanEqual13(t, creader2)
				assertGreaterThanEqual13(t, creader3)
			})
		})

		t.Run("write when readers in use", func(t *testing.T) {
			assertGreaterThanEqual13_0 := func(t *testing.T, creader *CombinedReader) {
				assertResult(t, creader, 13, filters.OperatorGreaterThanEqual, []uint64{})
			}
			assertGreaterThanEqual13_1 := func(t *testing.T, creader *CombinedReader) {
				assertResult(t, creader, 13, filters.OperatorGreaterThanEqual, []uint64{119, 219, 113, 213})
			}
			assertGreaterThanEqual13_2 := func(t *testing.T, creader *CombinedReader) {
				assertResult(t, creader, 13, filters.OperatorGreaterThanEqual, []uint64{117, 217, 119, 219, 113, 213, 15, 25})
			}

			seg := NewSegmentInMemory(logger)
			creader0 := createReader(seg)
			seg.MergeMemtableEventually(mt1)
			creader1 := createReader(seg)
			seg.MergeMemtableEventually(mt2)
			creader2 := createReader(seg)
			seg.MergeMemtableEventually(mt3)
			creader3 := createReader(seg)

			// before merge
			assertGreaterThanEqual13_0(t, creader0)
			assertGreaterThanEqual13_1(t, creader1)
			assertGreaterThanEqual13_2(t, creader2)
			assertGreaterThanEqual13(t, creader3)

			// close readers to allow merge
			creader0.Close()
			creader1.Close()
			creader2.Close()
			creader3.Close()

			waitUntilMemtablesMerged(t, seg)

			// after merge
			creader := createReader(seg)
			assertGreaterThanEqual13(t, creader)
		})
	})
}

// outOfRangeSecondKeyCursor yields a valid key then an out-of-range key,
// reproducing a corrupt/truncated segment without a byte-corrupt fixture.
type outOfRangeSecondKeyCursor struct{ step int }

func (c *outOfRangeSecondKeyCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.step = 0
	return c.Next()
}

func (c *outOfRangeSecondKeyCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	defer func() { c.step++ }()
	switch c.step {
	case 0:
		return 0, roaringset.BitmapLayer{Additions: sroar.NewBitmap(), Deletions: sroar.NewBitmap()}, true
	case 1:
		return 200, roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, true
	default:
		return 0, roaringset.BitmapLayer{}, false
	}
}

// TestSegmentInMemoryMergeSegmentByCursor_RejectsOutOfRangeKey pins
// weaviate/weaviate#12215: an out-of-range key must return an error, not panic.
func TestSegmentInMemoryMergeSegmentByCursor_RejectsOutOfRangeKey(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSegmentInMemory(logger)

	err := s.MergeSegmentByCursor(&outOfRangeSecondKeyCursor{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid key")
}

func TestSegmentInMemoryReader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := NewSegmentInMemory(logger)
	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)

	waitUntilMemtablesMerged(t, seg)

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()

	require.Len(t, readers, 1)
	reader := readers[0]

	t.Run("read valid operators", func(t *testing.T) {
		type testCase struct {
			value    uint64
			operator filters.Operator
			expected []uint64
		}

		// the segment stores values 0, 4, 5, 13, 17 and 19
		testCases := []testCase{
			{value: 0, operator: filters.OperatorEqual, expected: []uint64{10, 20}},
			{value: 0, operator: filters.OperatorNotEqual, expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 0, operator: filters.OperatorLessThan, expected: []uint64{}},
			{value: 0, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20}},
			{value: 0, operator: filters.OperatorGreaterThan, expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 0, operator: filters.OperatorGreaterThanEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},

			{value: 1, operator: filters.OperatorEqual, expected: []uint64{}},
			{value: 1, operator: filters.OperatorNotEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 1, operator: filters.OperatorLessThan, expected: []uint64{10, 20}},
			{value: 1, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20}},
			{value: 1, operator: filters.OperatorGreaterThan, expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 1, operator: filters.OperatorGreaterThanEqual, expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},

			{value: 4, operator: filters.OperatorEqual, expected: []uint64{14, 24}},
			{value: 4, operator: filters.OperatorNotEqual, expected: []uint64{10, 20, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 4, operator: filters.OperatorLessThan, expected: []uint64{10, 20}},
			{value: 4, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20, 14, 24}},
			{value: 4, operator: filters.OperatorGreaterThan, expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 4, operator: filters.OperatorGreaterThanEqual, expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},

			{value: 8, operator: filters.OperatorEqual, expected: []uint64{}},
			{value: 8, operator: filters.OperatorNotEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: 8, operator: filters.OperatorLessThan, expected: []uint64{10, 20, 14, 24, 15, 25}},
			{value: 8, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20, 14, 24, 15, 25}},
			{value: 8, operator: filters.OperatorGreaterThan, expected: []uint64{113, 213, 117, 217, 119, 219}},
			{value: 8, operator: filters.OperatorGreaterThanEqual, expected: []uint64{113, 213, 117, 217, 119, 219}},

			{value: 13, operator: filters.OperatorEqual, expected: []uint64{113, 213}},
			{value: 13, operator: filters.OperatorNotEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 117, 217, 119, 219}},
			{value: 13, operator: filters.OperatorLessThan, expected: []uint64{10, 20, 14, 24, 15, 25}},
			{value: 13, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213}},
			{value: 13, operator: filters.OperatorGreaterThan, expected: []uint64{117, 217, 119, 219}},
			{value: 13, operator: filters.OperatorGreaterThanEqual, expected: []uint64{113, 213, 117, 217, 119, 219}},

			{value: math.MaxUint64, operator: filters.OperatorEqual, expected: []uint64{}},
			{value: math.MaxUint64, operator: filters.OperatorNotEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: math.MaxUint64, operator: filters.OperatorLessThan, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: math.MaxUint64, operator: filters.OperatorLessThanEqual, expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219}},
			{value: math.MaxUint64, operator: filters.OperatorGreaterThan, expected: []uint64{}},
			{value: math.MaxUint64, operator: filters.OperatorGreaterThanEqual, expected: []uint64{}},
		}

		memMax := NewMemtable(logger)
		memMax.Insert(0, []uint64{1})
		memMax.Insert(math.MaxUint64-1, []uint64{2})
		memMax.Insert(math.MaxUint64, []uint64{3})

		maxCases := []testCase{
			{value: math.MaxUint64 - 1, operator: filters.OperatorEqual, expected: []uint64{2}},
			{value: math.MaxUint64 - 1, operator: filters.OperatorNotEqual, expected: []uint64{1, 3}},
			{value: math.MaxUint64 - 1, operator: filters.OperatorLessThan, expected: []uint64{1}},
			{value: math.MaxUint64 - 1, operator: filters.OperatorLessThanEqual, expected: []uint64{1, 2}},
			{value: math.MaxUint64 - 1, operator: filters.OperatorGreaterThan, expected: []uint64{3}},
			{value: math.MaxUint64 - 1, operator: filters.OperatorGreaterThanEqual, expected: []uint64{2, 3}},
			{value: math.MaxUint64, operator: filters.OperatorEqual, expected: []uint64{3}},
			{value: math.MaxUint64, operator: filters.OperatorNotEqual, expected: []uint64{1, 2}},
			{value: math.MaxUint64, operator: filters.OperatorLessThan, expected: []uint64{1, 2}},
			{value: math.MaxUint64, operator: filters.OperatorLessThanEqual, expected: []uint64{1, 2, 3}},
			{value: math.MaxUint64, operator: filters.OperatorGreaterThan, expected: []uint64{}},
			{value: math.MaxUint64, operator: filters.OperatorGreaterThanEqual, expected: []uint64{3}},
		}

		// ints encode 0 as 1<<63, so a value's lowest set bit can sit above bit 31
		memHigh := NewMemtable(logger)
		memHigh.Insert(0, []uint64{1})
		memHigh.Insert(1<<62, []uint64{2})
		memHigh.Insert(1<<63, []uint64{3})

		highCases := []testCase{
			{value: 1 << 63, operator: filters.OperatorEqual, expected: []uint64{3}},
			{value: 1 << 63, operator: filters.OperatorNotEqual, expected: []uint64{1, 2}},
			{value: 1 << 63, operator: filters.OperatorLessThan, expected: []uint64{1, 2}},
			{value: 1 << 63, operator: filters.OperatorGreaterThanEqual, expected: []uint64{3}},
			{value: 1 << 62, operator: filters.OperatorGreaterThanEqual, expected: []uint64{2, 3}},
		}

		var emptyCases []testCase
		for _, op := range segmentReaderOperators {
			for _, value := range []uint64{0, 5, math.MaxUint64} {
				emptyCases = append(emptyCases, testCase{value: value, operator: op})
			}
		}

		segments := []struct {
			name  string
			seg   *SegmentInMemory
			cases []testCase
		}{
			{name: "stored small values", seg: seg, cases: testCases},
			{name: "stored max values", seg: newSegmentInMemoryFrom(t, memMax), cases: maxCases},
			{name: "stored values with high lowest bits", seg: newSegmentInMemoryFrom(t, memHigh), cases: highCases},
			{name: "empty", seg: NewSegmentInMemory(logger), cases: emptyCases},
		}

		// the ranged pool hands out spare capacity and dirty buffers, the noop pool neither
		pools := map[string]roaringset.BitmapBufPool{
			"noop":   roaringset.NewBitmapBufPoolNoop(),
			"ranged": newRangedBufPool(t),
		}

		for _, s := range segments {
			for poolName, pool := range pools {
				readers, releaseReaders := s.seg.Readers(pool)
				for _, tc := range s.cases {
					t.Run(fmt.Sprintf("%s, %s pool, %s %d", s.name, poolName, tc.operator.Name(), tc.value), func(t *testing.T) {
						layer, release, err := readers[0].Read(context.Background(), tc.value, tc.operator)
						require.NoError(t, err)
						defer release()

						assert.ElementsMatch(t, tc.expected, layer.Additions.ToArray())
						assert.Nil(t, layer.Deletions)
						// CombinedReader merges into a read's result in place, so zeroing it
						// fails every later case if a read returned a segment bitmap
						layer.Additions.AndConc(sroar.NewBitmap(), 1)
					})
				}
				releaseReaders()
			}
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
			{
				name:     "contains none",
				operator: filters.ContainsNone,
			},
			{
				name:     "not",
				operator: filters.OperatorNot,
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

	seg := NewSegmentInMemory(logger)
	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)

	waitUntilMemtablesMerged(t, seg)

	// copies is how many pooled bitmaps a read builds, held how many its result keeps
	testCases := []struct {
		value    uint64
		operator filters.Operator
		copies   int64
		held     int64
	}{
		{value: 0, operator: filters.OperatorEqual, copies: 1, held: 1},
		{value: 13, operator: filters.OperatorEqual, copies: 1, held: 1},
		{value: 8, operator: filters.OperatorEqual, copies: 1, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorEqual, copies: 1, held: 1},
		{value: 0, operator: filters.OperatorNotEqual, copies: 2, held: 1},
		{value: 13, operator: filters.OperatorNotEqual, copies: 2, held: 1},
		{value: 8, operator: filters.OperatorNotEqual, copies: 2, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorNotEqual, copies: 2, held: 1},
		{value: 0, operator: filters.OperatorGreaterThanEqual, copies: 1, held: 1},
		{value: 13, operator: filters.OperatorGreaterThanEqual, copies: 1, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorGreaterThanEqual, copies: 1, held: 1},
		{value: 0, operator: filters.OperatorGreaterThan, copies: 2, held: 1},
		{value: 13, operator: filters.OperatorGreaterThan, copies: 1, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorGreaterThan, copies: 0, held: 0},
		{value: 0, operator: filters.OperatorLessThanEqual, copies: 1, held: 1},
		{value: 13, operator: filters.OperatorLessThanEqual, copies: 2, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorLessThanEqual, copies: 1, held: 1},
		{value: 0, operator: filters.OperatorLessThan, copies: 0, held: 0},
		{value: 13, operator: filters.OperatorLessThan, copies: 2, held: 1},
		{value: math.MaxUint64, operator: filters.OperatorLessThan, copies: 2, held: 1},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s %d", tc.operator.Name(), tc.value), func(t *testing.T) {
			bufPool := newHeadroomPool()
			readers, releaseReaders := seg.Readers(bufPool)
			defer releaseReaders()
			require.Len(t, readers, 1)

			layer, release, err := readers[0].Read(context.Background(), tc.value, tc.operator)
			require.NoError(t, err)

			assert.Len(t, bufPool.headroom, int(tc.copies))
			for bm, headroom := range bufPool.headroom {
				assert.Equal(t, bm == layer.Additions, headroom, "only the result may have headroom")
			}
			assert.Equal(t, tc.held, bufPool.Outstanding(), "a scratch bitmap was not released")
			release()
			assert.Zero(t, bufPool.Outstanding())
			assert.Equal(t, tc.copies, bufPool.PeakOutstanding())
		})
	}
}

// Pins a read's result taking memtable rows at new keys and in its containers'
// tails without moving its keys node or growing its buffer.
func TestSegmentInMemoryReaderResultHeadroom(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const docsPerKey = 100

	mem := NewMemtable(logger)
	for key := uint64(0); key < 4; key++ {
		for i := uint64(0); i < docsPerKey; i++ {
			value := i % 20
			if value == 19 {
				value = math.MaxUint64
			}
			mem.Insert(value, []uint64{key<<16 + i})
		}
	}
	seg := newSegmentInMemoryFrom(t, mem)

	// every operator below matches one of these values
	values := []uint64{0, 13, math.MaxUint64}
	active := NewMemtable(logger)
	for i, value := range values {
		for _, docID := range []uint64{20 << 16, 30 << 16, docsPerKey, 3<<16 + docsPerKey} {
			active.Insert(value, []uint64{docID + uint64(i)})
		}
	}

	// covers each site that builds a returned result, and each route into one
	testCases := []struct {
		value    uint64
		operator filters.Operator
	}{
		{value: 13, operator: filters.OperatorEqual},
		{value: math.MaxUint64, operator: filters.OperatorEqual},
		{value: 13, operator: filters.OperatorNotEqual},
		{value: math.MaxUint64, operator: filters.OperatorNotEqual},
		{value: 13, operator: filters.OperatorLessThan},
		{value: 13, operator: filters.OperatorLessThanEqual},
		{value: 0, operator: filters.OperatorLessThanEqual},
		{value: math.MaxUint64, operator: filters.OperatorLessThanEqual},
		{value: 13, operator: filters.OperatorGreaterThan},
		{value: 0, operator: filters.OperatorGreaterThan},
		{value: 13, operator: filters.OperatorGreaterThanEqual},
		{value: 0, operator: filters.OperatorGreaterThanEqual},
	}

	bufPool := newRangedBufPool(t)
	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s %d", tc.operator.Name(), tc.value), func(t *testing.T) {
			readers, releaseReaders := seg.Readers(bufPool)
			defer releaseReaders()
			require.Len(t, readers, 1)

			alone, releaseAlone, err := readers[0].Read(ctx, tc.value, tc.operator)
			require.NoError(t, err)
			defer releaseAlone()

			memLayer, _, err := NewMemtableReader(active).Read(ctx, tc.value, tc.operator)
			require.NoError(t, err)
			require.False(t, memLayer.Additions.IsEmpty(), "memtable must add rows or the case tests nothing")

			combined := NewCombinedReader([]InnerReader{readers[0], NewMemtableReader(active)}, noopRelease, 1, logger)
			bm, release, err := combined.Read(ctx, tc.value, tc.operator)
			require.NoError(t, err)
			defer release()

			assert.Subset(t, bm.ToArray(), memLayer.Additions.ToArray())
			assert.Equal(t, alone.Additions.CapInBytes(), bm.CapInBytes(), "result buffer grew")
			assert.Equal(t, keysNodeSize(alone.Additions), keysNodeSize(bm), "result keys node moved")
		})
	}
}

// Pins weaviate/weaviate#12199: folding segments out of order (newest before
// oldest) lets a stale value silently win, even though the docID's membership
// still checks out.
func TestSegmentInMemoryFoldOrderValueIntegrity(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bufPool := roaringset.NewBitmapBufPoolNoop()

	const (
		docX      = uint64(500) // value changes across the two segments
		docStable = uint64(99)  // never changes; membership backstop
		valOld    = uint64(3)
		valNew    = uint64(5)
		valStable = uint64(42)
	)

	olderSegment := func() SegmentCursor {
		mt := NewMemtable(logger)
		mt.Insert(valOld, []uint64{docX})
		mt.Insert(valStable, []uint64{docStable})
		return newFakeSegmentCursor(mt)
	}
	newerSegment := func() SegmentCursor {
		mt := NewMemtable(logger)
		mt.Insert(valNew, []uint64{docX})
		return newFakeSegmentCursor(mt)
	}

	equalDocIDs := func(t *testing.T, seg *SegmentInMemory, value uint64) []uint64 {
		t.Helper()
		readers, release := seg.Readers(bufPool)
		defer release()
		require.Len(t, readers, 1)
		layer, relRead, err := readers[0].Read(context.Background(), value, filters.OperatorEqual)
		require.NoError(t, err)
		defer relRead()
		return layer.Additions.ToArray()
	}

	t.Run("correct oldest->newest fold: newest value wins", func(t *testing.T) {
		seg := NewSegmentInMemory(logger)
		require.NoError(t, seg.MergeSegmentByCursor(olderSegment()))
		require.NoError(t, seg.MergeSegmentByCursor(newerSegment()))

		assert.Equal(t, []uint64{docX}, equalDocIDs(t, seg, valNew))
		assert.NotContains(t, equalDocIDs(t, seg, valOld), docX)
		// The untouched docID keeps its value (membership + value backstop).
		assert.Equal(t, []uint64{docStable}, equalDocIDs(t, seg, valStable))
	})

	t.Run("trap: incremental older-onto-newer fold corrupts the value (old wins)", func(t *testing.T) {
		seg := NewSegmentInMemory(logger)
		require.NoError(t, seg.MergeSegmentByCursor(newerSegment()))
		require.NoError(t, seg.MergeSegmentByCursor(olderSegment()))

		assert.Equal(t, []uint64{docX}, equalDocIDs(t, seg, valOld))
		assert.NotContains(t, equalDocIDs(t, seg, valNew), docX)
	})
}

func TestSegmentInMemoryCompact(t *testing.T) {
	// dense enough that layer 0 holds bitmap containers
	const keys, docsPerKey = 4, 8192

	t.Run("dead space reclaimed", func(t *testing.T) {
		seg := newSegmentInMemoryWithDeadSpace(t, keys, docsPerKey)
		layer0Before := seg.bitmaps[0].LenInBytes()
		sizeBefore := 0
		for i := range seg.bitmaps {
			sizeBefore += seg.bitmaps[i].LenInBytes()
		}

		seg.Compact()

		assert.Less(t, seg.bitmaps[0].CapInBytes(), layer0Before, "odd containers of layer 0 must turn into arrays")
		assert.Less(t, seg.Size(), sizeBefore)
	})
}

// Pins a compacted layer taking a flush's rows at new keys and in its
// containers' tails without moving its keys node or growing its buffer.
func TestSegmentInMemoryCompactAbsorbsMerge(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const keys, docsPerKey = 64, 40

	seg := newSegmentInMemoryWithDeadSpace(t, keys, docsPerKey)
	seg.Compact()

	mem := NewMemtable(logger)
	for i, value := range []uint64{0, 13, math.MaxUint64} {
		for _, docID := range []uint64{keys << 16, (keys + 8) << 16, docsPerKey, 2<<16 + docsPerKey} {
			mem.Insert(value, []uint64{docID + uint64(i)})
		}
	}

	var capBefore, keysBefore, cardBefore [65]int
	for i, bm := range seg.bitmaps {
		capBefore[i], keysBefore[i], cardBefore[i] = bm.CapInBytes(), int(keysNodeSize(bm)), bm.GetCardinality()
	}

	seg.MergeMemtableEventually(mem)
	waitUntilMemtablesMerged(t, seg)

	require.Greater(t, seg.bitmaps[64].GetCardinality(), cardBefore[64], "top layer must take rows or the case tests nothing")
	for i, bm := range seg.bitmaps {
		if bm.GetCardinality() == cardBefore[i] {
			continue
		}
		assert.Equal(t, capBefore[i], bm.CapInBytes(), "layer %d grew", i)
		assert.Equal(t, keysBefore[i], int(keysNodeSize(bm)), "layer %d keys node moved", i)
	}
}

// Pins a compacted layer growing by sroar's plain doubling, not by the spare ratio again.
func TestSegmentInMemoryCompactPadsOnlyOnce(t *testing.T) {
	logger, _ := test.NewNullLogger()

	testCases := []struct {
		name string
		seg  *SegmentInMemory
	}{
		{name: "populated", seg: newSegmentInMemoryWithDeadSpace(t, 4, 40)},
		{name: "empty", seg: NewSegmentInMemory(logger)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.seg.Compact()
			want := tc.seg.bitmaps[0].Clone()

			// one container per merge, so a growth is a single step
			for key := uint64(100); ; key++ {
				require.Less(t, key, uint64(1000), "layer 0 never grew")
				capBefore := tc.seg.bitmaps[0].CapInBytes()

				mem := NewMemtable(logger)
				mem.Insert(0, []uint64{key << 16})
				mergeMemtableNow(tc.seg, mem)
				want.Set(key << 16)

				if capAfter := tc.seg.bitmaps[0].CapInBytes(); capAfter != capBefore {
					assert.Equal(t, 2*capBefore, capAfter)
					break
				}
			}
			assert.Equal(t, want.ToArray(), tc.seg.bitmaps[0].ToArray())
		})
	}
}

// BenchmarkSegmentInMemoryCompactHeadroom merges a run of flushes into layers of
// 1 to 13 MiB compacted with each spare ratio. It reports the bytes growths copied
// under the write lock, the flushes merged before the first one, and the idle capacity.
func BenchmarkSegmentInMemoryCompactHeadroom(b *testing.B) {
	logger, _ := test.NewNullLogger()
	const flushes = 24

	for _, keys := range []uint64{128, 512, 2048} {
		base := newHeadroomBenchLayers(keys)
		for _, flush := range []struct {
			name             string
			appends, updates uint64
		}{
			{"appends_64k", 1 << 16, 0},
			{"appends_512k", 1 << 19, 0},
			{"updates_64k", 1 << 16, 1 << 12},
			{"updates_512k", 1 << 19, 1 << 15},
		} {
			memtables := newHeadroomBenchFlushes(logger, keys, flushes, flush.appends, flush.updates)
			for _, spareRatio := range []float64{0, 0.02, 0.05, 0.10, 0.25} {
				name := fmt.Sprintf("layer_%.0fMiB/%s/spare_%.2f", float64(base[0].LenInBytes())/(1<<20), flush.name, spareRatio)
				b.Run(name, func(b *testing.B) {
					copied, maxCopy, copies, flushesBeforeCopy, runs := 0, 0, 0, 0, 0
					var seg *SegmentInMemory
					for b.Loop() {
						b.StopTimer()
						seg = NewSegmentInMemory(logger)
						for i, bm := range base {
							seg.bitmaps[i] = bm.CompactedTo(nil, sroar.BuildOpts{InitialSpareRatio: spareRatio})
						}
						b.StartTimer()

						copiedInRun := false
						for _, mem := range memtables {
							var capBefore, lenBefore [65]int
							for i, bm := range seg.bitmaps {
								capBefore[i], lenBefore[i] = bm.CapInBytes(), bm.LenInBytes()
							}
							mergeMemtableNow(seg, mem)
							for i, bm := range seg.bitmaps {
								if bm.CapInBytes() != capBefore[i] {
									copied += lenBefore[i]
									maxCopy = max(maxCopy, lenBefore[i])
									copies++
									copiedInRun = true
								}
							}
							if !copiedInRun {
								flushesBeforeCopy++
							}
						}
						runs++
					}

					length := 0
					for _, bm := range seg.bitmaps {
						length += bm.LenInBytes()
					}
					b.ReportMetric(float64(copied)/float64(runs)/(1<<20), "copied_MiB/op")
					b.ReportMetric(float64(maxCopy)/(1<<20), "max_copy_MiB")
					b.ReportMetric(float64(copies)/float64(runs), "copies/op")
					b.ReportMetric(float64(flushesBeforeCopy)/float64(runs), "flushes_before_copy")
					b.ReportMetric(float64(seg.Size()-length)/(1<<20), "idle_MiB")
					b.ReportMetric(float64(seg.Size())/float64(length), "cap/len")
				})
			}
		}
	}
}

// newHeadroomBenchLayers builds the layers of keys containers of docs with 8-bit
// values. Every 4th container keeps one doc in 64, as if the rest were deleted.
func newHeadroomBenchLayers(keys uint64) [65]*sroar.Bitmap {
	var layers [65]*sroar.Bitmap
	for i := range layers {
		layers[i] = sroar.NewBitmap()
	}
	const keysPerChunk = 64
	var lists [9][]uint64
	for first := uint64(0); first < keys; first += keysPerChunk {
		for l := range lists {
			lists[l] = lists[l][:0]
		}
		for key := first; key < min(first+keysPerChunk, keys); key++ {
			for i := uint64(0); i < 1<<16; i++ {
				if key%4 == 3 && i%64 != 0 {
					continue
				}
				docID := key<<16 + i
				lists[0] = append(lists[0], docID)
				for bit, value := 0, headroomBenchValue(docID); bit < 8; bit++ {
					if value>>bit&1 == 1 {
						lists[bit+1] = append(lists[bit+1], docID)
					}
				}
			}
		}
		for l, list := range lists {
			layers[l].Or(sroar.FromSortedList(list))
		}
	}
	return layers
}

// newHeadroomBenchFlushes appends docs past the last of keys containers and
// moves random live docs to new values, as a property-only update does.
func newHeadroomBenchFlushes(logger logrus.FieldLogger, keys uint64, flushes int, appends, updates uint64) []*Memtable {
	rnd := rand.New(rand.NewPCG(1, 2))
	memtables := make([]*Memtable, flushes)
	nextDocID := keys << 16
	for f := range memtables {
		mem := NewMemtable(logger)
		for range appends {
			mem.Insert(headroomBenchValue(nextDocID), []uint64{nextDocID})
			nextDocID++
		}
		for range updates {
			key, i := rnd.Uint64N(keys), rnd.Uint64N(1<<16)
			if key%4 == 3 {
				i &^= 63
			}
			mem.Insert(rnd.Uint64N(256), []uint64{key<<16 + i})
		}
		memtables[f] = mem
	}
	return memtables
}

func headroomBenchValue(docID uint64) uint64 {
	return docID * 0x9E3779B97F4A7C15 >> 56
}

func BenchmarkSegmentInMemoryReader(b *testing.B) {
	bufPool := newRangedBufPool(b)
	ctx := context.Background()
	for _, data := range newRangeBenchData(b) {
		readers, releaseReaders := newSegmentInMemoryFrom(b, data.mem).Readers(bufPool)
		for _, val := range data.values {
			for _, op := range segmentReaderOperators {
				b.Run(fmt.Sprintf("%s/%s/%s", data.name, val.name, op.Name()), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						_, release, err := readers[0].Read(ctx, val.value, op)
						if err != nil {
							b.Fatal(err)
						}
						release()
					}
				})
			}
		}
		releaseReaders()
	}
}

// headroomPool records whether each bitmap built from it got headroom.
type headroomPool struct {
	*roaringset.BitmapBufPoolTrackingForTests
	headroom map[*sroar.Bitmap]bool
}

func newHeadroomPool() *headroomPool {
	return &headroomPool{
		BitmapBufPoolTrackingForTests: roaringset.NewBitmapBufPoolTrackingForTests(),
		headroom:                      map[*sroar.Bitmap]bool{},
	}
}

func (p *headroomPool) Clone(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	cloned, put := p.BitmapBufPoolTrackingForTests.Clone(bm)
	p.headroom[cloned] = false
	return cloned, put
}

func (p *headroomPool) CloneWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (*sroar.Bitmap, func()) {
	cloned, put := p.BitmapBufPoolTrackingForTests.CloneWithHeadroom(bm, spareRatio)
	p.headroom[cloned] = true
	return cloned, put
}

func (p *headroomPool) ZeroedLayout(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	zeroed, put := p.BitmapBufPoolTrackingForTests.ZeroedLayout(bm)
	p.headroom[zeroed] = false
	return zeroed, put
}

func (p *headroomPool) ZeroedLayoutWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (*sroar.Bitmap, func()) {
	zeroed, put := p.BitmapBufPoolTrackingForTests.ZeroedLayoutWithHeadroom(bm, spareRatio)
	p.headroom[zeroed] = true
	return zeroed, put
}

func newSegmentInMemoryFrom(tb testing.TB, mem *Memtable) *SegmentInMemory {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	seg := NewSegmentInMemory(logger)
	require.NoError(tb, seg.MergeSegmentByCursor(newFakeSegmentCursor(mem)))
	return seg
}

// newSegmentInMemoryWithDeadSpace merges two segments over keys containers of
// docsPerKey docs each. The second deletes all but every 64th doc of the odd
// containers and moves every 4th doc of the even ones to another value.
func newSegmentInMemoryWithDeadSpace(tb testing.TB, keys, docsPerKey uint64) *SegmentInMemory {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	base, update := NewMemtable(logger), NewMemtable(logger)
	for key := uint64(0); key < keys; key++ {
		for i := uint64(0); i < docsPerKey; i++ {
			docID := key<<16 + i
			base.Insert(deadSpaceValue(i), []uint64{docID})
			switch {
			case key%2 == 1 && i%64 != 0:
				update.Delete(0, []uint64{docID})
			case key%2 == 0 && i%4 == 0:
				update.Insert(i%20+100, []uint64{docID})
			}
		}
	}
	seg := newSegmentInMemoryFrom(tb, base)
	require.NoError(tb, seg.MergeSegmentByCursor(newFakeSegmentCursor(update)))
	return seg
}

// deadSpaceValue spreads docs over 19 low values and the maximum.
func deadSpaceValue(i uint64) uint64 {
	if value := i % 20; value != 19 {
		return value
	}
	return math.MaxUint64
}

func keysNodeSize(bm *sroar.Bitmap) uint64 {
	return binary.LittleEndian.Uint64(bm.ToBuffer())
}

// mergeMemtableNow merges mem on the calling goroutine.
func mergeMemtableNow(s *SegmentInMemory, mem *Memtable) {
	s.memtables = append(s.memtables, mem)
	s.mergeMemtables()
}

// newRangedBufPool returns the production pool, whose buckets round sizes up.
func newRangedBufPool(tb testing.TB) roaringset.BitmapBufPool {
	logger, _ := test.NewNullLogger()
	pool, closePool := roaringset.NewBitmapBufPoolDefault(logger, nil, 0, 0)
	tb.Cleanup(closePool)
	return pool
}

func assertElemsByBit(t *testing.T, s *SegmentInMemory, expectedElemsByBit map[int][]uint64) {
	t.Helper()
	for bit := 0; bit < 65; bit++ {
		if elems, ok := expectedElemsByBit[bit]; ok {
			assert.ElementsMatch(t, elems, s.bitmaps[bit].ToArray())
		} else {
			assert.True(t, s.bitmaps[bit].IsEmpty())
		}
	}
}

func waitUntilMemtablesMerged(t *testing.T, s *SegmentInMemory) {
	t.Helper()
	require.Eventually(t, func() bool { return s.countPendingMemtables() == 0 }, time.Second, 10*time.Millisecond)
}
