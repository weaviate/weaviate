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
	"testing"
	"time"

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
			bufPool := roaringset.NewBitmapBufPoolTrackingForTests()
			readers, releaseReaders := seg.Readers(bufPool)
			defer releaseReaders()
			require.Len(t, readers, 1)

			_, release, err := readers[0].Read(context.Background(), tc.value, tc.operator)
			require.NoError(t, err)

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
	keysNodeSize := func(bm *sroar.Bitmap) uint64 { return binary.LittleEndian.Uint64(bm.ToBuffer()) }

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

func newSegmentInMemoryFrom(tb testing.TB, mem *Memtable) *SegmentInMemory {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	seg := NewSegmentInMemory(logger)
	require.NoError(tb, seg.MergeSegmentByCursor(newFakeSegmentCursor(mem)))
	return seg
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
