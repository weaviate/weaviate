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
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestSegmentInMemory(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("bitmaps are initialized and empty", func(t *testing.T) {
		s := NewSegmentInMemory(logger, nil)

		for i := range s.bitmaps {
			assert.NotNil(t, s.bitmaps[i])
			assert.True(t, s.bitmaps[i].IsEmpty())
		}
	})

	t.Run("size is sum of bitmap sizes", func(t *testing.T) {
		bmSize := sroar.NewBitmap().LenInBytes()

		s := NewSegmentInMemory(logger, nil)
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

			seg := NewSegmentInMemory(logger, nil)
			seg.MergeSegmentByCursor(cur1)
			seg.MergeSegmentByCursor(cur2)
			seg.MergeSegmentByCursor(cur3)

			assertElemsByBit(t, seg, expectedElemsByBit)
		})

		t.Run("memtables", func(t *testing.T) {
			seg := NewSegmentInMemory(logger, nil)
			seg.MergeMemtableEventually(mt1)
			seg.MergeMemtableEventually(mt2)
			seg.MergeMemtableEventually(mt3)

			waitUntilMemtablesMerged(t, seg)
			assertElemsByBit(t, seg, expectedElemsByBit)
		})

		t.Run("segments + memtable", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)

			seg := NewSegmentInMemory(logger, nil)
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
			seg := NewSegmentInMemory(logger, nil)
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

			seg := NewSegmentInMemory(logger, nil)
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
	s := NewSegmentInMemory(logger, nil)

	err := s.MergeSegmentByCursor(&outOfRangeSecondKeyCursor{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid key")
}

func TestSegmentInMemoryReader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := NewSegmentInMemory(logger, nil)
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
			{name: "empty", seg: NewSegmentInMemory(logger, nil), cases: emptyCases},
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

	seg := NewSegmentInMemory(logger, nil)
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
	const keys, docsPerKey = 64, 100

	mem := NewMemtable(logger)
	for key := uint64(0); key < keys; key++ {
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
		for _, docID := range []uint64{keys << 16, (keys + 8) << 16, docsPerKey, 3<<16 + docsPerKey} {
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

	// the noop pool does not round sizes up, so only readSpareRatio makes room
	bufPool := roaringset.NewBitmapBufPoolNoop()
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
		seg := NewSegmentInMemory(logger, nil)
		require.NoError(t, seg.MergeSegmentByCursor(olderSegment()))
		require.NoError(t, seg.MergeSegmentByCursor(newerSegment()))

		assert.Equal(t, []uint64{docX}, equalDocIDs(t, seg, valNew))
		assert.NotContains(t, equalDocIDs(t, seg, valOld), docX)
		// The untouched docID keeps its value (membership + value backstop).
		assert.Equal(t, []uint64{docStable}, equalDocIDs(t, seg, valStable))
	})

	t.Run("trap: incremental older-onto-newer fold corrupts the value (old wins)", func(t *testing.T) {
		seg := NewSegmentInMemory(logger, nil)
		require.NoError(t, seg.MergeSegmentByCursor(newerSegment()))
		require.NoError(t, seg.MergeSegmentByCursor(olderSegment()))

		assert.Equal(t, []uint64{docX}, equalDocIDs(t, seg, valOld))
		assert.NotContains(t, equalDocIDs(t, seg, valNew), docX)
	})
}

func TestSegmentInMemoryCompact(t *testing.T) {
	logger, _ := test.NewNullLogger()
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

	t.Run("dead space reclaimed at run time", func(t *testing.T) {
		seg := newSegmentInMemoryWithDeadSpace(t, keys, docsPerKey)
		seg.compactInterval = 0
		layer0 := seg.bitmaps[0]
		mem := NewMemtable(logger)
		for docID := uint64(0); docID < 2000; docID++ {
			mem.Insert(3, []uint64{docID})
		}

		mergeMemtableNow(seg, mem)

		assert.Less(t, seg.bitmaps[0].CapInBytes(), layer0.LenInBytes(), "odd containers of layer 0 must turn into arrays")
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
		{name: "empty", seg: NewSegmentInMemory(logger, nil)},
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

// Pins that a memtable added while others merge is served meanwhile and merged after them.
func TestSegmentInMemoryMemtableAddedWhileMerging(t *testing.T) {
	logger, _ := test.NewNullLogger()
	newMemtable := func(value uint64, docIDs ...uint64) *Memtable {
		mem := NewMemtable(logger)
		mem.Insert(value, docIDs)
		return mem
	}
	// doc 1 gets a new value in each, so a wrong order is visible
	newMemtables := func() (mt1, mt2, mt3 *Memtable) {
		return newMemtable(1, 1, 10), newMemtable(2, 1, 20), newMemtable(3, 1, 30)
	}
	assertServesAll := func(t *testing.T, seg *SegmentInMemory) {
		t.Helper()
		assert.Equal(t, []uint64{10}, combinedRows(t, seg, 1, filters.OperatorEqual))
		assert.Equal(t, []uint64{20}, combinedRows(t, seg, 2, filters.OperatorEqual))
		assert.Equal(t, []uint64{1, 30}, combinedRows(t, seg, 3, filters.OperatorEqual))
	}
	assertMerged := func(t *testing.T, seg *SegmentInMemory) {
		t.Helper()
		require.Zero(t, seg.countPendingMemtables())
		assertServesAll(t, seg)
	}

	// mergeBehindReader returns once a merge of mt1 and mt2 has built their nodes and
	// waits for the returned release.
	mergeBehindReader := func(mt1, mt2 *Memtable) (seg *SegmentInMemory, release func()) {
		seg = NewSegmentInMemory(logger, nil)
		_, releaseReaders := seg.Readers(roaringset.NewBitmapBufPoolNoop())
		seg.memtables = append(seg.memtables, mt1)
		seg.MergeMemtableEventually(mt2)
		synctest.Wait()
		return seg, sync.OnceFunc(releaseReaders)
	}

	t.Run("merge", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			mt1, mt2, mt3 := newMemtables()
			seg, release := mergeBehindReader(mt1, mt2)
			defer release()

			// holds the merge at the removal, inside bitmapsLock
			seg.memtablesLock.Lock()
			unlock := sync.OnceFunc(seg.memtablesLock.Unlock)
			defer unlock()
			release()
			// a goroutine waiting for a mutex keeps synctest's clock from advancing
			locked := func() bool {
				runtime.Gosched()
				if seg.bitmapsLock.TryRLock() {
					seg.bitmapsLock.RUnlock()
					return false
				}
				return true
			}
			taken := locked()
			for i := 0; i < 1_000_000 && !taken; i++ {
				taken = locked()
			}
			require.True(t, taken, "the merge never took bitmapsLock")
			for range 100_000 {
				require.True(t, locked(), "the list must change under bitmapsLock")
			}

			seg.memtables = append(seg.memtables, mt3)
			unlock()
			assertServesAll(t, seg)

			synctest.Wait()
			assertMerged(t, seg)
		})
	})

	t.Run("compaction", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			mt1, mt2, mt3 := newMemtables()
			checker := newBlockingAllocChecker(t)
			seg := NewSegmentInMemory(logger, checker)
			seg.compactInterval = 0

			seg.memtables = append(seg.memtables, mt1)
			seg.MergeMemtableEventually(mt2)
			synctest.Wait()
			require.True(t, seg.bitmapsLock.TryRLock(), "compaction holds bitmapsLock")
			seg.bitmapsLock.RUnlock()

			seg.MergeMemtableEventually(mt3)
			assertServesAll(t, seg)
			synctest.Wait()
			require.Equal(t, []*Memtable{mt3}, seg.memtables, "a second merge ran alongside the first")

			checker.resume()
			synctest.Wait()
			require.Zero(t, seg.countPendingMemtables())
			assertServesAll(t, seg)
			assert.False(t, seg.changed)
		})
	})
}

func TestSegmentInMemoryDropsMergedMemtables(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)
	seg := NewSegmentInMemory(logger, nil)
	seg.memtables = append(seg.memtables, mt1, mt2, mt3)
	backing := seg.memtables[:cap(seg.memtables)]

	seg.mergeMemtables()

	for i, mem := range backing {
		assert.Nil(t, mem, "slot %d keeps a merged memtable alive", i)
	}
}

// Pins merging mergeBatchMemtables at a time, compacting once drained or every compactDeferBatches batches.
func TestSegmentInMemoryMergeBacklog(t *testing.T) {
	logger, _ := test.NewNullLogger()
	for _, tc := range []struct {
		memtables       int
		pendingAtChecks []int
	}{
		{10, []int{0}},
		{32, []int{0}},
		{33, []int{1, 0}},
		{72, []int{40, 8, 0}},
	} {
		t.Run(fmt.Sprintf("%d memtables", tc.memtables), func(t *testing.T) {
			seg := NewSegmentInMemory(logger, nil)
			seg.compactInterval = 0
			var pendingAtChecks []int
			// the checker refuses, so each check is recorded without paying for a compaction
			seg.allocChecker = fakeAllocChecker{check: func(int64) error {
				pendingAtChecks = append(pendingAtChecks, seg.countPendingMemtables())
				return errors.New("refused")
			}}
			for docID := range tc.memtables {
				mem := NewMemtable(logger)
				mem.Insert(1, []uint64{uint64(docID)})
				seg.memtables = append(seg.memtables, mem)
			}

			seg.mergeMemtables()

			assert.Equal(t, tc.pendingAtChecks, pendingAtChecks)
			assert.Zero(t, seg.countPendingMemtables())
		})
	}
}

// Pins compaction only once compactInterval has passed since the last one and a merge changed the layers.
func TestSegmentInMemoryCompactionDue(t *testing.T) {
	logger, _ := test.NewNullLogger()
	docIDs := func(first, count uint64) []uint64 {
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = first + uint64(i)
		}
		return ids
	}
	inserts := func(first, count uint64) *Memtable {
		mem := NewMemtable(logger)
		mem.Insert(7, docIDs(first, count))
		return mem
	}
	// newSeg must run inside the synctest bubble, so its deadline is on the fake clock
	newSeg := func(t *testing.T, checker memwatch.AllocChecker) *SegmentInMemory {
		seg := NewSegmentInMemory(logger, checker)
		require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(inserts(0, 1000))))
		return seg
	}
	// mergeCompacts merges mem and reports whether the layers were swapped
	mergeCompacts := func(seg *SegmentInMemory, mem *Memtable) bool {
		before := seg.bitmaps[0]
		mergeMemtableNow(seg, mem)
		return seg.bitmaps[0] != before
	}

	for _, tc := range []struct {
		name  string
		after time.Duration
		mem   *Memtable
		want  bool
	}{
		{"appends after an interval", compactInterval, inserts(1000, 1), true},
		{"nothing changed after an interval", compactInterval, NewMemtable(logger), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				seg := newSeg(t, nil)
				time.Sleep(tc.after)
				assert.Equal(t, tc.want, mergeCompacts(seg, tc.mem))
				if tc.want {
					assert.False(t, seg.changed)
				}
			})
		})
	}

	t.Run("an interval after the last compaction", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			seg := newSeg(t, nil)
			time.Sleep(compactInterval)
			require.True(t, mergeCompacts(seg, inserts(0, 1)))

			time.Sleep(compactInterval - time.Nanosecond)
			require.False(t, mergeCompacts(seg, inserts(0, 1)), "compacted before an interval passed")
			time.Sleep(time.Nanosecond)
			assert.True(t, mergeCompacts(seg, inserts(0, 1)))
		})
	})

	t.Run("a refusal keeps the deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			refuse := true
			seg := newSeg(t, fakeAllocChecker{check: func(int64) error {
				if refuse {
					return errors.New("refused")
				}
				return nil
			}})
			time.Sleep(compactInterval)
			require.False(t, mergeCompacts(seg, inserts(0, 1)))

			refuse = false
			assert.True(t, mergeCompacts(seg, inserts(0, 1)), "the next merge must compact without waiting again")
		})
	})

	t.Run("indexes built together compact apart", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			deadlines := map[time.Time]bool{}
			for range 20 {
				seg := NewSegmentInMemory(logger, nil)
				deadline := seg.compactedAt.Add(compactInterval)
				require.Greater(t, time.Until(deadline), compactInterval/2)
				require.LessOrEqual(t, time.Until(deadline), compactInterval)
				deadlines[deadline] = true
			}
			assert.Greater(t, len(deadlines), 1, "every index got the same deadline")
		})
	})
}

func TestSegmentInMemoryCompactionAllocChecker(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	refuse := true
	var checked []int64
	update := NewMemtable(logger)
	for docID := uint64(0); docID < 2000; docID++ {
		update.Insert(3, []uint64{docID})
	}
	// merging the rows it already holds leaves no dead space to pad the estimate
	seg := newSegmentInMemoryFrom(t, update)
	seg.Compact()
	seg.logger = logger
	seg.compactInterval = 0
	seg.allocChecker = fakeAllocChecker{check: func(size int64) error {
		checked = append(checked, size)
		if refuse {
			return errors.New("refused")
		}
		return nil
	}}
	before := seg.bitmaps

	mergeMemtableNow(seg, update)
	require.True(t, before == seg.bitmaps, "a refused compaction must keep every layer")
	require.True(t, seg.changed)
	require.Len(t, checked, 1)
	assert.Equal(t, 1, countLogs(hook, logrus.DebugLevel))
	assert.Len(t, hook.AllEntries(), 1)

	refuse = false
	one := NewMemtable(logger)
	one.Insert(3, []uint64{1})
	mergeMemtableNow(seg, one)
	require.NotSame(t, before[0], seg.bitmaps[0])
	assert.False(t, seg.changed)
	require.Len(t, checked, 2)
	assert.GreaterOrEqual(t, checked[1], int64(seg.Size()), "must cover the copies")
}

func TestSegmentInMemoryCompactionMatchesUncompactedTwin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	rnd := rand.New(rand.NewPCG(1, 2))
	values := []uint64{0, 1, 5, 13, 1 << 40, math.MaxUint64 - 1, math.MaxUint64}
	seg, twin := NewSegmentInMemory(logger, nil), NewSegmentInMemory(logger, nil)
	twin.compactInterval = math.MaxInt64

	const merges = 25
	compactions := 0
	for i := range merges {
		changes := 20 + rnd.IntN(600)
		if i == 0 {
			changes = 12_000 // enough for bitmap containers
		}
		mem := NewMemtable(logger)
		for range changes {
			docID := rnd.Uint64N(3)<<16 + rnd.Uint64N(1<<13)
			if rnd.IntN(3) == 0 {
				mem.Delete(0, []uint64{docID})
			} else {
				mem.Insert(values[rnd.IntN(len(values))], []uint64{docID})
			}
		}
		// every third merge compacts, so most land on layers compacted earlier
		seg.compactInterval = math.MaxInt64
		if i%3 == 0 {
			seg.compactInterval = 0
		}
		before := seg.bitmaps[0]
		mergeMemtableNow(seg, mem)
		mergeMemtableNow(twin, mem)
		if seg.bitmaps[0] != before {
			compactions++
			requireSameLayers(t, twin, seg)
		}
	}
	requireSameLayers(t, twin, seg)
	require.Greater(t, compactions, 1)
	require.Less(t, compactions, merges, "some merges must land on layers compacted earlier")

	for _, value := range values {
		for _, op := range segmentReaderOperators {
			assert.Equal(t, combinedRows(t, twin, value, op), combinedRows(t, seg, value, op), "%s %d", op.Name(), value)
		}
	}
}

func TestSegmentInMemoryCompactionSwapWaitsForReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		checker := newBlockingAllocChecker(t)
		seg := NewSegmentInMemory(logger, checker)
		seg.compactInterval = 0
		before := seg.bitmaps[0]
		mem := NewMemtable(logger)
		mem.Insert(5, []uint64{1, 2, 3})
		seg.MergeMemtableEventually(mem)
		synctest.Wait()
		require.True(t, seg.bitmapsLock.TryRLock(), "compaction holds bitmapsLock")
		seg.bitmapsLock.RUnlock()

		readers, releaseReaders := seg.Readers(roaringset.NewBitmapBufPoolNoop())
		release := sync.OnceFunc(releaseReaders)
		defer release()
		read := func() []uint64 {
			layer, releaseLayer, err := readers[0].Read(context.Background(), 5, filters.OperatorEqual)
			require.NoError(t, err)
			defer releaseLayer()
			return layer.Additions.ToArray()
		}
		require.Equal(t, []uint64{1, 2, 3}, read())

		checker.resume()
		synctest.Wait()
		require.Same(t, before, seg.bitmaps[0], "the swap did not wait for the reader")
		assert.Equal(t, []uint64{1, 2, 3}, read())

		release()
		synctest.Wait()
		require.False(t, seg.merging)
		assert.NotSame(t, before, seg.bitmaps[0])
		assert.Equal(t, []uint64{1, 2, 3}, combinedRows(t, seg, 5, filters.OperatorEqual))
	})
}

// Pins a panic while copying a layer, which leaves the index mergeable and the
// compaction due.
func TestSegmentInMemoryCompactionPanic(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false") // the integration run sets it to true
	synctest.Test(t, func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		checker := newBlockingAllocChecker(t)
		seg := NewSegmentInMemory(logger, checker)
		seg.compactInterval = 0
		// the last layer's only container ends past its buffer, so copying it panics
		whole := roaringset.NewBitmap(5 << 16)
		broken := whole.ToBuffer()
		seg.bitmaps[64] = sroar.FromBuffer(append(make([]byte, 0, len(broken)-8), broken[:len(broken)-8]...))
		before := seg.bitmaps[0]

		mem := NewMemtable(logger)
		for docID := uint64(0); docID < 100; docID++ {
			mem.Insert(3, []uint64{docID})
		}
		seg.MergeMemtableEventually(mem)
		synctest.Wait()
		require.True(t, seg.bitmapsLock.TryRLock(), "compaction holds bitmapsLock")
		release := sync.OnceFunc(seg.bitmapsLock.RUnlock)
		defer release()
		checker.resume()
		synctest.Wait()
		require.Positive(t, countLogs(hook, logrus.ErrorLevel), "copying must panic or the case tests nothing")
		release()
		require.False(t, seg.merging, "the panic left merging set")
		require.Zero(t, seg.countPendingMemtables())
		require.Same(t, before, seg.bitmaps[0])
		require.True(t, seg.changed)

		seg.bitmaps[64] = sroar.NewBitmap()
		one := NewMemtable(logger)
		one.Insert(3, []uint64{100})
		seg.MergeMemtableEventually(one)
		synctest.Wait()
		assert.NotSame(t, before, seg.bitmaps[0])
		assert.Equal(t, 101, seg.bitmaps[0].GetCardinality())
	})
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
						seg = NewSegmentInMemory(logger, nil)
						seg.compactInterval = math.MaxInt64 // a compaction would hide the growths
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

// BenchmarkSegmentInMemoryReadHeadroom merges a pending, a flushing and an active
// memtable into results of just under 1 MiB, just under the pool's largest bucket, and
// above it. It reports growths, the buffers they outgrew and idle capacity at release.
func BenchmarkSegmentInMemoryReadHeadroom(b *testing.B) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	pool := newRangedBufPool(b)

	for _, keys := range []uint64{156, 5020, 8192} {
		seg := NewSegmentInMemory(logger, nil)
		seg.bitmaps = newHeadroomBenchLayers(keys)
		seg.Compact()
		for _, flush := range []struct {
			name             string
			appends, updates uint64
		}{
			{"appends_1k", 1 << 10, 1 << 8},
			{"appends_64k", 1 << 16, 1 << 12},
			{"appends_256k", 1 << 18, 1 << 14},
		} {
			memtables := newHeadroomBenchFlushes(logger, keys, 3, flush.appends, flush.updates)
			seg.memtables = memtables[:1]
			for _, spareRatio := range []float64{0, 0.05, 0.10, 0.25} {
				name := fmt.Sprintf("result_%.1fMiB/%s/spare_%.2f", float64(seg.bitmaps[0].LenInBytes())/(1<<20), flush.name, spareRatio)
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					bufPool := &spareRatioPool{BitmapBufPool: pool, spareRatio: spareRatio}
					growths, outgrown, idle, runs := 0, 0, 0, 0
					for b.Loop() {
						readers, release := seg.Readers(bufPool)
						readers = append(readers, NewMemtableReader(memtables[1]), NewMemtableReader(memtables[2]))
						combined := NewCombinedReader(readers, release, concurrency.SROAR_MERGE, logger)

						getsBefore, capsBefore := bufPool.gets.Load(), bufPool.caps.Load()
						// >= 0 clones layer 0, so the pool serves the result and nothing else
						bm, releaseBm, err := combined.Read(ctx, 0, filters.OperatorGreaterThanEqual)
						if err != nil {
							b.Fatal(err)
						}
						growths += int(bufPool.gets.Load()-getsBefore) - 1
						outgrown += int(bufPool.caps.Load()-capsBefore) - bm.CapInBytes()
						idle += bm.CapInBytes() - bm.LenInBytes()
						runs++
						releaseBm()
						combined.Close()
					}
					b.ReportMetric(float64(growths)/float64(runs), "growths/op")
					b.ReportMetric(float64(outgrown)/float64(runs)/(1<<20), "outgrown_MiB/op")
					b.ReportMetric(float64(idle)/float64(runs)/(1<<20), "idle_MiB/op")
				})
			}
		}
	}
}

// spareRatioPool clones with its own spare ratio, whatever the caller asks for, and
// counts the buffers Get hands out and their capacity.
type spareRatioPool struct {
	roaringset.BitmapBufPool
	spareRatio float64
	gets, caps atomic.Int64
}

func (p *spareRatioPool) Get(minCap int) ([]byte, func()) {
	buf, put := p.BitmapBufPool.Get(minCap)
	p.gets.Add(1)
	// a bitmap uses the even prefix of its buffer, which CapInBytes reports
	p.caps.Add(int64(cap(buf) / 2 * 2))
	return buf, put
}

func (p *spareRatioPool) CloneWithHeadroom(bm *sroar.Bitmap, _ float64) (*sroar.Bitmap, func()) {
	dst, put := p.GetBitmapStruct()
	return bm.CloneTo(dst, sroar.BuildOpts{Pool: p, InitialSpareRatio: p.spareRatio}), put
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

func requireSameLayers(tb testing.TB, want, got *SegmentInMemory) {
	tb.Helper()
	for i := range want.bitmaps {
		require.True(tb, slices.Equal(want.bitmaps[i].ToArray(), got.bitmaps[i].ToArray()), "layer %d", i)
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
	seg := NewSegmentInMemory(logger, nil)
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
	pool, closePool := roaringset.NewBitmapBufPoolDefault(logger, nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
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

// mergeTestTimeout leaves room for a slow runner under -race with coverage.
const mergeTestTimeout = 10 * time.Second

func waitUntilMemtablesMerged(t *testing.T, s *SegmentInMemory) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.memtablesLock.Lock()
		defer s.memtablesLock.Unlock()
		return len(s.memtables) == 0 && !s.merging
	}, mergeTestTimeout, time.Millisecond)
}

func countLogs(hook *test.Hook, level logrus.Level) int {
	count := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == level {
			count++
		}
	}
	return count
}

// combinedRows reads the layers and the pending memtables.
func combinedRows(t *testing.T, seg *SegmentInMemory, value uint64, op filters.Operator) []uint64 {
	t.Helper()
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	creader := NewCombinedReader(readers, release, 1, seg.logger)
	defer creader.Close()
	bm, releaseBm, err := creader.Read(context.Background(), value, op)
	require.NoError(t, err)
	defer releaseBm()
	return bm.ToArray()
}

type fakeAllocChecker struct {
	memwatch.AllocChecker
	check func(size int64) error
}

func (c fakeAllocChecker) CheckAlloc(size int64) error { return c.check(size) }

// blockingAllocChecker holds each compaction before its copies until resume.
type blockingAllocChecker struct {
	memwatch.AllocChecker
	resumed chan struct{}
	resume  func()
}

func newBlockingAllocChecker(t *testing.T) *blockingAllocChecker {
	c := &blockingAllocChecker{resumed: make(chan struct{})}
	c.resume = sync.OnceFunc(func() { close(c.resumed) })
	t.Cleanup(c.resume)
	return c
}

func (c *blockingAllocChecker) CheckAlloc(int64) error {
	<-c.resumed
	return nil
}
