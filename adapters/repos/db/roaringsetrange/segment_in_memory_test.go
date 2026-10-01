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
	"math"
	"testing"
	"time"
	"unsafe"

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

	t.Run("merging", func(t *testing.T) {
		mt1, mt2, mt3 := createTestMemtables(logger)

		t.Run("segments", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)
			cur3 := newFakeSegmentCursor(mt3)

			seg := NewSegmentInMemory(logger)
			seg.MergeSegmentByCursor(cur1)
			seg.MergeSegmentByCursor(cur2)
			seg.MergeSegmentByCursor(cur3)

			assertElemsByBit(t, seg, testMemtablesElemsByBit)
		})

		t.Run("memtables", func(t *testing.T) {
			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(mt1)
			seg.MergeMemtableEventually(mt2)
			seg.MergeMemtableEventually(mt3)

			waitUntilMemtablesMerged(t, seg)
			assertElemsByBit(t, seg, testMemtablesElemsByBit)
		})

		t.Run("segments + memtable", func(t *testing.T) {
			cur1 := newFakeSegmentCursor(mt1)
			cur2 := newFakeSegmentCursor(mt2)

			seg := NewSegmentInMemory(logger)
			seg.MergeSegmentByCursor(cur1)
			seg.MergeSegmentByCursor(cur2)
			seg.MergeMemtableEventually(mt3)

			waitUntilMemtablesMerged(t, seg)
			assertElemsByBit(t, seg, testMemtablesElemsByBit)
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

	bufPool := roaringset.NewBitmapBufPoolTrackingForTests()
	readers, release := seg.Readers(bufPool)
	defer release()

	require.Len(t, readers, 1)
	reader := readers[0]

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

				assert.GreaterOrEqual(t, int64(1), bufPool.Outstanding())
				release()
				assert.Zero(t, bufPool.Outstanding())
			})
		}
	})
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
		return readDocIDs(t, seg, bufPool, value, filters.OperatorEqual)
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

// testMemtablesElemsByBit is what the three createTestMemtables memtables
// merge to, per bit layer. Layers the map omits end up empty.
var testMemtablesElemsByBit = map[int][]uint64{
	0: {10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
	1: {119, 219, 117, 217, 15, 25, 113, 213},
	2: {119, 219},
	3: {14, 24, 15, 25, 113, 213},
	4: {113, 213},
	5: {119, 219, 117, 217},
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

// TestSegmentInMemoryShrink pins that Shrink hands the layer bitmaps' spare
// capacity back to the heap without changing what the rep serves.
func TestSegmentInMemoryShrink(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bufPool := roaringset.NewBitmapBufPoolNoop()

	t.Run("spare capacity is gone and reads are unchanged", func(t *testing.T) {
		s := newOvergrownSegmentInMemory(t, logger)
		require.Positive(t, spareCapacityInBytes(t, s), "fixture must leave the layers overgrown")

		testCases := []struct {
			name     string
			value    uint64
			operator filters.Operator
			noMatch  bool
		}{
			{name: "equal", value: valueStride, operator: filters.OperatorEqual},
			{name: "equal lowest value", value: 0, operator: filters.OperatorEqual},
			{name: "equal max uint64", value: math.MaxUint64, operator: filters.OperatorEqual, noMatch: true},
			{name: "not equal", value: valueStride, operator: filters.OperatorNotEqual},
			{name: "less than", value: valueStride, operator: filters.OperatorLessThan},
			{name: "less than lowest value", value: 0, operator: filters.OperatorLessThan, noMatch: true},
			{name: "less than equal", value: valueStride, operator: filters.OperatorLessThanEqual},
			{name: "less than equal max uint64", value: math.MaxUint64, operator: filters.OperatorLessThanEqual},
			{name: "greater than", value: valueStride, operator: filters.OperatorGreaterThan},
			{name: "greater than max uint64", value: math.MaxUint64, operator: filters.OperatorGreaterThan, noMatch: true},
			{name: "greater than equal", value: valueStride, operator: filters.OperatorGreaterThanEqual},
			{name: "greater than equal lowest value", value: 0, operator: filters.OperatorGreaterThanEqual},
		}

		beforeShrink := make([][]uint64, len(testCases))
		for i, tc := range testCases {
			beforeShrink[i] = readDocIDs(t, s, bufPool, tc.value, tc.operator)
			if !tc.noMatch {
				require.NotEmpty(t, beforeShrink[i], "%s must match before the shrink, or it proves nothing", tc.name)
			}
		}

		s.Shrink()
		assert.Zero(t, spareCapacityInBytes(t, s))

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, beforeShrink[i], readDocIDs(t, s, bufPool, tc.value, tc.operator))
			})
		}
	})

	t.Run("layers left empty by the merge survive the shrink", func(t *testing.T) {
		mt1, mt2, _ := createTestMemtables(logger)
		s := NewSegmentInMemory(logger)
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt1)))
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt2)))

		beforeShrink := make([][]uint64, len(s.bitmaps))
		emptyLayers := 0
		for i := range s.bitmaps {
			beforeShrink[i] = s.bitmaps[i].ToArray()
			if len(beforeShrink[i]) == 0 {
				emptyLayers++
			}
		}
		require.Positive(t, emptyLayers, "fixture must leave some layers empty")

		s.Shrink()

		for i := range s.bitmaps {
			require.NotNil(t, s.bitmaps[i], "layer %d", i)
			assert.Equal(t, beforeShrink[i], s.bitmaps[i].ToArray(), "layer %d", i)
		}
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213},
			readDocIDs(t, s, bufPool, 20, filters.OperatorGreaterThanEqual))
	})

	t.Run("a rep with no merged segment survives the shrink", func(t *testing.T) {
		s := NewSegmentInMemory(logger)
		s.Shrink()

		for i := range s.bitmaps {
			require.NotNil(t, s.bitmaps[i], "layer %d", i)
			assert.True(t, s.bitmaps[i].IsEmpty(), "layer %d", i)
		}
		assert.Empty(t, readDocIDs(t, s, bufPool, 0, filters.OperatorGreaterThanEqual))

		mt := NewMemtable(logger)
		mt.Insert(7, []uint64{70, 71})
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt)))
		assert.ElementsMatch(t, []uint64{70, 71}, readDocIDs(t, s, bufPool, 7, filters.OperatorEqual))
	})

	t.Run("a memtable merged in after the shrink still lands", func(t *testing.T) {
		mt1, mt2, mt3 := createTestMemtables(logger)
		s := NewSegmentInMemory(logger)
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt1)))
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt2)))

		s.Shrink()
		s.MergeMemtableEventually(mt3)
		waitUntilMemtablesMerged(t, s)

		assertElemsByBit(t, s, testMemtablesElemsByBit)
	})

	t.Run("a segment merged in after the shrink regrows the layers", func(t *testing.T) {
		s := newOvergrownSegmentInMemory(t, logger)
		s.Shrink()
		require.Zero(t, spareCapacityInBytes(t, s))

		// Segment index overgrownSegments is the first doc-ID space the fixture leaves free.
		require.NoError(t, s.MergeSegmentByCursor(overgrowingSegmentCursor(logger, overgrownSegments)))
		assert.Positive(t, spareCapacityInBytes(t, s),
			"callers must shrink only once no further merge is pending")
	})

	t.Run("shrink waits for an outstanding reader", func(t *testing.T) {
		s := newOvergrownSegmentInMemory(t, logger)
		readers, release := s.Readers(bufPool)
		require.Len(t, readers, 1)

		shrunk := make(chan struct{})
		go func() {
			s.Shrink()
			close(shrunk)
		}()

		layer, releaseRead, err := readers[0].Read(context.Background(), valueStride, filters.OperatorEqual)
		require.NoError(t, err)
		docIDs := layer.Additions.ToArray()
		releaseRead()

		select {
		case <-shrunk:
			t.Fatal("shrink swapped the layers while a reader still held them")
		default:
		}

		release()
		select {
		case <-shrunk:
		case <-time.After(5 * time.Second):
			t.Fatal("shrink did not return after the reader released")
		}

		assert.Equal(t, docIDs, readDocIDs(t, s, bufPool, valueStride, filters.OperatorEqual))
	})
}

// overgrowingSegmentCursor spreads data over many of the 65 layers and over many
// sroar containers, which is what makes sroar's doubling growth overshoot.
const (
	overgrownSegments      = 4
	valuesPerSegment       = 600
	docIDsPerValue         = 20
	valueStride            = 99991
	docIDSpacePerSegment   = 1_000_000
	docIDStridePerValue    = 37
	docIDStrideWithinValue = 7919
)

func newOvergrownSegmentInMemory(t *testing.T, logger logrus.FieldLogger) *SegmentInMemory {
	t.Helper()

	s := NewSegmentInMemory(logger)
	for segment := 0; segment < overgrownSegments; segment++ {
		require.NoError(t, s.MergeSegmentByCursor(overgrowingSegmentCursor(logger, segment)))
	}
	return s
}

func overgrowingSegmentCursor(logger logrus.FieldLogger, segment int) SegmentCursor {
	mt := NewMemtable(logger)
	for v := uint64(0); v < valuesPerSegment; v++ {
		docIDs := make([]uint64, docIDsPerValue)
		for d := range docIDs {
			docIDs[d] = uint64(segment)*docIDSpacePerSegment +
				v*docIDStridePerValue + uint64(d)*docIDStrideWithinValue
		}
		mt.Insert(v*valueStride, docIDs)
	}
	return newFakeSegmentCursor(mt)
}

// spareCapacityInBytes is the capacity the layer bitmaps hold beyond their
// content, which sroar does not export. It reads Bitmap's first field as
// the data slice, and fails if that length stops matching LenInBytes.
func spareCapacityInBytes(t *testing.T, s *SegmentInMemory) int {
	t.Helper()

	spare := 0
	for i := range s.bitmaps {
		data := *(*[]uint16)(unsafe.Pointer(s.bitmaps[i]))
		require.Equal(t, s.bitmaps[i].LenInBytes(), len(data)*2,
			"sroar.Bitmap's first field is no longer its data slice")
		spare += (cap(data) - len(data)) * 2
	}
	return spare
}

func readDocIDs(t *testing.T, s *SegmentInMemory, bufPool roaringset.BitmapBufPool,
	value uint64, operator filters.Operator,
) []uint64 {
	t.Helper()

	readers, release := s.Readers(bufPool)
	defer release()
	require.Len(t, readers, 1)

	layer, releaseRead, err := readers[0].Read(context.Background(), value, operator)
	require.NoError(t, err)
	defer releaseRead()

	return layer.Additions.ToArray()
}
