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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestRoaringSetRangeReaderConsistentView(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	key1, key2, key3, key4 := uint64(1), uint64(2), uint64(3), uint64(4)
	createValidateReader := func(reader ReaderRoaringSetRange, expected map[uint64][]uint64) func(*testing.T) {
		return func(t *testing.T) {
			t.Helper()
			for k, expectedV := range expected {
				v, release, err := reader.Read(context.Background(), k, filters.OperatorEqual)

				require.NoError(t, err)
				require.Equal(t, expectedV, v.ToArray())
				release()
			}
		}
	}

	// Initial disk: two segments A (key1->{1}) and B (key2->{2})
	diskSegments := &SegmentGroup{
		logger: logger,
		segments: []Segment{
			newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
				key1: roaringset.NewBitmap(1),
			}, sroar.NewBitmap()),
			newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
				key2: roaringset.NewBitmap(2),
			}, sroar.NewBitmap()),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	// Active memtable contains key3->{3}
	initialMemtable := newTestMemtableRoaringSetRange(map[uint64][]uint64{
		key3: {3},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyRoaringSetRange,
	}

	// Open the reader that should see key1..key3 only and stay stable
	reader1 := b.ReaderRoaringSetRange()
	validateOriginalReader := createValidateReader(reader1, map[uint64][]uint64{
		key1: {1},
		key2: {2},
		key3: {3},
	})
	validateOriginalReader(t)

	// 2) Switch memtables (new empty active, old active -> flushing)
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableRoaringSetRange(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// Reader remains stable (still key1..key3)
	validateOriginalReader(t)

	// 3) New write to the new active: key4->{4}
	require.NoError(t, b.RoaringSetRangeAdd(key4, 4))

	// Reader must still NOT see key4
	validateOriginalReader(t)

	// 4) Flush the flushing memtable (which holds key3) to disk as segment C
	segC := flushRoaringSetRangeTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(segC)

	// Cursor remains unchanged
	validateOriginalReader(t)

	// 5) Compact disk segments while reader is open:
	//    initial: A, B, C
	//    compaction #1: A+B, C
	segAB := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(1),
		key2: roaringset.NewBitmap(2),
	}, sroar.NewBitmap())
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory()

	//    compaction #2: (A+B) + C  => ABC
	segABC := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(1),
		key2: roaringset.NewBitmap(2),
		key3: roaringset.NewBitmap(3),
	}, sroar.NewBitmap())
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory()

	// Cursor still sees only key1..key3
	validateOriginalReader(t)
	reader1.Close()

	// 6) A new cursor now sees the latest state: key1..key4 (key4 is in active)
	reader2 := b.ReaderRoaringSetRange()
	defer reader2.Close()

	createValidateReader(reader2, map[uint64][]uint64{
		key1: {1},
		key2: {2},
		key3: {3},
		key4: {4},
	})(t)
}

func TestRoaringSetRangeReaderConsistentViewInMemo(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	key1, key2, key3, key4 := uint64(1), uint64(2), uint64(3), uint64(4)
	createValidateReader := func(reader ReaderRoaringSetRange, expected map[uint64][]uint64) func(*testing.T) {
		return func(t *testing.T) {
			t.Helper()
			for k, expectedV := range expected {
				v, release, err := reader.Read(context.Background(), k, filters.OperatorEqual)

				require.NoError(t, err)
				require.Equal(t, expectedV, v.ToArray())
				release()
			}
		}
	}

	// Active memtable contains key3->{3}
	initialMemtable := newTestMemtableRoaringSetRange(map[uint64][]uint64{
		key3: {3},
	})

	// Initial segment in memo: (key1->{1}, key2->{2})
	mt := roaringsetrange.NewMemtable(logger)
	mt.Insert(key1, []uint64{1})
	mt.Insert(key2, []uint64{2})
	segInMemo := roaringsetrange.NewSegmentInMemory(logger)
	segInMemo.MergeMemtableEventually(mt)

	b := Bucket{
		active: initialMemtable,
		disk: &SegmentGroup{
			segments:                       []Segment{},
			roaringSetRangeSegmentInMemory: segInMemo,
		},
		strategy:             StrategyRoaringSetRange,
		keepSegmentsInMemory: true,
		bitmapBufPool:        roaringset.NewBitmapBufPoolNoop(),
	}

	// Open the reader that should see key1..key3 only and stay stable
	reader1 := b.ReaderRoaringSetRange()
	validateOriginalReader := createValidateReader(reader1, map[uint64][]uint64{
		key1: {1},
		key2: {2},
		key3: {3},
	})
	validateOriginalReader(t)

	// 2) Switch memtables (new empty active, old active -> flushing)
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableRoaringSetRange(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// Reader remains stable (still key1..key3)
	validateOriginalReader(t)

	// 3) New write to the new active: key4->{4}
	require.NoError(t, b.RoaringSetRangeAdd(key4, 4))

	// Reader must still NOT see key4
	validateOriginalReader(t)

	// 4) Flush the flushing memtable (which holds key3) to disk
	b.atomicallyAddDiskSegmentAndRemoveFlushing(&fakeSegment{})

	// Cursor still sees only key1..key3
	validateOriginalReader(t)
	reader1.Close()

	// 6) A new cursor now sees the latest state: key1..key4 (key4 is in active)
	reader2 := b.ReaderRoaringSetRange()
	defer reader2.Close()

	createValidateReader(reader2, map[uint64][]uint64{
		key1: {1},
		key2: {2},
		key3: {3},
		key4: {4},
	})(t)
}

// TestRoaringSetRangeWritePathRefCount ensures that all write paths of the
// RoaringSetRange type correctly use and release refcounts on the active memtable
// and therefore do not block a flushlock for the entire duration of the write.
func TestRoaringSetRangeWritePathRefCount(t *testing.T) {
	key1, key2 := uint64(1), uint64(2)

	b := Bucket{
		strategy: StrategyRoaringSetRange,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableRoaringSetRange(nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}

	// add 3
	err := b.RoaringSetRangeAdd(key1, 1)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	err = b.RoaringSetRangeAdd(key1, 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	err = b.RoaringSetRangeAdd(key2, 101)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// remove 1
	err = b.RoaringSetRangeRemove(key1, 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	reader := b.ReaderRoaringSetRange()

	v, release, err := reader.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, v.ToArray())
	release()

	v, release, err = reader.Read(context.Background(), key2, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{101}, v.ToArray())
	release()
}
