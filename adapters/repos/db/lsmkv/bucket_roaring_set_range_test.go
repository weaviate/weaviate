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

package lsmkv

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/cyclemanager"
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

func countLogLevel(hook *test.Hook, level logrus.Level) int {
	n := 0
	for _, e := range hook.AllEntries() {
		if e.Level == level {
			n++
		}
	}
	return n
}

// newRangeableRepEmpty returns an unpopulated rep: presence set empty
// (IsUnpopulated == true) despite the 65-bitmap skeleton giving Size() > 0.
func newRangeableRepEmpty(logger logrus.FieldLogger) *roaringsetrange.SegmentInMemory {
	return roaringsetrange.NewSegmentInMemory(logger)
}

func newRangeableRepPopulated(t *testing.T, logger logrus.FieldLogger, value, docID uint64) *roaringsetrange.SegmentInMemory {
	t.Helper()
	rep := roaringsetrange.NewSegmentInMemory(logger)
	mt := roaringsetrange.NewMemtable(logger)
	mt.Insert(value, []uint64{docID})
	rep.MergeMemtableEventually(mt)
	require.Eventually(t, func() bool { return !rep.IsUnpopulated() },
		time.Second, 5*time.Millisecond, "rep must become populated")
	return rep
}

func newRangeableDiskSegment(value, docID uint64) Segment {
	return newFakeRoaringSetRangeSegment(
		map[uint64]*sroar.Bitmap{value: roaringset.NewBitmap(docID)}, sroar.NewBitmap(),
	)
}

func newRangeableBucket(logger logrus.FieldLogger, keepInMem, deferred bool,
	rep *roaringsetrange.SegmentInMemory, disk []Segment,
) *Bucket {
	return &Bucket{
		strategy:                  StrategyRoaringSetRange,
		keepSegmentsInMemory:      keepInMem,
		rangeableInMemoryDeferred: deferred,
		logger:                    logger,
		bitmapBufPool:             roaringset.NewBitmapBufPoolNoop(),
		active:                    newTestMemtableRoaringSetRange(nil),
		disk:                      &SegmentGroup{logger: logger, segments: disk, roaringSetRangeSegmentInMemory: rep},
	}
}

func readEqual(t *testing.T, b *Bucket, value uint64) []uint64 {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	v, release, err := reader.Read(context.Background(), value, filters.OperatorEqual)
	require.NoError(t, err)
	defer release()
	return v.ToArray()
}

// Pins weaviate/weaviate#12199: the last WithKeepSegmentsInMemory wins.
func TestRoaringSetRangeKeepSegmentsInMemoryOverride(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	newBucket := func(t *testing.T, opts ...BucketOption) *Bucket {
		t.Helper()
		base := []BucketOption{
			WithStrategy(StrategyRoaringSetRange),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		}
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			append(base, opts...)...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	t.Run("knob default true builds the rep (control)", func(t *testing.T) {
		b := newBucket(t, WithKeepSegmentsInMemory(true))
		defer b.Shutdown(ctx)
		assert.True(t, b.keepSegmentsInMemory)
		assert.NotNil(t, b.disk.roaringSetRangeSegmentInMemory)
	})

	t.Run("override false after true wins: no rep, disk reader", func(t *testing.T) {
		b := newBucket(t, WithKeepSegmentsInMemory(true), WithKeepSegmentsInMemory(false))
		defer b.Shutdown(ctx)
		assert.False(t, b.keepSegmentsInMemory)
		assert.Nil(t, b.disk.roaringSetRangeSegmentInMemory, "no in-memory rep must be built")

		require.NoError(t, b.RoaringSetRangeAdd(5, 100))
		require.NoError(t, b.FlushAndSwitch())
		assert.Equal(t, []uint64{100}, readEqual(t, b, 5))
	})
}

// Pins weaviate/weaviate#12199: an unpopulated rep with disk segments must
// fall back to disk and WARN once (rep/disk use different docIDs to prove
// which path served the read).
func TestRoaringSetRangeDiskFallback(t *testing.T) {
	const value = uint64(5)

	t.Run("populated rep + disk segments: in-mem path, no fallback, no WARN", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepPopulated(t, logger, value, 100)
		disk := []Segment{newRangeableDiskSegment(value, 200)} // different docID
		b := newRangeableBucket(logger, true, false, rep, disk)

		assert.Equal(t, []uint64{100}, readEqual(t, b, value), "served from the rep, not disk")
		assert.Zero(t, countLogLevel(hook, logrus.WarnLevel))
	})

	t.Run("empty rep + disk segments: fallback, correct result, WARN", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepEmpty(logger)
		// Size() is the wrong discriminant: the skeleton reports non-zero Size()
		// yet IsUnpopulated() is true.
		require.Greater(t, rep.Size(), 0)
		require.True(t, rep.IsUnpopulated())
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, true, false, rep, disk)

		assert.Equal(t, []uint64{200}, readEqual(t, b, value), "served from disk")
		assert.Equal(t, 1, countLogLevel(hook, logrus.WarnLevel))
		assert.Contains(t, hook.LastEntry().Message, "rangeable in-memory index is empty")
	})

	t.Run("empty rep + no disk segments: no fallback, empty result, no WARN", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepEmpty(logger)
		b := newRangeableBucket(logger, true, false, rep, nil)

		assert.Empty(t, readEqual(t, b, value))
		assert.Zero(t, countLogLevel(hook, logrus.WarnLevel))
	})

	t.Run("mass-delete-emptied rep + disk segments: fallback, correct residual, WARN", func(t *testing.T) {
		// Benign cause: mass-delete empties bitmaps[0] while disk tombstones
		// remain and no restart occurred.
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepEmpty(logger)
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, true, false, rep, disk)

		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Equal(t, 1, countLogLevel(hook, logrus.WarnLevel))
	})

	t.Run("WARN is deduplicated per bucket-open", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepEmpty(logger)
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, true, false, rep, disk)

		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Equal(t, 1, countLogLevel(hook, logrus.WarnLevel), "second read must not emit a second WARN")
	})
}

// Pins weaviate/weaviate#12199: a deferred-marked bucket emits the
// disk-serving INFO once; unmarked buckets never do.
func TestRoaringSetRangeDeferredServingINFO(t *testing.T) {
	const value = uint64(5)

	t.Run("marked disk bucket: INFO once under repeated reads", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, false, true, nil, disk)

		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Equal(t, 1, countLogLevel(hook, logrus.InfoLevel), "INFO fires exactly once per bucket-open")
		assert.Contains(t, hook.LastEntry().Message, "restored automatically")
		assert.Zero(t, countLogLevel(hook, logrus.WarnLevel))
	})

	t.Run("unmarked knob-off bucket: no INFO", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, false, false, nil, disk)

		assert.Equal(t, []uint64{200}, readEqual(t, b, value))
		assert.Zero(t, countLogLevel(hook, logrus.InfoLevel))
	})

	t.Run("marker selects a log line only, never a read path", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		rep := newRangeableRepPopulated(t, logger, value, 100)
		disk := []Segment{newRangeableDiskSegment(value, 200)}
		b := newRangeableBucket(logger, true, true, rep, disk)

		assert.Equal(t, []uint64{100}, readEqual(t, b, value), "in-mem path served, not disk")
		assert.Zero(t, countLogLevel(hook, logrus.InfoLevel), "marker must not trigger the disk-path INFO")
		assert.Zero(t, countLogLevel(hook, logrus.WarnLevel))
	})
}

// BenchmarkRoaringSetRangeReaderPopulatedHotPath bounds the overhead added to
// the populated-rep hot path: one short-circuited IsEmpty() check.
func BenchmarkRoaringSetRangeReaderPopulatedHotPath(b *testing.B) {
	logger, _ := test.NewNullLogger()
	rep := roaringsetrange.NewSegmentInMemory(logger)
	mt := roaringsetrange.NewMemtable(logger)
	mt.Insert(5, []uint64{100})
	rep.MergeMemtableEventually(mt)
	for rep.IsUnpopulated() {
		time.Sleep(time.Millisecond)
	}
	bucket := newRangeableBucketForBench(logger, rep)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bucket.ReaderRoaringSetRange()
		r.Close()
	}
}

func newRangeableBucketForBench(logger logrus.FieldLogger, rep *roaringsetrange.SegmentInMemory) *Bucket {
	return &Bucket{
		strategy:             StrategyRoaringSetRange,
		keepSegmentsInMemory: true,
		logger:               logger,
		bitmapBufPool:        roaringset.NewBitmapBufPoolNoop(),
		active:               newTestMemtableRoaringSetRange(nil),
		disk:                 &SegmentGroup{logger: logger, roaringSetRangeSegmentInMemory: rep},
	}
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
