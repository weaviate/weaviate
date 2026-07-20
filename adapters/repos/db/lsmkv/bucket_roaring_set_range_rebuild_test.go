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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// readRangeEqual is a goroutine-safe readEqual: it returns an error instead
// of calling testify assertions, which aren't safe off the test goroutine.
func readRangeEqual(ctx context.Context, b *Bucket, value uint64) ([]uint64, error) {
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	v, release, err := reader.Read(ctx, value, filters.OperatorEqual)
	if err != nil {
		return nil, err
	}
	defer release()

	return v.ToArray(), nil
}

// TestSegmentGroupRoaringSetRangeRep_BuildThenCatchUp pins that a flush
// landing between the bulk-build snapshot and the install/publish step is
// still caught up into the rep, not lost.
func TestSegmentGroupRoaringSetRangeRep_BuildThenCatchUp(t *testing.T) {
	ctx := context.Background()
	key1, key2, key3 := uint64(1), uint64(2), uint64(3)

	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetRangeAdd(key1, 10))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetRangeAdd(key2, 20))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 2, b.disk.Len())

	rep, merged, release, err := b.disk.buildRoaringSetRangeRep(ctx)
	require.NoError(t, err)
	require.Len(t, merged, 2)
	require.Nil(t, b.disk.roaringSetRangeSegmentInMemory, "build alone must not publish")

	// Race window: a flush lands a new tail segment between build and install.
	require.NoError(t, b.RoaringSetRangeAdd(key3, 30))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 3, b.disk.Len())

	require.NoError(t, b.disk.installRoaringSetRangeRep(rep, merged, release))
	assert.Same(t, rep, b.disk.roaringSetRangeSegmentInMemory, "install must publish the built rep")

	b.rangeableRepRebuilt.Store(true)

	for key, wantDocID := range map[uint64]uint64{key1: 10, key2: 20, key3: 30} {
		got := readEqual(t, b, key)
		assert.Equal(t, []uint64{wantDocID}, got, "key %d must be present after catch-up", key)
	}
}

// TestSegmentGroupRoaringSetRangeRep_BuildRespectsContextCancel pins that a
// cancelled context aborts the bulk merge instead of running to completion.
func TestSegmentGroupRoaringSetRangeRep_BuildRespectsContextCancel(t *testing.T) {
	logger, _ := test.NewNullLogger()
	seg := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{1: roaringset.NewBitmap(1)}, sroar.NewBitmap())
	sg := &SegmentGroup{logger: logger, segments: []Segment{seg}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := sg.buildRoaringSetRangeRep(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestBucketRebuildRangeableSegmentInMemory_Correctness pins that rebuild
// results match the pre-rebuild disk read.
func TestBucketRebuildRangeableSegmentInMemory_Correctness(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	defer b.Shutdown(ctx)

	entries := [][2]uint64{{1, 100}, {2, 200}, {3, 300}}
	for _, kv := range entries {
		require.NoError(t, b.RoaringSetRangeAdd(kv[0], kv[1]))
		require.NoError(t, b.FlushAndSwitch())
	}
	require.Equal(t, 3, b.disk.Len(), "each add+flush must land its own segment")
	require.Nil(t, b.disk.roaringSetRangeSegmentInMemory)
	require.False(t, b.rangeableServesFromMemory())

	preRebuild := map[uint64][]uint64{}
	for _, kv := range entries {
		preRebuild[kv[0]] = readEqual(t, b, kv[0])
	}

	require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))

	assert.True(t, b.rangeableServesFromMemory())
	require.NotNil(t, b.disk.roaringSetRangeSegmentInMemory)
	assert.False(t, b.disk.roaringSetRangeSegmentInMemory.IsUnpopulated())

	for _, kv := range entries {
		assert.Equal(t, preRebuild[kv[0]], readEqual(t, b, kv[0]), "post-rebuild result must match pre-rebuild disk read")
	}
}

// TestBucketRebuildRangeableSegmentInMemory_Idempotent pins the two no-op
// cases: a repeated call, and a bucket already serving from memory.
func TestBucketRebuildRangeableSegmentInMemory_Idempotent(t *testing.T) {
	ctx := context.Background()

	t.Run("second call on an already-rebuilt bucket is a no-op", func(t *testing.T) {
		dir := t.TempDir()
		b := createTestBucketRoaringSetRange(t, ctx, dir, false)
		defer b.Shutdown(ctx)

		require.NoError(t, b.RoaringSetRangeAdd(1, 100))
		require.NoError(t, b.FlushAndSwitch())

		require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))
		repAfterFirst := b.disk.roaringSetRangeSegmentInMemory
		require.NotNil(t, repAfterFirst)

		require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))
		assert.Same(t, repAfterFirst, b.disk.roaringSetRangeSegmentInMemory, "second call must not rebuild")
	})

	t.Run("bucket opened with keepSegmentsInMemory=true no-ops", func(t *testing.T) {
		dir := t.TempDir()
		b := createTestBucketRoaringSetRange(t, ctx, dir, true)
		defer b.Shutdown(ctx)

		require.NoError(t, b.RoaringSetRangeAdd(1, 100))
		require.NoError(t, b.FlushAndSwitch())

		repAtOpen := b.disk.roaringSetRangeSegmentInMemory
		require.NotNil(t, repAtOpen)

		require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))
		assert.Same(t, repAtOpen, b.disk.roaringSetRangeSegmentInMemory)
		assert.False(t, b.rangeableRepRebuilt.Load(),
			"keepSegmentsInMemory alone accounts for serving; the rebuilt flag must stay false")
	})
}

// TestBucketRebuildRangeableSegmentInMemory_WrongStrategy pins that a
// non-roaring-set-range bucket returns an error instead of panicking.
func TestBucketRebuildRangeableSegmentInMemory_WrongStrategy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucket(t, ctx, dir, StrategyReplace)
	defer b.Shutdown(ctx)

	err := b.RebuildRangeableSegmentInMemory(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), StrategyRoaringSetRange)
}

// TestBucketRebuildRangeableSegmentInMemory_FlushAfterPublishMergesIntoRep
// pins that a flush completing after publish merges into the rep, not lost
// (regression: gating on keepSegmentsInMemory alone).
func TestBucketRebuildRangeableSegmentInMemory_FlushAfterPublishMergesIntoRep(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))
	require.True(t, b.rangeableServesFromMemory())

	require.NoError(t, b.RoaringSetRangeAdd(2, 200))
	require.NoError(t, b.FlushAndSwitch())

	assert.Equal(t, []uint64{200}, readEqual(t, b, 2), "flush completed after publish must merge into the rep")
	assert.Equal(t, []uint64{100}, readEqual(t, b, 1), "pre-existing rep content must survive the later flush")
}

// TestBucketRebuildRangeableSegmentInMemory_ConcurrentReadsAndWrites pins
// that no write is lost and no reader observes a torn result while
// concurrent with RebuildRangeableSegmentInMemory. Run with -race.
func TestBucketRebuildRangeableSegmentInMemory_ConcurrentReadsAndWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	defer b.Shutdown(ctx)

	const baselineCount = 40
	for i := uint64(1); i <= baselineCount; i++ {
		require.NoError(t, b.RoaringSetRangeAdd(i, i*10))
		if i%7 == 0 {
			require.NoError(t, b.FlushAndSwitch())
		}
	}
	require.NoError(t, b.FlushAndSwitch())
	require.Greater(t, b.disk.Len(), 1, "test needs multiple disk segments to be meaningful")

	const writerBase = uint64(10_000)
	const writerCount = 60

	writerDone := make(chan struct{})
	writerErrs := make(chan error, 1)
	go func() {
		defer close(writerDone)
		for i := uint64(1); i <= writerCount; i++ {
			if err := b.RoaringSetRangeAdd(writerBase+i, (writerBase+i)*10); err != nil {
				writerErrs <- fmt.Errorf("writer add: %w", err)
				return
			}
			if i%5 == 0 {
				if err := b.FlushAndSwitch(); err != nil {
					writerErrs <- fmt.Errorf("writer flush: %w", err)
					return
				}
			}
		}
	}()

	stopReaders := make(chan struct{})
	var readerWG sync.WaitGroup
	readerErrs := make(chan error, 4)
	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				got, err := readRangeEqual(ctx, b, 3) // known-flushed baseline value
				if err != nil {
					readerErrs <- fmt.Errorf("reader error: %w", err)
					return
				}
				if len(got) != 1 || got[0] != 30 {
					readerErrs <- fmt.Errorf("baseline value 3 read %v mid-rebuild, want [30]", got)
					return
				}
			}
		}()
	}

	require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))

	<-writerDone
	select {
	case err := <-writerErrs:
		t.Fatalf("writer goroutine failed: %v", err)
	default:
	}
	require.NoError(t, b.FlushAndSwitch(), "flush any writer data still in the active memtable")

	close(stopReaders)
	readerWG.Wait()
	close(readerErrs)
	for err := range readerErrs {
		t.Error(err)
	}

	assert.True(t, b.rangeableServesFromMemory())
	for i := uint64(1); i <= baselineCount; i++ {
		assert.Equal(t, []uint64{i * 10}, readEqual(t, b, i), "baseline value %d lost or corrupted", i)
	}
	for i := uint64(1); i <= writerCount; i++ {
		assert.Equal(t, []uint64{(writerBase + i) * 10}, readEqual(t, b, writerBase+i), "writer value %d lost or corrupted", i)
	}
}

// TestSegmentGroupRoaringSetRangeRep_ShutdownDuringRebuildBlocksNotPanics pins
// weaviate/weaviate#12215: shutdown must block on the rebuild's held segment
// refs, not race its merge. Run with -race.
func TestSegmentGroupRoaringSetRangeRep_ShutdownDuringRebuildBlocksNotPanics(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)

	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetRangeAdd(2, 200))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 2, b.disk.Len())

	rep, merged, release, err := b.disk.buildRoaringSetRangeRep(ctx)
	require.NoError(t, err)
	require.Len(t, merged, 2)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- b.Shutdown(ctx) }()

	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown completed while the rebuild still held segment refs (want: blocked): %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, b.disk.installRoaringSetRangeRep(rep, merged, release))

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete after the rebuild released its segment refs")
	}
}

// TestSegmentGroupRoaringSetRangeRep_InstallAfterGroupShutdownNoPanic pins
// weaviate/weaviate#12215: installRoaringSetRangeRep must not slice-bounds
// panic when segments were nil'd out (shutdown) between build and install.
func TestSegmentGroupRoaringSetRangeRep_InstallAfterGroupShutdownNoPanic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	t.Cleanup(func() { b.Shutdown(ctx) })

	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetRangeAdd(2, 200))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 2, b.disk.Len())

	rep, merged, release, err := b.disk.buildRoaringSetRangeRep(ctx)
	require.NoError(t, err)
	require.Len(t, merged, 2)

	// Simulate a concurrent shutdown() completing between build and
	// install: sg.segments = nil, exactly as SegmentGroup.shutdown does
	// under maintenanceLock.Lock() after waitForReferenceCountToReachZero.
	b.disk.maintenanceLock.Lock()
	b.disk.segments = nil
	b.disk.maintenanceLock.Unlock()

	require.NotPanics(t, func() {
		err = b.disk.installRoaringSetRangeRep(rep, merged, release)
	})
	require.NoError(t, err)
}

// alwaysRefuseAllocChecker refuses every allocation, simulating a
// memory-constrained node.
type alwaysRefuseAllocChecker struct{}

func (alwaysRefuseAllocChecker) CheckAlloc(sizeInBytes int64) error {
	return fmt.Errorf("simulated memory pressure: refusing %d bytes", sizeInBytes)
}

func (alwaysRefuseAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (alwaysRefuseAllocChecker) Refresh(updateMappings bool) {}

// TestSegmentGroupRoaringSetRangeRep_AllocCheckerRefusalSoftDegrades pins
// weaviate/weaviate#12215: an allocChecker refusal must be an ordinary
// error, not a panic, so the caller can soft-degrade to disk serving.
func TestSegmentGroupRoaringSetRangeRep_AllocCheckerRefusalSoftDegrades(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucketRoaringSetRange(t, ctx, dir, false)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 1, b.disk.Len())

	b.disk.allocChecker = alwaysRefuseAllocChecker{}

	_, merged, release, err := b.disk.buildRoaringSetRangeRep(ctx)
	require.Error(t, err, "an allocChecker refusal must be returned as an ordinary error")
	assert.Contains(t, err.Error(), "memory pressure")
	assert.Nil(t, merged)
	assert.Nil(t, release, "no view should be left held on refusal")

	require.Error(t, b.RebuildRangeableSegmentInMemory(ctx),
		"the refusal must propagate through the full rebuild call, not panic")
	assert.False(t, b.rangeableRepRebuilt.Load(), "a refused rebuild must not publish")
}

// TestRebuildRangeable_EmptyRepWithDiskSegments_DoesNotPublish pins
// weaviate/weaviate#12215: a rebuild whose rep folds empty while disk
// segments exist (e.g. all rows deleted) must not publish.
func TestRebuildRangeable_EmptyRepWithDiskSegments_DoesNotPublish(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, hook := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyRoaringSetRange),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithKeepSegmentsInMemory(false),
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	defer b.Shutdown(ctx)

	// Add then remove the same docID under the same value, flushed to
	// separate segments, so bitmaps[0] folds empty while disk segments
	// (tombstone-bearing) still exist - the all-deleted-property shape.
	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetRangeRemove(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.Equal(t, 2, b.disk.Len())

	err = b.RebuildRangeableSegmentInMemory(ctx)
	require.ErrorIs(t, err, ErrRangeableRepUnpopulatedAfterRebuild)
	assert.False(t, b.rangeableRepRebuilt.Load(), "an unpopulated rep with disk segments must not publish")

	require.Equal(t, 1, countLogLevel(hook, logrus.WarnLevel), "the disk-serving diagnostic must be logged")
	assert.Contains(t, hook.LastEntry().Message, "leaving disk-serving in place")

	assert.Empty(t, readEqual(t, b, 1), "disk-path read must still return the correct (empty) result")
}

// TestReaderRoaringSetRange_PublishedFlagTrusted pins weaviate/weaviate#12215:
// once published (rangeableRepRebuilt==true), reads must trust the rep and
// never re-run the per-read unpopulated+disk-segments fallback check.
func TestReaderRoaringSetRange_PublishedFlagTrusted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, hook := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyRoaringSetRange),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithKeepSegmentsInMemory(false),
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetRangeAdd(1, 100))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RebuildRangeableSegmentInMemory(ctx))
	require.True(t, b.rangeableRepRebuilt.Load())

	for range 3 {
		assert.Equal(t, []uint64{100}, readEqual(t, b, 1))
	}
	assert.Zero(t, countLogLevel(hook, logrus.WarnLevel),
		"a validated, published rep must never trigger the per-read fallback WARN")
}
