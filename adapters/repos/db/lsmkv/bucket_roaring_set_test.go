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
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/concurrency/testinghelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestRoaringSetWritePathRefCount ensures that all write paths of the
// RoaringSet type correctly use and release refcounts on the active memtable
// and therefore do not block a flushlock for the entire duration of the wrige.
func TestRoaringSetWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableRoaringSet(nil),
		logger:   nullLogger(),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}

	// add one
	err := b.RoaringSetAddOne([]byte("key1"), 1)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add list
	err = b.RoaringSetAddList([]byte("key1"), []uint64{2, 3, 4})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add bitmap
	err = b.RoaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{5, 6, 7}))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// remove one
	err = b.RoaringSetRemoveOne([]byte("key1"), 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, releaseBufPol, err := b.RoaringSetGet(context.Background(), []byte("key1"))
	defer releaseBufPol()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, v.ToArray())
}

// TestBucket_RoaringSetGetFromConsistentView_MemtableErrorLeavesTheDiskRowUnread
// pins that a failing memtable read cannot leak the disk row's pooled buffer:
// memtables are read first, so the buffer is never acquired at all.
func TestBucket_RoaringSetGetFromConsistentView_MemtableErrorLeavesTheDiskRowUnread(t *testing.T) {
	t.Parallel()

	readErr := errors.New("simulated memtable read error")

	newDiskSeg := func() *fakeSegment {
		return newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"key1": bitmapFromSlice([]uint64{1, 2, 3}),
		})
	}

	t.Run("active read error leaves the disk row unread, returns caller-safe noop", func(t *testing.T) {
		diskSeg := newDiskSeg()
		active := newTestMemtableRoaringSet(nil)
		active.roaringSetGetErr = readErr

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
			logger:   nullLogger(),
		}
		view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(view, []byte("key1"), concurrency.SROAR_MERGE)
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.NotNil(t, release)

		require.Equal(t, 0, diskSeg.getCounter,
			"the disk row must not be read once the active read has failed")
		require.Equal(t, 0, diskSeg.roaringSetReleases,
			"nothing was acquired, so nothing needs releasing")

		release()
		require.Equal(t, 0, diskSeg.roaringSetReleases,
			"returned release must be a noop")
	})

	t.Run("flushing read error leaves the disk row unread", func(t *testing.T) {
		diskSeg := newDiskSeg()
		flushing := newTestMemtableRoaringSet(nil)
		flushing.roaringSetGetErr = readErr
		active := newTestMemtableRoaringSet(nil)

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
			logger:   nullLogger(),
		}
		view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(view, []byte("key1"), concurrency.SROAR_MERGE)
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.Equal(t, 0, diskSeg.getCounter,
			"the disk row must not be read once the flushing read has failed")
		require.Equal(t, 0, diskSeg.roaringSetReleases,
			"nothing was acquired, so nothing needs releasing")

		release()
		require.Equal(t, 0, diskSeg.roaringSetReleases)
	})

	t.Run("success path defers nothing, caller owns the release", func(t *testing.T) {
		diskSeg := newDiskSeg()
		active := newTestMemtableRoaringSet(nil)

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
			logger:   nullLogger(),
		}
		view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(view, []byte("key1"), concurrency.SROAR_MERGE)
		require.NoError(t, err)
		require.NotNil(t, bm)

		require.Equal(t, 0, diskSeg.roaringSetReleases,
			"success path must not release before the caller does")

		release()
		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"caller's release must free the disk layer exactly once")
	})
}

// TestBucket_RoaringSetGet_RespectsConcurrencyBudget pins RoaringSetGet's
// merge fan-out to the per-query budget without blowing the goroutine ceiling.
func TestBucket_RoaringSetGet_RespectsConcurrencyBudget(t *testing.T) {
	// Kill switch is this bound's red control, but no CI job sets it: CI
	// only ever runs the green (budget-enforced) path.
	if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
		t.Skip("budget cap disabled via kill switch")
	}
	// Merge fan-out only exists at SROAR_MERGE>=2 (GOMAXPROCS>=4); skipping
	// here silently would hide the guard, so CI fails loudly instead.
	if concurrency.SROAR_MERGE < 2 {
		if os.Getenv("CI") != "" {
			t.Fatalf("bounding tests require GOMAXPROCS>=4, refusing to skip silently on CI (SROAR_MERGE=%d)",
				concurrency.SROAR_MERGE)
		}
		t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
			concurrency.SROAR_MERGE)
	}

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	// never auto-flush; we flush explicitly to control the disk segment count
	b.SetMemtableThreshold(1e9)

	// numContainers keeps worker count (min(numContainers/24, SROAR_MERGE))
	// above 1; dense containers keep an ignored-budget worker alive across
	// sampler ticks.
	const (
		numContainers      = 128
		valuesPerContainer = 128
	)
	values := make([]uint64, 0, numContainers*valuesPerContainer)
	for c := 0; c < numContainers; c++ {
		for j := 0; j < valuesPerContainer; j++ {
			values = append(values, uint64(c)<<16+uint64(j))
		}
	}

	key := []byte("key")

	// many disk segments keep each Get's merges live across the sampler window
	const numSegments = 12
	for s := 0; s < numSegments; s++ {
		require.NoError(t, b.RoaringSetAddList(key, values))
		require.NoError(t, b.FlushAndSwitch())
	}

	budget1 := concurrency.CtxWithBudget(ctx, 1)

	// correctness: budget=1 result must match the unconstrained query
	got1, release1, err := b.RoaringSetGet(budget1, key)
	require.NoError(t, err)
	arr1 := got1.ToArray()
	release1()

	gotDefault, releaseDefault, err := b.RoaringSetGet(ctx, key)
	require.NoError(t, err)
	arrDefault := gotDefault.ToArray()
	releaseDefault()

	require.Equal(t, values, arr1)
	require.Equal(t, arrDefault, arr1)

	// budget=1 spawns no extra workers; slack absorbs sampler/GC noise
	testinghelpers.AssertGoroutineCeiling(t, 24, 1, 8, 200*time.Millisecond, func() error {
		bm, release, err := b.RoaringSetGet(budget1, key)
		if err != nil {
			return err
		}
		_ = bm
		release()
		return nil
	})
}

// TestBatchReaderMatchesRoaringSetGet proves the batched-read
// primitive returns byte-identical results to the per-key RoaringSetGet entry
// point, whether the key is present across several on-disk segments plus the
// active memtable, or absent entirely, and that one reader serves every read.
// The active memtable holds unflushed data when the reader is built, so the
// batch reads it too and parity is exact.
func TestBatchReaderMatchesRoaringSetGet(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	// never auto-flush; flush explicitly so the key spans several disk
	// segments plus a final write left in the active memtable
	b.SetMemtableThreshold(1e9)

	keyA := []byte("key-a")
	keyB := []byte("key-b")
	keyMissing := []byte("key-missing")

	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{1, 2, 3}))
	require.NoError(t, b.RoaringSetAddList(keyB, []uint64{10, 20}))
	require.NoError(t, b.FlushAndSwitch())

	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{4, 5}))
	require.NoError(t, b.FlushAndSwitch())

	// left in the active memtable, unflushed
	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{6}))

	wantA, releaseWantA, err := b.RoaringSetGet(ctx, keyA)
	require.NoError(t, err)
	arrWantA := append([]uint64(nil), wantA.ToArray()...)
	releaseWantA()

	wantB, releaseWantB, err := b.RoaringSetGet(ctx, keyB)
	require.NoError(t, err)
	arrWantB := append([]uint64(nil), wantB.ToArray()...)
	releaseWantB()

	wantMissing, releaseWantMissing, err := b.RoaringSetGet(ctx, keyMissing)
	require.NoError(t, err)
	arrWantMissing := append([]uint64(nil), wantMissing.ToArray()...)
	releaseWantMissing()
	require.Empty(t, arrWantMissing, "absent key must resolve to an empty, non-nil bitmap")

	// one reader serves all three reads below
	// one reader serves the whole batch, in the order it sorts into
	keys := sortedKeysOf(t, []string{string(keyA), string(keyB), string(keyMissing)})
	view := b.GetConsistentView()
	defer view.ReleaseView()
	reader, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), keys)
	require.NoError(t, err)

	requireRowsAre(t, reader, keys, map[string][]uint64{
		string(keyA):       arrWantA,
		string(keyB):       arrWantB,
		string(keyMissing): nil,
	})
}

// TestBatchReaderSurvivesFlushAndSwitch pins the view stability a whole-batch
// reader relies on: a flush landing mid-fold must not disturb keys still to be
// read. The second key's window fills after the switch, through a memtable the
// bucket has already moved past, and must still answer the pre-switch state —
// a fresh RoaringSetGet, by contrast, sees the post-switch write. (Writes
// before the switch still land in the old active memtable the view
// references, so only post-switch invisibility is asserted.)
func TestBatchReaderSurvivesFlushAndSwitch(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // flush only via the explicit switch below

	first, second := []byte("key1"), []byte("key2")

	// one disk segment plus unflushed state in the active memtable, so the
	// view references both layer kinds when taken
	for _, k := range [][]byte{first, second} {
		require.NoError(t, b.RoaringSetAddList(k, []uint64{1, 2, 3}))
	}
	require.NoError(t, b.FlushAndSwitch())
	for _, k := range [][]byte{first, second} {
		require.NoError(t, b.RoaringSetAddList(k, []uint64{4}))
	}

	keys := sortedKeysOf(t, []string{string(first), string(second)})
	view := b.GetConsistentView()
	defer view.ReleaseView()
	// One key per window, so the second key's window fills after the switch
	// below, reading a memtable the bucket has already moved past.
	reader, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, 1, math.MaxInt)
	require.NoError(t, err)

	before, releaseBefore, err := reader.Next(concurrency.SROAR_MERGE)
	require.NoError(t, err)
	arrBefore := append([]uint64(nil), before.ToArray()...)
	releaseBefore()
	require.Equal(t, []uint64{1, 2, 3, 4}, arrBefore)

	// flush the memtable the view references, then write past the view
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetAddList(second, []uint64{5}))

	after, releaseAfter, err := reader.Next(concurrency.SROAR_MERGE)
	require.NoError(t, err)
	require.Equal(t, arrBefore, after.ToArray(),
		"a window filled after the switch must still read the pre-switch state")
	releaseAfter()

	fresh, releaseFresh, err := b.RoaringSetGet(ctx, second)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, fresh.ToArray(),
		"a fresh read must see the post-switch write the view cannot")
	releaseFresh()
}

// TestBucket_RoaringSet_DeleteThenReaddAcrossSegments is a read-path regression
// test for roaringset reads with tombstones spread across multiple disk segments
// plus the active memtable, including a doc deleted in one segment and re-added
// in a later layer. The first (oldest) segment carries no tombstones, so it
// takes the empty-deletions path in segment.roaringSetGet; the test pins that
// the full add / delete / re-add fold still resolves correctly. (Note: Flatten
// ignores the base layer's Deletions, so this exercises the surrounding fold
// rather than the base layer's deletions bitmap directly.)
func TestBucket_RoaringSet_DeleteThenReaddAcrossSegments(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // flush explicitly so writes span several segments

	tombstoned := []byte("tombstoned")
	clean := []byte("clean")

	// segment 1 (oldest): additions only -> empty deletions -> shared empty path
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{1, 2, 3, 4, 5}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{100, 101}))
	require.NoError(t, b.FlushAndSwitch())

	// segment 2: real deletions of 2 and 4, plus addition 6
	require.NoError(t, b.RoaringSetRemoveOne(tombstoned, 2))
	require.NoError(t, b.RoaringSetRemoveOne(tombstoned, 4))
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{6}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{102}))
	require.NoError(t, b.FlushAndSwitch())

	// active memtable (unflushed): re-add 2 (deleted in segment 2) and add 7
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{2, 7}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{103}))

	// {1,2,3,4,5} -(del 2,4)-> {1,3,5} +6 -> {1,3,5,6}; re-add {2,7} -> {1,2,3,5,6,7}
	// (doc 2, deleted in segment 2, must survive because it is re-added later)
	requireRoaringSetElements(t, ctx, b, tombstoned, []uint64{1, 2, 3, 5, 6, 7})
	// pure additions across every layer -> every layer takes the empty path
	requireRoaringSetElements(t, ctx, b, clean, []uint64{100, 101, 102, 103})
}

// TestBucket_RoaringSet_DeletionsOnlyOldestSegment pins the empty-additions
// base across multiple disk segments: the oldest segment holding a key
// becomes the mutable accumulator base of the disk fold, so the segment read
// must substitute a non-nil additions bitmap when the node stores only
// deletions — the newer segment's merge runs AndNot/Or directly on it.
// Covers both segment read modes, whose branches both return nil additions
// for the empty region.
func TestBucket_RoaringSet_DeletionsOnlyOldestSegment(t *testing.T) {
	tests := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()

			opts := append([]BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			}, tt.opts...)
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

			b.SetMemtableThreshold(1e9) // flush explicitly so writes span several segments

			key := []byte("key")

			// segment 1 (oldest): deletions only -> empty additions region
			require.NoError(t, b.RoaringSetRemoveOne(key, 7))
			require.NoError(t, b.FlushAndSwitch())

			// segment 2: additions merged into the substituted base (the Or arm)
			require.NoError(t, b.RoaringSetAddList(key, []uint64{1, 2, 3}))
			require.NoError(t, b.FlushAndSwitch())

			// segment 3: deletes 2, added by segment 2, so the AndNot arm's
			// effect on the accumulated base is visible in the result
			require.NoError(t, b.RoaringSetRemoveOne(key, 2))
			require.NoError(t, b.FlushAndSwitch())

			requireRoaringSetElements(t, ctx, b, key, []uint64{1, 3})
		})
	}
}

// TestBucket_RoaringSet_ReleasesAllPooledBuffers pins two contracts of the
// disk read: the buffers a node's read pools must all be freed by the single
// returned release, in either read mode (mmap pools the additions clone, a
// pread read pools the whole node buffer), and the oldest segment's own
// deletions must never alter the result — the deletions-carrying scenarios
// exercise base nodes whose deletions the read deliberately skips.
func TestBucket_RoaringSet_ReleasesAllPooledBuffers(t *testing.T) {
	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true)}},
	}

	// The oldest segment's shape is what matters, because only the oldest
	// segment holding the key is read through segment.roaringSetGet, which
	// pools the buffer the returned release must free — newer segments are
	// folded via roaringSetMergeWith, which pools nothing lasting.
	scenarios := []struct {
		name     string
		seg1     func(t *testing.T, b *Bucket, key []byte)
		expected []uint64
	}{
		{
			name: "oldest segment has additions and deletions",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{2}))
				require.NoError(t, b.RoaringSetRemoveOne(key, 5))
			},
			expected: []uint64{1, 2, 3},
		},
		{
			name: "oldest segment has additions only",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{2}))
			},
			expected: []uint64{1, 2, 3},
		},
		{
			name: "oldest segment has deletions only",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetRemoveOne(key, 5))
			},
			expected: []uint64{1, 3},
		},
	}

	for _, mode := range modes {
		for _, scenario := range scenarios {
			t.Run(mode.name+"/"+scenario.name, func(t *testing.T) {
				ctx := context.Background()
				logger, _ := test.NewNullLogger()
				pool := roaringset.NewBitmapBufPoolTrackingForTests()

				opts := append([]BucketOption{
					WithStrategy(StrategyRoaringSet),
					WithBitmapBufPool(pool),
				}, mode.opts...)
				b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

				b.SetMemtableThreshold(1e9)

				key := []byte("key")
				scenario.seg1(t, b, key)
				require.NoError(t, b.FlushAndSwitch())

				require.NoError(t, b.RoaringSetAddList(key, []uint64{1, 3}))
				require.NoError(t, b.FlushAndSwitch())

				bm, release, err := b.RoaringSetGet(ctx, key)
				require.NoError(t, err)
				require.ElementsMatch(t, scenario.expected, bm.ToArray())
				release()

				require.Zero(t, pool.Outstanding(), "single release must free every pooled buffer")
			})
		}
	}
}

func requireRoaringSetElements(t *testing.T, ctx context.Context, b *Bucket, key []byte, want []uint64) {
	t.Helper()
	bm, release, err := b.RoaringSetGet(ctx, key)
	require.NoError(t, err)
	defer release()
	require.ElementsMatch(t, want, bm.ToArray())
}

// TestBatchReaderValidatesAndFoldsLikeThePerKeyPath pins the batch reader's contract: it validates the
// strategy once, and it folds exactly the layers a per-key read would — except
// that an active memtable empty at view time is skipped for the whole batch,
// the behavior the reader exists for.
func TestBatchReaderValidatesAndFoldsLikeThePerKeyPath(t *testing.T) {
	t.Parallel()

	t.Run("wrong strategy is refused", func(t *testing.T) {
		b := Bucket{
			strategy: StrategyReplace,
			disk:     &SegmentGroup{},
			logger:   nullLogger(),
		}
		view := b.GetConsistentView()
		defer view.ReleaseView()

		reader, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, []string{"k"}))
		require.Error(t, err)
		require.Nil(t, reader)
	})

	// The eight presence combinations of the three layers — a new case belongs
	// in one of these rows, not beside them. The flushing rows are the only
	// thing pinning that a flushing memtable is always probed; the disk-less
	// rows exercise the merger's adopt-the-first-memtable-layer branch, which a
	// disk row would hide. Every row is a single container, so sroar merges it
	// sequentially whatever mergeConc says: this table pins which layers are
	// folded, never how the fold fans out.
	layerTests := []struct {
		name     string
		disk     []uint64 // nil: key absent from the disk segment
		flushing []uint64 // nil: no flushing memtable at all
		active   []uint64 // nil: empty active memtable, so it gets skipped
		// flushingDeletes are tombstoned by the flushing memtable. The merger
		// replays each layer's deletions before its additions, so a doc deleted
		// here and re-added by the (newer) active memtable survives only if the
		// layers are folded oldest first.
		flushingDeletes []uint64
		// activeOtherKey seeds the active memtable under a different key, so it
		// is non-empty (and therefore probed) while missing the key under test
		activeOtherKey bool
		want           []uint64
	}{
		{name: "disk only", disk: []uint64{1, 2}, want: []uint64{1, 2}},
		{name: "disk and active", disk: []uint64{1, 2}, active: []uint64{3}, want: []uint64{1, 2, 3}},
		{name: "disk and flushing, active skipped", disk: []uint64{1, 2}, flushing: []uint64{3}, want: []uint64{1, 2, 3}},
		{
			name: "all three layers", disk: []uint64{1}, flushing: []uint64{2}, active: []uint64{3},
			want: []uint64{1, 2, 3},
		},
		{name: "flushing only", flushing: []uint64{7, 8}, want: []uint64{7, 8}},
		{name: "active only", active: []uint64{7, 8}, want: []uint64{7, 8}},
		{name: "flushing and active, no disk row", flushing: []uint64{7}, active: []uint64{8}, want: []uint64{7, 8}},
		{name: "key absent from every layer", want: []uint64{}},
		// the active memtable holds data, so it is probed rather than skipped,
		// but not this key — the layer's NotFound branch, which every row above
		// either skips outright or answers
		{name: "active probed, holds another key", disk: []uint64{1, 2}, activeOtherKey: true, want: []uint64{1, 2}},
		// Layer order: the flushing memtable's tombstone is older than the
		// active memtable's re-add, so the doc must survive. Folding newest
		// first would apply the delete last and silently drop it.
		{
			name: "flushing deletes what active re-adds",
			disk: []uint64{1, 2, 3}, flushingDeletes: []uint64{2}, active: []uint64{2},
			want: []uint64{1, 2, 3},
		},
		// The same tombstone with nothing re-adding it must still take effect.
		{
			name: "flushing deletes, active untouched",
			disk: []uint64{1, 2, 3}, flushingDeletes: []uint64{2}, active: []uint64{9},
			want: []uint64{1, 3, 9},
		},
	}
	for _, tc := range layerTests {
		t.Run(tc.name, func(t *testing.T) {
			diskRows := map[string]*sroar.Bitmap{}
			if tc.disk != nil {
				diskRows["k"] = bitmapFromSlice(tc.disk)
			}
			b := Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: []Segment{newFakeRoaringSetSegment(diskRows)}},
				active:   newTestMemtableRoaringSet(rowOrNil(tc.active)),
				logger:   nullLogger(),
			}
			if tc.activeOtherKey {
				b.active = newTestMemtableRoaringSet(map[string][]uint64{"other": {42}})
			}
			if tc.flushing != nil {
				b.flushing = newTestMemtableRoaringSet(map[string][]uint64{"k": tc.flushing})
			}
			if tc.flushingDeletes != nil {
				// Through roaringSetRemoveList, not straight into the tree, so size
				// tracks what it holds — the reader skips a memtable reporting 0.
				mt := newTestMemtableRoaringSet(nil)
				require.NoError(t, mt.roaringSetRemoveList([]byte("k"), tc.flushingDeletes))
				b.flushing = mt
			}

			view := b.GetConsistentView()
			defer view.ReleaseView()
			reader, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, []string{"k"}))
			require.NoError(t, err)

			bm, release, err := reader.Next(concurrency.SROAR_MERGE)
			require.NoError(t, err)
			defer release()
			require.Equal(t, tc.want, bm.ToArray())
		})
	}

	t.Run("write into a then-empty active memtable is invisible to the batch", func(t *testing.T) {
		b := Bucket{
			strategy: StrategyRoaringSet,
			disk: &SegmentGroup{segments: []Segment{newFakeRoaringSetSegment(
				map[string]*sroar.Bitmap{"k": bitmapFromSlice([]uint64{1, 2})})}},
			active: newTestMemtableRoaringSet(nil), // empty when the view is taken
			logger: nullLogger(),
		}

		view := b.GetConsistentView()
		defer view.ReleaseView()
		reader, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, []string{"k"}))
		require.NoError(t, err)

		// a racing write into the active memtable the reader snapshotted as
		// empty, through the same path a real writer takes so the size
		// accounting is exercised too
		require.NoError(t, b.RoaringSetAddList([]byte("k"), []uint64{99}))

		bm, release, err := reader.Next(1)
		require.NoError(t, err)
		defer release()
		require.Equal(t, []uint64{1, 2}, bm.ToArray(),
			"a write into the then-empty active memtable must be skipped for the whole batch")
	})
}

// rowOrNil maps a nil slice to a nil map, so newTestMemtableRoaringSet builds
// an empty memtable rather than one holding an empty row.
func rowOrNil(docIDs []uint64) map[string][]uint64 {
	if docIDs == nil {
		return nil
	}
	return map[string][]uint64{"k": docIDs}
}

// TestBatchReaderFoldsActiveTombstones pins the invariant the
// empty-active-memtable skip rests on: a delete makes the active memtable
// non-empty, so the batch still reads it. If deletions ever stopped counting
// towards Memtable.Size(), the skip would silently resurrect every deleted doc
// — a wrong-results bug with no error anywhere, and the read-path tests that
// use RoaringSetGet would not catch it.
func TestBatchReaderFoldsActiveTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // flush only where the test says so

	key := []byte("k")
	require.NoError(t, b.RoaringSetAddList(key, []uint64{1, 2, 3}))
	require.NoError(t, b.FlushAndSwitch()) // additions land on disk

	// the tombstone stays in the otherwise-empty active memtable
	require.NoError(t, b.RoaringSetRemoveOne(key, 2))

	view := b.GetConsistentView()
	defer view.ReleaseView()
	reader, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, []string{string(key)}))
	require.NoError(t, err)

	bm, release, err := reader.Next(1)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []uint64{1, 3}, bm.ToArray(),
		"a tombstone-only active memtable must not be treated as empty")
}

// completeParkedFlush finishes the flush b.flushing holds, so Bucket.Shutdown
// can return. The slot is cleared whatever happens, because Shutdown waits on
// it with no bound when its context is context.Background().
func completeParkedFlush(t *testing.T, b *Bucket) {
	t.Helper()
	if b.flushing == nil {
		return
	}
	defer func() { b.flushing = nil }()
	b.waitForZeroWriters(b.flushing)
	segmentPath, err := b.flushing.flush()
	require.NoError(t, err)
	segment, err := b.disk.initAndPrecomputeNewSegment(segmentPath)
	require.NoError(t, err)
	require.NoError(t, b.atomicallyAddDiskSegmentAndRemoveFlushing(segment))
	require.Nil(t, b.flushing, "the segment add must clear the flushing slot")
}

// TestBatchReaderFoldsARealSwitchOldestFirst pins the layer order against a
// bucket that performed the switch itself. Every other two-memtable test puts a
// memtable in b.flushing and another in b.active by hand — some of them behind
// a real GetConsistentView — so the test is what decides which one is older.
// Here the switch decides, which is what viewMemtablesOldestFirst assumes: that
// b.flushing predates b.active.
//
// The fixture is a document deleted in the switched-out memtable and re-added
// in the new active one. Folded oldest first it survives; reversed, the
// deletion lands last and it disappears. An additions-only fixture answers the
// same either way, which is why one is not enough here.
func TestBatchReaderFoldsARealSwitchOldestFirst(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9) // switch only where this test says so

	// Registered after the Shutdown cleanup so it runs before it, and above
	// b.FlushAndSwitch so a failure there is covered too. The Shutdown cleanup
	// passes context.Background(), so Shutdown's wait on b.flushing never ends.
	t.Cleanup(func() { completeParkedFlush(t, b) })

	key := []byte("k")
	require.NoError(t, b.RoaringSetAddList(key, []uint64{7}))
	require.NoError(t, b.FlushAndSwitch()) // 7 is now on disk

	require.NoError(t, b.RoaringSetRemoveOne(key, 7))
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched, "the delete must land in a memtable the switch moves")

	require.NoError(t, b.RoaringSetAddList(key, []uint64{7}))

	view := b.GetConsistentView()
	defer view.ReleaseView()
	require.NotNil(t, view.Flushing, "without a flushing memtable there are no two orders to tell apart")

	keys := sortedKeysOf(t, []string{string(key)})
	r, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
	require.NoError(t, err)
	got, release, err := r.Next(concurrency.SROAR_MERGE)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []uint64{7}, docsOrNil(got),
		"the re-add in active must outlive the delete in flushing")
}

// TestBatchReaderLeavesNoSegmentRefBehind pins the reader's half of the
// ownership split: it borrows the view's segments and releases nothing, so the
// single GetConsistentView/ReleaseView pair around a fold is what has to
// balance. Nothing inside the reader guards against a view already released,
// which is why the balance is asserted from outside it.
func TestBatchReaderLeavesNoSegmentRefBehind(t *testing.T) {
	readErr := errors.New("injected memtable failure")

	tests := []struct {
		name string
		// failing swaps in an active memtable that errors, so the fold stops on
		// an error rather than reaching the end of the batch. The balance has to
		// hold either way, and only the drained half was covered.
		failing bool
	}{
		{name: "the batch is drained"},
		{name: "a read fails part way", failing: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()

			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
			// Registered after the Shutdown cleanup so it runs before it, and above
			// b.FlushAndSwitch so a failure there is covered too. The Shutdown cleanup
			// passes context.Background(), so Shutdown's wait on b.flushing never ends.
			t.Cleanup(func() { completeParkedFlush(t, b) })

			names := []string{"aaa", "bbb", "ccc"}
			for i, n := range names {
				require.NoError(t, b.RoaringSetAddList([]byte(n), []uint64{uint64(i)}))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.Equal(t, 1, b.disk.Len(), "the fold must have a real segment to hold a ref on")

			seg := b.disk.segments[0]
			// Registered after the Shutdown cleanup so it runs before it, and above the
			// assertions below: any failure among them can leave a ref outstanding, and
			// SegmentGroup.waitForReferenceCountToReachZero waits on it with no context.
			t.Cleanup(func() {
				for n := seg.getRefs(); n > 0; n-- {
					seg.decRef()
				}
			})
			require.Zero(t, seg.getRefs(), "precondition: no view held yet")

			view := b.GetConsistentView()
			require.Positive(t, seg.getRefs(), "the view must hold the segment while the fold runs")

			if tc.failing {
				// The flush left the real active memtable empty, and the reader
				// drops an empty one, so a memtable that can fail has to be put
				// back before there is a read to fail.
				active := newTestMemtableRoaringSet(map[string][]uint64{"aaa": {0}})
				active.roaringSetGetWindowErr = readErr
				view.Active = active
			}

			keys := sortedKeysOf(t, names)
			r, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
			require.NoError(t, err)

			if tc.failing {
				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.ErrorIs(t, err, readErr)
				require.Nil(t, release, "a failed read returns nothing to release")
			} else {
				for range names {
					_, release, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					release()
				}
			}
			require.Positive(t, seg.getRefs(), "the reader must not release what it borrowed")

			view.ReleaseView()
			require.Zero(t, seg.getRefs(), "one view, one release: the fold must leave the segment as it found it")
		})
	}
}

// TestRoaringSetCursorSeekOnDiskSegment seeks a real flushed segment, the only
// thing that exercises SeekPayloadStart's rebase onto the payload slice; an
// offset wrong by HeaderSize would abort in the bitmap read, not silently
// return a neighbour's key.
func TestRoaringSetCursorSeekOnDiskSegment(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9)

	// keys leave gaps, so a probe can land between two of them
	keys := []string{"key-02", "key-04", "key-06", "key-08"}
	for i, key := range keys {
		require.NoError(t, b.RoaringSetAddList([]byte(key), []uint64{uint64(i) + 1}))
	}
	// everything must live on disk: a memtable hit would not reach the seeker
	require.NoError(t, b.FlushAndSwitch())

	tests := []struct {
		name     string
		seek     string
		wantKey  string
		wantNone bool
	}{
		{name: "exact match on the first key", seek: "key-02", wantKey: "key-02"},
		{name: "exact match mid-tree", seek: "key-06", wantKey: "key-06"},
		{name: "between two keys", seek: "key-05", wantKey: "key-06"},
		{name: "below the smallest key", seek: "key-00", wantKey: "key-02"},
		{name: "exact match on the last key", seek: "key-08", wantKey: "key-08"},
		{name: "past the highest key", seek: "key-99", wantNone: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := b.CursorRoaringSet()
			defer c.Close()

			key, bm := c.Seek([]byte(test.seek))
			if test.wantNone {
				require.Nil(t, key)
				return
			}

			require.Equal(t, test.wantKey, string(key))
			// the payload has to be the one belonging to that key, which is what a
			// mis-rebased offset would get wrong
			want := uint64(slices.Index(keys, test.wantKey)) + 1
			require.Equal(t, []uint64{want}, bm.ToArray())
		})
	}

	t.Run("seek then walk to the end", func(t *testing.T) {
		c := b.CursorRoaringSet()
		defer c.Close()

		var seen []string
		for key, _ := c.Seek([]byte("key-05")); key != nil; key, _ = c.Next() {
			seen = append(seen, string(key))
		}
		require.Equal(t, []string{"key-06", "key-08"}, seen)
	})
}

// TestBatchReadersShareOneViewIndependently pins what a parallel fold is built
// on: several readers on the SAME held view, each over its own share of one
// batch, walking at once. Each must read its share exactly as a single reader
// over the whole batch does — two windows filling at the same time must not
// hand each other's rows back.
func TestBatchReadersShareOneViewIndependently(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9) // flush explicitly, so the split below holds

	const numKeys = 64
	names := make([]string, numKeys)
	for i := range names {
		names[i] = fmt.Sprintf("key-%03d", i)
		require.NoError(t, b.RoaringSetAddList([]byte(names[i]), []uint64{uint64(i), uint64(100 + i)}))
		if i == numKeys/2 {
			// leave the rest in the active memtable, so the readers walk both a
			// segment and the windowed memtable path
			require.NoError(t, b.FlushAndSwitch())
		}
	}
	keys := sortedKeysOf(t, names)

	view := b.GetConsistentView()
	defer view.ReleaseView()

	// what one reader over the whole batch reads, in batch order
	whole, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, 4, BatchReaderWindowBytes)
	require.NoError(t, err)
	want := make(map[string][]uint64, keys.Len())
	for i := 0; i < keys.Len(); i++ {
		bm, release, err := whole.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		want[string(keys.At(i))] = docsOrNil(bm)
		release()
	}

	const workers = 4
	got := make([]map[string][]uint64, workers)
	readers := make([]*RoaringSetBatchReader, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		from, to := w*numKeys/workers, (w+1)*numKeys/workers
		share := keys.Sub(from, to)
		// opened before the goroutines start, as the fold does
		reader, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), share, 4, BatchReaderWindowBytes)
		require.NoError(t, err)
		readers[w] = reader

		wg.Add(1)
		go func() {
			defer wg.Done()
			rows := make(map[string][]uint64, share.Len())
			for i := 0; i < share.Len(); i++ {
				bm, release, err := reader.Next(concurrency.SROAR_MERGE)
				if !assert.NoError(t, err) {
					return
				}
				rows[string(share.At(i))] = docsOrNil(bm)
				release()
			}
			got[w] = rows
		}()
	}
	wg.Wait()

	merged := map[string][]uint64{}
	for w, rows := range got {
		require.NotNil(t, rows, "worker %d did not finish its share", w)
		for k, v := range rows {
			_, seen := merged[k]
			require.False(t, seen, "key %q was read by more than one worker", k)
			merged[k] = v
		}
	}
	require.Equal(t, want, merged,
		"readers sharing a view must read their shares as one reader reads the batch")

	// without this the test could pass on a fixture where each share fits one
	// window, which is the shape that has no concurrent fill to get wrong
	for w, reader := range readers {
		st := reader.Stats()
		require.Greater(t, st.Fills, 1, "worker %d must span more than one window", w)
		require.Positive(t, st.Memtables, "worker %d must read through a memtable", w)
	}
}

// TestBatchReadersOnOneViewAgreeOnMemtables pins what narrowing a view once
// buys: whether the active memtable participates is decided for every reader at
// one instant. The size is read live, so deciding per reader would let a write
// landing between two opens give one batch two answers — which is what the
// unnarrowed case at the end shows.
func TestBatchReadersOnOneViewAgreeOnMemtables(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9)

	require.NoError(t, b.RoaringSetAddList([]byte("k"), []uint64{1}))
	// everything on disk, so the active memtable is empty and a reader resolving
	// for itself would drop it
	require.NoError(t, b.FlushAndSwitch())

	view := b.GetConsistentView()
	defer view.ReleaseView()
	narrowed := view.WithoutEmptyActiveMemtable()
	require.NotNil(t, view.Active, "narrowing must leave the caller's own view alone")
	require.Nil(t, narrowed.view.Active, "an untouched active memtable contributes nothing")

	// the write the readers straddle
	require.NoError(t, b.RoaringSetAddList([]byte("k"), []uint64{2}))

	keys := sortedKeysOf(t, []string{"k"})
	read := func(t *testing.T, r *RoaringSetBatchReader) []uint64 {
		t.Helper()
		bm, release, err := r.Next(1)
		require.NoError(t, err)
		defer release()
		return bm.ToArray()
	}

	first, err := NewRoaringSetBatchReader(narrowed, keys)
	require.NoError(t, err)
	second, err := NewRoaringSetBatchReader(narrowed, keys)
	require.NoError(t, err)

	require.Equal(t, []uint64{1}, read(t, first))
	require.Equal(t, []uint64{1}, read(t, second),
		"a reader opened after the write must answer as the one opened before it")

	unnarrowed, err := NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), keys)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, read(t, unnarrowed),
		"the same view unnarrowed reads the write, which is what narrowing settles")
}

// TestBucketRoaringSetRefusesSecondaryIndexes pins that the pair is refused
// where the bucket is built. The flush guards it too, but by then the bucket
// has acknowledged writes it can never flush: every cycle fails, the memtable
// never drains, and each attempt leaves another commit log on disk.
func TestBucketRoaringSetRefusesSecondaryIndexes(t *testing.T) {
	logger, _ := test.NewNullLogger()

	newBucket := func(t *testing.T, opts ...BucketOption) (*Bucket, error) {
		t.Helper()
		return NewBucketCreator().NewBucket(context.Background(), t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			append([]BucketOption{WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop())}, opts...)...)
	}

	t.Run("roaring set with secondary indexes is refused", func(t *testing.T) {
		b, err := newBucket(t, WithStrategy(StrategyRoaringSet), WithSecondaryIndices(1))
		require.ErrorContains(t, err, "secondary indexes")
		require.Nil(t, b)
	})

	t.Run("roaring set without them opens", func(t *testing.T) {
		b, err := newBucket(t, WithStrategy(StrategyRoaringSet))
		require.NoError(t, err)
		require.NoError(t, b.Shutdown(context.Background()))
	})

	// The two siblings build their own index the same way, so they cannot carry
	// one either. Roaring set's range sibling would record a count of zero and
	// lose the option silently; inverted stamps the count it was given and then
	// cannot load the segment it just wrote.
	t.Run("the other strategies that build their own index are refused too", func(t *testing.T) {
		for _, strategy := range []string{StrategyRoaringSetRange, StrategyInverted} {
			t.Run(strategy, func(t *testing.T) {
				b, err := newBucket(t, WithStrategy(strategy), WithSecondaryIndices(1))
				require.ErrorContains(t, err, "secondary indexes")
				require.Nil(t, b)
			})
		}
	})

	t.Run("a strategy that uses the shared index writer still takes them", func(t *testing.T) {
		for _, strategy := range []string{StrategyReplace, StrategySetCollection, StrategyMapCollection} {
			t.Run(strategy, func(t *testing.T) {
				b, err := newBucket(t, WithStrategy(strategy), WithSecondaryIndices(1))
				require.NoError(t, err)
				require.NoError(t, b.Shutdown(context.Background()))
			})
		}
	})
}

// TestBucketShutdownDrainsWriters pins that Shutdown waits for writers already
// past getActiveMemtableForWrite. That helper releases the flush read lock
// before its caller writes, so flushLock alone does not say the memtable is
// idle: such a write would land in a memtable Shutdown has already finished
// with, behind a commit log it has already closed, so nothing replays it.
//
// Shutdown picks one of two exits by the commit log's size, and both drop the
// write: the segment flush deletes the commit log, and the WAL-reuse exit keeps
// the file but closes it before the write arrives. Both thresholds are run, so
// the drain is not pinned only on the side this fixture's writes happen to
// fall on.
//
// The writer count is held directly rather than raced, so the window is the
// test's to control.
func TestBucketShutdownDrainsWriters(t *testing.T) {
	for _, tc := range []struct {
		name            string
		minWalThreshold int64
	}{
		{name: "segment flush", minWalThreshold: 0},
		{name: "WAL reuse", minWalThreshold: config.DefaultPersistenceMaxReuseWalSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shutdownDrainsWriters(t, tc.minWalThreshold)
		})
	}
}

func shutdownDrainsWriters(t *testing.T, minWalThreshold int64) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	open := func(t *testing.T) *Bucket {
		t.Helper()
		b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyRoaringSet),
			WithMinWalThreshold(minWalThreshold),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
		require.NoError(t, err)
		return b
	}

	b := open(t)
	b.SetMemtableThreshold(1e9)
	require.NoError(t, b.RoaringSetAddOne([]byte("early"), 1))

	// Stand where a writer stands after getActiveMemtableForWrite has returned:
	// holding the memtable and its count, with no bucket lock.
	active, err := b.getActiveMemtableForWrite()
	require.NoError(t, err)
	release := active.decWriterCount
	// Deferred as well as released below: the drain waits on this count with no
	// deadline, so a FailNow between here and the explicit release would strand
	// the Shutdown goroutine holding two bucket locks for the rest of the binary.
	releaseOnce := sync.OnceFunc(release)
	defer releaseOnce()

	shutdown := make(chan error, 1)
	go func() { shutdown <- b.Shutdown(ctx) }()

	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown finished while a writer was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, active.roaringSetAddOne([]byte("late"), 42))
	releaseOnce()

	// Bounded: waitForZeroWriters has no ctx bail-out, so a regression to writer
	// accounting would hang this receive. A bare receive would take the whole
	// package down with a timeout panic instead of naming the failure.
	select {
	case err := <-shutdown:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Shutdown did not return: the drain is still waiting on a writer count")
	}

	reopened := open(t)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, reopened.Shutdown(cleanupCtx))
	})

	bm, releaseGet, err := reopened.RoaringSetGet(ctx, []byte("late"))
	require.NoError(t, err)
	defer releaseGet()
	require.Equal(t, []uint64{42}, bm.ToArray(),
		"the write was acknowledged but reached neither the segment nor a commit log")
}

// TestBucketRoaringSetRefusesANilKey pins the contract every memtable walk
// rests on. A nil key is the end-of-walk marker for the flush and for both read
// cursors, so a node carrying one would end a walk at that node — silently, and
// for the flush that means renaming a header-only segment into place and
// deleting the commit log behind it.
//
// An absent value is not a nil key: it is indexed by the null pseudo-property's
// own bucket under a one-byte key, so nil reaching here is a caller's mistake.
// A zero-length key is a real key and stays accepted.
func TestBucketRoaringSetRefusesANilKey(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	// Bounded: a failed flush parks a memtable in b.flushing that nothing clears,
	// and Shutdown polls for it with no deadline of its own. An unbounded cleanup
	// here turns any failure in this test into a package-wide timeout panic.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(cleanupCtx))
	})

	refusals := map[string]func(key []byte) error{
		"AddOne":    func(k []byte) error { return b.RoaringSetAddOne(k, 1) },
		"RemoveOne": func(k []byte) error { return b.RoaringSetRemoveOne(k, 1) },
		"AddList":   func(k []byte) error { return b.RoaringSetAddList(k, []uint64{1}) },
		"AddBitmap": func(k []byte) error { return b.RoaringSetAddBitmap(k, bitmapFromSlice([]uint64{1})) },
		"AddBatch": func(k []byte) error {
			return b.RoaringSetAddBatch([]RoaringSetBatchEntry{{Key: k, Values: []uint64{1}}})
		},
		"RemoveBatch": func(k []byte) error {
			return b.RoaringSetRemoveBatch([]RoaringSetBatchEntry{{Key: k, Values: []uint64{1}}})
		},
	}

	for name, write := range refusals {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, write(nil), "must not be nil")
			require.NoError(t, write([]byte{}),
				"a zero-length key is a key the tree can hold, not an absent one")
		})
	}

	// A key of its own, so the subtests above cannot decide what this reads back.
	// The zero-length key sorts first, which is where a nil key would have sat,
	// so a walk that stopped on it would lose "aaa" too.
	require.NoError(t, b.RoaringSetAddList([]byte{}, []uint64{11, 12}))
	require.NoError(t, b.RoaringSetAddList([]byte("aaa"), []uint64{7}))
	require.NoError(t, b.FlushAndSwitch())

	for _, want := range []struct {
		key    []byte
		docIDs []uint64
	}{
		{key: []byte{}, docIDs: []uint64{11, 12}},
		{key: []byte("aaa"), docIDs: []uint64{7}},
	} {
		bm, release, err := b.RoaringSetGet(ctx, want.key)
		require.NoError(t, err)
		require.Subset(t, bm.ToArray(), want.docIDs,
			"key %q did not survive the flush", want.key)
		release()
	}
}
