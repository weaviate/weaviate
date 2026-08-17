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
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/concurrency/testinghelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
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

// TestBucket_RoaringSetGetFromConsistentView_ReleasesDiskLayerOnError pins that
// a failing memtable read cannot leak the disk row's pooled buffer.
//
// The memtables are read first, so a failure there returns before the buffer is
// acquired at all. The assertions are therefore that the disk row was never
// read, which is stronger than that it was read and released, and does not
// depend on every error path remembering a defer.
func TestBucket_RoaringSetGetFromConsistentView_ReleasesDiskLayerOnError(t *testing.T) {
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
	reader, err := NewRoaringSetBatchReader(view, keys)
	require.NoError(t, err)

	requireRowsAre(t, reader, keys, map[string][]uint64{
		string(keyA):       arrWantA,
		string(keyB):       arrWantB,
		string(keyMissing): nil,
	})
}

// TestBatchReaderSurvivesFlushAndSwitch pins the view
// stability that justifies holding one view for a whole batch: a flush landing
// mid-fold must not disturb the keys still to be read. The second key's window
// is filled after the switch, through a memtable the bucket has already moved
// past, and must still answer as it did before — writes landing in the new
// active memtable stay invisible. A fresh RoaringSetGet sees the post-switch
// write, proving the view pinned state rather than the two paths coincidentally
// agreeing.
// (A view is not a write snapshot: writes before the switch go to the old
// active memtable the view references, so only post-switch invisibility is
// asserted.)
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
	// One key per window, so the second key's window is filled after the switch
	// below rather than before it — which is the ordering a long fold meets and
	// the only one that reads a memtable the bucket has already moved past.
	reader, err := newRoaringSetBatchReader(view, keys, 1)
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

		reader, err := NewRoaringSetBatchReader(view, sortedKeysOf(t, []string{"k"}))
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
				// Written through roaringSetRemoveList rather than straight into
				// the tree, so the memtable's size reflects what it holds. The
				// reader skips a memtable reporting size 0, and inserting behind
				// its accounting builds a memtable that holds a row and reports
				// none — a state no write path produces.
				mt := newTestMemtableRoaringSet(nil)
				require.NoError(t, mt.roaringSetRemoveList([]byte("k"), tc.flushingDeletes))
				b.flushing = mt
			}

			view := b.GetConsistentView()
			defer view.ReleaseView()
			reader, err := NewRoaringSetBatchReader(view, sortedKeysOf(t, []string{"k"}))
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
		reader, err := NewRoaringSetBatchReader(view, sortedKeysOf(t, []string{"k"}))
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
	reader, err := NewRoaringSetBatchReader(view, sortedKeysOf(t, []string{string(key)}))
	require.NoError(t, err)

	bm, release, err := reader.Next(1)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []uint64{1, 3}, bm.ToArray(),
		"a tombstone-only active memtable must not be treated as empty")
}
