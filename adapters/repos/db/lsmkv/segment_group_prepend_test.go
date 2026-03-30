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
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// createTestBucketRoaringSet creates a RoaringSet bucket in the given
// directory, suitable for testing PrependSegmentsFromBucket.
func createTestBucketRoaringSet(t *testing.T, ctx context.Context, dir string) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9) // prevent auto-flush
	return b
}

func TestSegmentGroup_PrependSegments(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		ctx := context.Background()

		// Create source bucket with some data and flush to disk.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)

		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-a"), []uint64{1, 2, 3}))
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-b"), []uint64{10, 20}))
		require.NoError(t, srcBucket.FlushAndSwitch())

		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-c"), []uint64{100}))
		require.NoError(t, srcBucket.FlushAndSwitch())

		// Shut down source bucket (precondition).
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create target bucket with its own data.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-d"), []uint64{999}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		// Prepend source segments into target.
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Verify all source data is readable via target.
		assertRoaringSetContains(t, tgtBucket, []byte("key-a"), []uint64{1, 2, 3})
		assertRoaringSetContains(t, tgtBucket, []byte("key-b"), []uint64{10, 20})
		assertRoaringSetContains(t, tgtBucket, []byte("key-c"), []uint64{100})

		// Verify target's own data is still present.
		assertRoaringSetContains(t, tgtBucket, []byte("key-d"), []uint64{999})

		// Verify source directory is untouched (files still exist).
		srcFiles, err := os.ReadDir(srcDir)
		require.NoError(t, err)
		assert.NotEmpty(t, srcFiles, "source dir should still have its files")
	})

	t.Run("read ordering correctness (newer target wins)", func(t *testing.T) {
		ctx := context.Background()

		// Source has key-x = {1, 2, 3}.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-x"), []uint64{1, 2, 3}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Target has key-x = {3, 4, 5}, which should be merged with source.
		// In roaring set strategy, values from both segments are OR-merged.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-x"), []uint64{3, 4, 5}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Roaring set merges additions: result should contain union.
		assertRoaringSetContains(t, tgtBucket, []byte("key-x"), []uint64{1, 2, 3, 4, 5})
	})

	t.Run("source with deletions (target newer wins)", func(t *testing.T) {
		ctx := context.Background()

		// Source: add key-x={1,2,3}, then remove 2.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-x"), []uint64{1, 2, 3}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.RoaringSetRemoveOne([]byte("key-x"), 2))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Target: add key-x={2,4} (re-adds 2 which source deleted).
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-x"), []uint64{2, 4}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Target's add of 2 is in a newer segment, so it should still be present.
		// Source segments (older) have: add {1,2,3}, then delete {2} => net {1,3}.
		// Target segment (newer): add {2,4}.
		// Merge: {1,2,3,4} — target's newer add of 2 takes precedence over source's delete.
		assertRoaringSetContains(t, tgtBucket, []byte("key-x"), []uint64{1, 2, 3, 4})
	})

	t.Run("empty target (no timestamp shift needed)", func(t *testing.T) {
		ctx := context.Background()

		// Create source with data.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-a"), []uint64{1, 2, 3}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create empty target (no flushes, no segments on disk).
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Source data should be readable.
		assertRoaringSetContains(t, tgtBucket, []byte("key-a"), []uint64{1, 2, 3})

		// Verify it survives reload.
		require.NoError(t, tgtBucket.Shutdown(ctx))
		tgtBucket2 := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket2.Shutdown(ctx)

		assertRoaringSetContains(t, tgtBucket2, []byte("key-a"), []uint64{1, 2, 3})
	})

	t.Run("empty source (no-op)", func(t *testing.T) {
		ctx := context.Background()

		// Empty source dir (no segments).
		srcDir := t.TempDir()

		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-a"), []uint64{42}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		segCountBefore := tgtBucket.disk.Len()
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Segment count unchanged.
		assert.Equal(t, segCountBefore, tgtBucket.disk.Len())

		// Data intact.
		assertRoaringSetContains(t, tgtBucket, []byte("key-a"), []uint64{42})
	})

	t.Run("concurrent reads during prepend", func(t *testing.T) {
		ctx := context.Background()

		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-src"), []uint64{1, 2, 3}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-tgt"), []uint64{10, 20}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		// Start concurrent readers.
		var wg sync.WaitGroup
		stop := make(chan struct{})
		readErrors := make(chan error, 100)

		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						bm, release, err := tgtBucket.RoaringSetGet([]byte("key-tgt"))
						if err != nil {
							readErrors <- err
							return
						}
						if !bm.Contains(10) || !bm.Contains(20) {
							readErrors <- assert.AnError
							release()
							return
						}
						release()
					}
				}
			}()
		}

		// Run prepend while readers are active.
		time.Sleep(5 * time.Millisecond) // let readers start
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		close(stop)
		wg.Wait()
		close(readErrors)

		for err := range readErrors {
			t.Fatalf("concurrent read error: %v", err)
		}

		// Verify all data after prepend.
		assertRoaringSetContains(t, tgtBucket, []byte("key-tgt"), []uint64{10, 20})
		assertRoaringSetContains(t, tgtBucket, []byte("key-src"), []uint64{1, 2, 3})
	})

	t.Run("compaction works correctly after prepend", func(t *testing.T) {
		ctx := context.Background()

		// Create source with data.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-src"), []uint64{1}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create target with multiple segments (compaction candidates).
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		for i := range 5 {
			require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-tgt"), []uint64{uint64(100 + i)}))
			require.NoError(t, tgtBucket.FlushAndSwitch())
		}

		segCountBefore := tgtBucket.disk.Len()

		// Prepend source segments (internally pauses compaction).
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))
		assert.Equal(t, segCountBefore+1, tgtBucket.disk.Len())

		// Now run compaction — it should work correctly with the new
		// segment ordering (prepended segments at position 0).
		for {
			compacted, err := tgtBucket.disk.compactOnce()
			require.NoError(t, err)
			if !compacted {
				break
			}
		}

		// All data should be present after compaction.
		assertRoaringSetContains(t, tgtBucket, []byte("key-src"), []uint64{1})
		assertRoaringSetContains(t, tgtBucket, []byte("key-tgt"), []uint64{100, 101, 102, 103, 104})

		// Fewer segments after compaction.
		assert.Less(t, tgtBucket.disk.Len(), segCountBefore+1)
	})

	t.Run("compaction merges src and dest segments across mixed levels", func(t *testing.T) {
		// Verifies that after prepending compacted source segments into a
		// compacted target bucket, further compaction merges src segments
		// into dest segments (not just compacting each side independently).
		//
		// Setup (per reviewer request):
		//   7 src segments → compact → 3 segments at levels [2, 1, 0]
		//   9 dest segments → compact → 2 segments at levels [3, 0]
		//   prepend → 5 segments: [2, 1, 0, 3, 0]
		//
		// After compaction, the unordered-levels fix merges src segments
		// into dest segments step by step:
		//   [2, 1, 0, 3, 0] → [2, 1, 3, 0] → [3, 3, 0] → [4, 0]
		// Final: 2 segments at levels [4, 0].
		ctx := context.Background()

		segmentLevels := func(sg *SegmentGroup) []uint16 {
			sg.maintenanceLock.RLock()
			defer sg.maintenanceLock.RUnlock()
			levels := make([]uint16, len(sg.segments))
			for i, seg := range sg.segments {
				levels[i] = seg.getLevel()
			}
			return levels
		}

		// --- Source: 7 segments → compact to [2, 1, 0] ---
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		for i := range 7 {
			require.NoError(t, srcBucket.RoaringSetAddList(
				[]byte(fmt.Sprintf("src-key-%d", i)), []uint64{uint64(i)}))
			require.NoError(t, srcBucket.FlushAndSwitch())
		}
		assert.Equal(t, 7, srcBucket.disk.Len())

		for {
			compacted, err := srcBucket.disk.compactOnce()
			require.NoError(t, err)
			if !compacted {
				break
			}
		}
		assert.Equal(t, 3, srcBucket.disk.Len())
		assert.Equal(t, []uint16{2, 1, 0}, segmentLevels(srcBucket.disk))
		require.NoError(t, srcBucket.Shutdown(ctx))

		// --- Dest: 9 segments → compact to [3, 0] ---
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		for i := range 9 {
			require.NoError(t, tgtBucket.RoaringSetAddList(
				[]byte(fmt.Sprintf("tgt-key-%d", i)), []uint64{uint64(100 + i)}))
			require.NoError(t, tgtBucket.FlushAndSwitch())
		}
		assert.Equal(t, 9, tgtBucket.disk.Len())

		for {
			compacted, err := tgtBucket.disk.compactOnce()
			require.NoError(t, err)
			if !compacted {
				break
			}
		}
		assert.Equal(t, 2, tgtBucket.disk.Len())
		assert.Equal(t, []uint16{3, 0}, segmentLevels(tgtBucket.disk))

		// --- Prepend: 5 segments [2, 1, 0, 3, 0] ---
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))
		assert.Equal(t, 5, tgtBucket.disk.Len())
		assert.Equal(t, []uint16{2, 1, 0, 3, 0}, segmentLevels(tgtBucket.disk))

		// --- Compact after prepend ---
		// The unordered-levels fix should merge src into dest segments:
		//   step 1: src(1) + src(0) → merged(1)   → [2, 1, 3, 0]
		//   step 2: src(2) + merged(1) → merged(3) → [3, 3, 0]
		//   step 3: merged(3) + dest(3) → merged(4) → [4, 0]
		for {
			compacted, err := tgtBucket.disk.compactOnce()
			require.NoError(t, err)
			if !compacted {
				break
			}
		}
		assert.Equal(t, 2, tgtBucket.disk.Len(),
			"src segments should be merged into dest, leaving 2 segments")
		assert.Equal(t, []uint16{4, 0}, segmentLevels(tgtBucket.disk))

		// All data from both src and dest must be present.
		for i := range 7 {
			assertRoaringSetContains(t, tgtBucket, []byte(fmt.Sprintf("src-key-%d", i)), []uint64{uint64(i)})
		}
		for i := range 9 {
			assertRoaringSetContains(t, tgtBucket, []byte(fmt.Sprintf("tgt-key-%d", i)), []uint64{uint64(100 + i)})
		}

		// Verify survives reload.
		require.NoError(t, tgtBucket.Shutdown(ctx))
		tgtBucket2 := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket2.Shutdown(ctx)

		for i := range 7 {
			assertRoaringSetContains(t, tgtBucket2, []byte(fmt.Sprintf("src-key-%d", i)), []uint64{uint64(i)})
		}
		for i := range 9 {
			assertRoaringSetContains(t, tgtBucket2, []byte(fmt.Sprintf("tgt-key-%d", i)), []uint64{uint64(100 + i)})
		}
	})

	t.Run("overlapping keys prove source segments are older", func(t *testing.T) {
		ctx := context.Background()

		// Source: add key-x={1}, remove key-x={2}.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-x"), []uint64{1, 2}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.RoaringSetRemoveOne([]byte("key-x"), 2))
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-x"), []uint64{10}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))
		// Source net state: {1, 10} present, {2} deleted.

		// Target: add key-x={2}, remove key-x={1}.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-x"), []uint64{1, 2}))
		require.NoError(t, tgtBucket.FlushAndSwitch())
		require.NoError(t, tgtBucket.RoaringSetRemoveOne([]byte("key-x"), 1))
		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-x"), []uint64{20}))
		require.NoError(t, tgtBucket.FlushAndSwitch())
		// Target net state: {2, 20} present, {1} deleted.

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// If source is correctly older, target's operations win:
		//   - Source added 1, but target (newer) removed 1 → 1 absent
		//   - Source removed 2, but target (newer) added 2 → 2 present
		//   - Source added 10, target didn't touch it → 10 present
		//   - Target added 20, source didn't touch it → 20 present
		bm, release, err := tgtBucket.RoaringSetGet([]byte("key-x"))
		require.NoError(t, err)
		defer release()

		assert.False(t, bm.Contains(1), "1 should be absent: target (newer) removed it")
		assert.True(t, bm.Contains(2), "2 should be present: target (newer) re-added it")
		assert.True(t, bm.Contains(10), "10 should be present: source added, target didn't touch")
		assert.True(t, bm.Contains(20), "20 should be present: target added it")
	})

	t.Run("crash recovery / reload from disk", func(t *testing.T) {
		ctx := context.Background()

		// Create source and flush.
		srcDir := t.TempDir()
		srcBucket := createTestBucketRoaringSet(t, ctx, srcDir)
		require.NoError(t, srcBucket.RoaringSetAddList([]byte("key-src"), []uint64{7, 8, 9}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create target, flush, prepend, then shutdown.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-tgt"), []uint64{42}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Verify before shutdown.
		assertRoaringSetContains(t, tgtBucket, []byte("key-src"), []uint64{7, 8, 9})
		assertRoaringSetContains(t, tgtBucket, []byte("key-tgt"), []uint64{42})

		// Shutdown and reopen from disk.
		require.NoError(t, tgtBucket.Shutdown(ctx))

		tgtBucket2 := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket2.Shutdown(ctx)

		// Data should survive the reload.
		assertRoaringSetContains(t, tgtBucket2, []byte("key-src"), []uint64{7, 8, 9})
		assertRoaringSetContains(t, tgtBucket2, []byte("key-tgt"), []uint64{42})
	})

	t.Run("strategy guard rejects Replace", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		logger, _ := test.NewNullLogger()

		// Create a Replace-strategy bucket.
		opts := []BucketOption{
			WithStrategy(StrategyReplace),
		}
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		defer b.Shutdown(ctx)

		err = b.PrependSegmentsFromBucket(ctx, t.TempDir())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replace")
		assert.Contains(t, err.Error(), "countNetAdditions")
	})

	t.Run("tmp files ignored on recovery", func(t *testing.T) {
		ctx := context.Background()

		// Create a target bucket.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)
		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-a"), []uint64{1}))
		require.NoError(t, tgtBucket.FlushAndSwitch())
		require.NoError(t, tgtBucket.Shutdown(ctx))

		// Simulate a crash mid-copy by leaving .tmp files.
		err := os.WriteFile(filepath.Join(tgtDir, "segment-999.db.tmp"), []byte("garbage"), 0o644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(tgtDir, "segment-999.bloom.tmp"), []byte("garbage"), 0o644)
		require.NoError(t, err)

		// Reopen — should succeed without error and ignore .tmp files.
		tgtBucket2 := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket2.Shutdown(ctx)

		assertRoaringSetContains(t, tgtBucket2, []byte("key-a"), []uint64{1})
	})

	t.Run("multiple prepends accumulate correctly and survive reload", func(t *testing.T) {
		ctx := context.Background()

		// Create two separate source buckets.
		srcDir1 := t.TempDir()
		srcBucket1 := createTestBucketRoaringSet(t, ctx, srcDir1)
		require.NoError(t, srcBucket1.RoaringSetAddList([]byte("key-1"), []uint64{1}))
		require.NoError(t, srcBucket1.FlushAndSwitch())
		require.NoError(t, srcBucket1.Shutdown(ctx))

		srcDir2 := t.TempDir()
		srcBucket2 := createTestBucketRoaringSet(t, ctx, srcDir2)
		require.NoError(t, srcBucket2.RoaringSetAddList([]byte("key-2"), []uint64{2}))
		require.NoError(t, srcBucket2.FlushAndSwitch())
		require.NoError(t, srcBucket2.Shutdown(ctx))

		// Target bucket.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucketRoaringSet(t, ctx, tgtDir)

		require.NoError(t, tgtBucket.RoaringSetAddList([]byte("key-tgt"), []uint64{99}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		// Prepend first source.
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir1))
		// Prepend second source (dynamic shift must place these before the
		// first prepend's segments, which are already shifted).
		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir2))

		assertRoaringSetContains(t, tgtBucket, []byte("key-1"), []uint64{1})
		assertRoaringSetContains(t, tgtBucket, []byte("key-2"), []uint64{2})
		assertRoaringSetContains(t, tgtBucket, []byte("key-tgt"), []uint64{99})

		// Shut down and reopen to verify that the on-disk filenames sort
		// correctly: src2 segments < src1 segments < tgt segments.
		require.NoError(t, tgtBucket.Shutdown(ctx))

		tgtBucket2 := createTestBucketRoaringSet(t, ctx, tgtDir)
		defer tgtBucket2.Shutdown(ctx)

		assertRoaringSetContains(t, tgtBucket2, []byte("key-1"), []uint64{1})
		assertRoaringSetContains(t, tgtBucket2, []byte("key-2"), []uint64{2})
		assertRoaringSetContains(t, tgtBucket2, []byte("key-tgt"), []uint64{99})
	})

	t.Run("SetCollection strategy", func(t *testing.T) {
		ctx := context.Background()

		// Create source Set bucket.
		srcDir := t.TempDir()
		srcBucket := createTestBucket(t, ctx, srcDir, StrategySetCollection)
		require.NoError(t, srcBucket.SetAdd([]byte("key-a"), [][]byte{[]byte("val1"), []byte("val2")}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create target Set bucket.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucket(t, ctx, tgtDir, StrategySetCollection)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.SetAdd([]byte("key-b"), [][]byte{[]byte("val3")}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Verify source data is readable via target.
		vals, err := tgtBucket.SetList([]byte("key-a"))
		require.NoError(t, err)
		assert.Len(t, vals, 2)

		// Verify target's own data.
		vals, err = tgtBucket.SetList([]byte("key-b"))
		require.NoError(t, err)
		assert.Len(t, vals, 1)

		// Verify survives reload.
		require.NoError(t, tgtBucket.Shutdown(ctx))
		tgtBucket2 := createTestBucket(t, ctx, tgtDir, StrategySetCollection)
		defer tgtBucket2.Shutdown(ctx)

		vals, err = tgtBucket2.SetList([]byte("key-a"))
		require.NoError(t, err)
		assert.Len(t, vals, 2)
	})

	t.Run("MapCollection strategy", func(t *testing.T) {
		ctx := context.Background()

		// Create source Map bucket.
		srcDir := t.TempDir()
		srcBucket := createTestBucket(t, ctx, srcDir, StrategyMapCollection)
		require.NoError(t, srcBucket.MapSet([]byte("row-a"), MapPair{Key: []byte("k1"), Value: []byte("v1")}))
		require.NoError(t, srcBucket.MapSet([]byte("row-a"), MapPair{Key: []byte("k2"), Value: []byte("v2")}))
		require.NoError(t, srcBucket.FlushAndSwitch())
		require.NoError(t, srcBucket.Shutdown(ctx))

		// Create target Map bucket.
		tgtDir := t.TempDir()
		tgtBucket := createTestBucket(t, ctx, tgtDir, StrategyMapCollection)
		defer tgtBucket.Shutdown(ctx)

		require.NoError(t, tgtBucket.MapSet([]byte("row-b"), MapPair{Key: []byte("k3"), Value: []byte("v3")}))
		require.NoError(t, tgtBucket.FlushAndSwitch())

		require.NoError(t, tgtBucket.PrependSegmentsFromBucket(ctx, srcDir))

		// Verify source data.
		pairs, err := tgtBucket.MapList(ctx, []byte("row-a"))
		require.NoError(t, err)
		assert.Len(t, pairs, 2)

		// Verify target's own data.
		pairs, err = tgtBucket.MapList(ctx, []byte("row-b"))
		require.NoError(t, err)
		assert.Len(t, pairs, 1)

		// Verify survives reload.
		require.NoError(t, tgtBucket.Shutdown(ctx))
		tgtBucket2 := createTestBucket(t, ctx, tgtDir, StrategyMapCollection)
		defer tgtBucket2.Shutdown(ctx)

		pairs, err = tgtBucket2.MapList(ctx, []byte("row-a"))
		require.NoError(t, err)
		assert.Len(t, pairs, 2)
	})
}

func TestParseSegmentTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "legacy format",
			input:    "segment-1771258130098421000.db",
			expected: 1771258130098421000,
		},
		{
			name:     "new format with level and strategy",
			input:    "segment-1771258130098421000.l0.s5.db",
			expected: 1771258130098421000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := parseSegmentTimestamp(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, ts)
		})
	}
}

func TestComputeTimestampShift(t *testing.T) {
	t.Run("empty target returns zero shift", func(t *testing.T) {
		shift, err := computeTimestampShift(
			[]string{"segment-1000000000.db"},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, int64(0), shift)
	})

	t.Run("source already before target", func(t *testing.T) {
		shift, err := computeTimestampShift(
			[]string{"segment-100.db"},
			[]string{"segment-999999999999999999.db"},
		)
		require.NoError(t, err)
		// Should return -1s gap (source already sorts before target).
		assert.Equal(t, int64(-1e9), shift)
	})

	t.Run("source after target requires negative shift", func(t *testing.T) {
		shift, err := computeTimestampShift(
			[]string{"segment-2000000000000000000.db"},
			[]string{"segment-1000000000000000000.db"},
		)
		require.NoError(t, err)
		assert.Less(t, shift, int64(0))
		// Verify: srcMax + shift < tgtMin
		assert.Less(t, int64(2000000000000000000)+shift, int64(1000000000000000000))
	})

	t.Run("works with new filename format", func(t *testing.T) {
		shift, err := computeTimestampShift(
			[]string{"segment-2000000000000000000.l0.s5.db"},
			[]string{"segment-1000000000000000000.l0.s5.db"},
		)
		require.NoError(t, err)
		assert.Less(t, shift, int64(0))
	})
}

func TestDiscoverDBFiles(t *testing.T) {
	t.Run("filters out non-db files and tmp files", func(t *testing.T) {
		dir := t.TempDir()
		// Create various files.
		for _, name := range []string{
			"segment-100.db",
			"segment-200.l0.s5.db",
			"segment-300.bloom",
			"segment-400_500.db.tmp",
			"segment-600.cna",
			"not-a-segment.txt",
		} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
		}

		files, err := discoverDBFiles(dir)
		require.NoError(t, err)
		assert.Equal(t, []string{
			"segment-100.db",
			"segment-200.l0.s5.db",
		}, files)
	})
}

func TestCopySegmentFiles_ConsistentRename(t *testing.T) {
	// Verify that copySegmentFiles renames both .db and auxiliary files
	// (e.g., .bloom) with the same shifted prefix. This matters because
	// newSegment with overwriteDerived=false will silently regenerate
	// missing auxiliaries, masking a rename mismatch bug.
	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.MkdirAll(dstDir, 0o755))

	// Create fake source segment files.
	for _, name := range []string{
		"segment-2000000000000000000.db",
		"segment-2000000000000000000.bloom",
		"segment-2000000000000000000.cna",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(srcDir, name), []byte("data"), 0o644))
	}

	dbFiles := []string{"segment-2000000000000000000.db"}
	shift := int64(-1000000000000000000) // shift by -1e18

	copiedDB, err := copySegmentFiles(srcDir, dstDir, dbFiles, shift)
	require.NoError(t, err)
	require.Len(t, copiedDB, 1)

	// The .db file should have the shifted timestamp.
	assert.Equal(t, "segment-1000000000000000000.db", copiedDB[0])

	// All auxiliary files must share the same shifted prefix.
	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)

	assert.Equal(t, []string{
		"segment-1000000000000000000.bloom",
		"segment-1000000000000000000.cna",
		"segment-1000000000000000000.db",
	}, names)
}

func TestCopySegmentFiles_NewFormatWithLevelAndStrategy(t *testing.T) {
	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.MkdirAll(dstDir, 0o755))

	// New format: segment-<ts>.l0.s5.db and matching auxiliaries.
	for _, name := range []string{
		"segment-2000000000000000000.l0.s5.db",
		"segment-2000000000000000000.l0.s5.bloom",
		"segment-2000000000000000000.l0.s5.metadata",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(srcDir, name), []byte("data"), 0o644))
	}

	dbFiles := []string{"segment-2000000000000000000.l0.s5.db"}
	shift := int64(-500000000000000000)

	copiedDB, err := copySegmentFiles(srcDir, dstDir, dbFiles, shift)
	require.NoError(t, err)
	require.Len(t, copiedDB, 1)
	assert.Equal(t, "segment-1500000000000000000.l0.s5.db", copiedDB[0])

	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)

	assert.Equal(t, []string{
		"segment-1500000000000000000.l0.s5.bloom",
		"segment-1500000000000000000.l0.s5.db",
		"segment-1500000000000000000.l0.s5.metadata",
	}, names)
}

func TestApplyTimestampShift(t *testing.T) {
	t.Run("positive shift", func(t *testing.T) {
		result, err := applyTimestampShift("segment-1000", 500)
		require.NoError(t, err)
		assert.Equal(t, "segment-1500", result)
	})

	t.Run("negative shift", func(t *testing.T) {
		result, err := applyTimestampShift("segment-1000", -300)
		require.NoError(t, err)
		assert.Equal(t, "segment-700", result)
	})

	t.Run("zero shift", func(t *testing.T) {
		result, err := applyTimestampShift("segment-1000", 0)
		require.NoError(t, err)
		assert.Equal(t, "segment-1000", result)
	})
}

// createTestBucket creates a bucket with the given strategy.
func createTestBucket(t *testing.T, ctx context.Context, dir, strategy string) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(strategy),
	}
	if strategy == StrategyRoaringSet {
		opts = append(opts, WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	return b
}

// assertRoaringSetContains verifies that getting the key from the bucket
// returns a bitmap containing all expected values.
func assertRoaringSetContains(t *testing.T, b *Bucket, key []byte, expected []uint64) {
	t.Helper()
	bm, release, err := b.RoaringSetGet(key)
	require.NoError(t, err)
	defer release()
	for _, v := range expected {
		assert.True(t, bm.Contains(v), "expected bitmap to contain %d for key %q", v, key)
	}
}
