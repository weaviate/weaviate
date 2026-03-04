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
	"os"
	"path/filepath"
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
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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

		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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

		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

		// Target's add of 2 is in a newer segment, so it should still be present.
		// Source segments (older) have: add {1,2,3}, then delete {2} => net {1,3}.
		// Target segment (newer): add {2,4}.
		// Merge: {1,2,3,4} — target's newer add of 2 takes precedence over source's delete.
		assertRoaringSetContains(t, tgtBucket, []byte("key-x"), []uint64{1, 2, 3, 4})
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
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))
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

		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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

		err = b.disk.PrependSegmentsFromBucket(ctx, t.TempDir())
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
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir1))
		// Prepend second source (dynamic shift must place these before the
		// first prepend's segments, which are already shifted).
		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir2))

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

		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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

		require.NoError(t, tgtBucket.disk.PrependSegmentsFromBucket(ctx, srcDir))

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
