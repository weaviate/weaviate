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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestAsyncDeletionLimit verifies that when maxPendingAsyncDeletions is set,
// the second compaction falls back to synchronous deletion rather than
// spawning a goroutine that would push pendingAsyncDeletions beyond the limit.
//
// Setup:
//   - Bucket B has maxPendingAsyncDeletions = 1.
//   - Flush two segments (seg_a, seg_b) and acquire a consistent view that pins
//     them. This ensures the first async deletion goroutine (after compacting
//     seg_a+seg_b) blocks until the view is released.
//   - First compaction produces merged_ab; async goroutine waits on seg_a/seg_b
//     refs → pendingAsyncDeletions = 1.
//   - Flush two more segments (seg_c, seg_d) and compact again. The limit is
//     reached, so seg_c and seg_d are deleted synchronously. Because no view
//     pins seg_c/seg_d, the sync deletion completes immediately.
//   - pendingAsyncDeletions remains 1 (only the first goroutine is still stuck).
//   - Release the view; the first goroutine unblocks and drains.
func TestAsyncDeletionLimit(t *testing.T) {
	ctx := t.Context()

	dirB := t.TempDir()
	bucketB, err := NewBucketCreator().NewBucket(ctx, dirB, dirB, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithMaxPendingAsyncDeletions(1))
	require.NoError(t, err)
	t.Cleanup(func() { bucketB.Shutdown(ctx) })

	bucketB.SetMemtableThreshold(1e9)

	// Flush first two segments.
	require.NoError(t, bucketB.Put([]byte("a"), []byte("1")))
	require.NoError(t, bucketB.FlushAndSwitch())
	require.NoError(t, bucketB.Put([]byte("b"), []byte("2")))
	require.NoError(t, bucketB.FlushAndSwitch())

	// Acquire a consistent view — this pins seg_a and seg_b (refs > 0).
	// The first async deletion goroutine will block until we release this view.
	segsB, releaseB := bucketB.disk.getConsistentViewOfSegments()
	require.Len(t, segsB, 2, "expected 2 flushed segments before first compaction")

	// First compaction: compacts seg_a + seg_b → merged_ab.
	// launchOrSyncDelete sees pending(0) < limit(1), so it spawns a goroutine.
	// That goroutine waits on seg_a/seg_b refs (held by the view above).
	compacted1, err := bucketB.disk.compactOnce()
	require.NoError(t, err)
	assert.True(t, compacted1, "first compaction should produce a merged segment")

	// Give the async goroutine a moment to start and increment the counter.
	require.Eventually(t, func() bool {
		return bucketB.disk.pendingAsyncDeletions.Load() == 1
	}, 2*time.Second, 10*time.Millisecond, "expected pendingAsyncDeletions to reach 1")

	// Flush two more segments (not pinned by any view).
	require.NoError(t, bucketB.Put([]byte("c"), []byte("3")))
	require.NoError(t, bucketB.FlushAndSwitch())
	require.NoError(t, bucketB.Put([]byte("d"), []byte("4")))
	require.NoError(t, bucketB.FlushAndSwitch())

	// Second compaction: compacts seg_c + seg_d → merged_cd.
	// launchOrSyncDelete sees pending(1) >= limit(1), falls back to sync.
	// seg_c and seg_d are NOT pinned by any view, so sync deletion finishes
	// immediately (no HOL block expected here).
	compactionDone := make(chan error, 1)
	go func() {
		_, err := bucketB.disk.compactOnce()
		compactionDone <- err
	}()

	select {
	case err := <-compactionDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second compaction timed out; sync fallback may have been blocked unexpectedly")
	}

	// Only the first async goroutine is still in-flight (waiting for the view
	// to be released). The second deletion ran synchronously — counter unchanged.
	assert.EqualValues(t, 1, bucketB.disk.pendingAsyncDeletions.Load(),
		"pendingAsyncDeletions must not exceed 1")

	// Release the view — the stuck async goroutine can now proceed.
	releaseB()

	// Drain all in-flight async deletions.
	bucketB.disk.asyncDeletionWg.Wait()

	assert.EqualValues(t, 0, bucketB.disk.pendingAsyncDeletions.Load(),
		"all async deletions should have drained after releasing the view")

	// Safe to shut down now that all async goroutines have exited.
	require.NoError(t, bucketB.Shutdown(ctx))
}
