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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCompactionNotHOLBlocked verifies that a compaction on bucket B is not
// head-of-line blocked by an active consistent view (reader) on bucket A.
//
// Before the fix, deleteOldSegmentsFromDisk was called synchronously after
// switchInMemory(), causing the entire single-threaded compaction cycle to
// stall while waiting for reader refs on unrelated buckets to reach zero.
//
// After the fix, the deletion is dispatched to a background goroutine, so
// bucket B's compaction returns immediately even when bucket A still holds refs.
func TestCompactionNotHOLBlocked(t *testing.T) {
	ctx := t.Context()

	// --- Bucket A: create two flushed segments, then hold a consistent view ---
	dirA := t.TempDir()
	bucketA, err := NewBucketCreator().NewBucket(ctx, dirA, dirA, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucketA.Shutdown(ctx)

	bucketA.SetMemtableThreshold(1e9)

	require.NoError(t, bucketA.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, bucketA.FlushAndSwitch())
	require.NoError(t, bucketA.Put([]byte("key2"), []byte("value2")))
	require.NoError(t, bucketA.FlushAndSwitch())

	// Acquire a consistent view on bucket A — this increments ref counts on its
	// two disk segments. The refs will not drop to zero until we call release().
	segsA, releaseA := bucketA.disk.getConsistentViewOfSegments()
	require.Len(t, segsA, 2, "expected 2 flushed segments in bucket A")

	// --- Bucket B: create two flushed segments and compact them ---
	dirB := t.TempDir()
	bucketB, err := NewBucketCreator().NewBucket(ctx, dirB, dirB, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucketB.Shutdown(ctx)

	bucketB.SetMemtableThreshold(1e9)

	require.NoError(t, bucketB.Put([]byte("a"), []byte("1")))
	require.NoError(t, bucketB.FlushAndSwitch())
	require.NoError(t, bucketB.Put([]byte("b"), []byte("2")))
	require.NoError(t, bucketB.FlushAndSwitch())

	// Compact bucket B in a goroutine and measure how long it takes.
	// The compaction itself should finish promptly — the async delete goroutine
	// will wait for bucket A's refs, but that must NOT block compactOnce().
	compactionDone := make(chan struct{})
	var compactionErr error
	var compacted bool

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		compacted, compactionErr = bucketB.disk.compactOnce()
		close(compactionDone)
	}()

	// Bucket B's compaction should complete well within 2 seconds even though
	// bucket A's consistent view is still held (refs > 0).
	select {
	case <-compactionDone:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("compaction of bucket B was HOL-blocked by bucket A's consistent view")
	}

	require.NoError(t, compactionErr)
	assert.True(t, compacted, "expected bucket B compaction to produce a merged segment")

	// Now release bucket A's consistent view. The background deletion goroutine
	// inside bucket A (if any) will then proceed. We must wait for it before
	// the deferred Shutdown calls run.
	releaseA()

	// Wait for the compaction goroutine to fully finish (it already has, but be tidy).
	wg.Wait()
}
