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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// openCountTestBucket opens (or reopens) a StrategyReplace bucket at a fixed dir
// with net-count-additions enabled — the same config the objects bucket uses
// (shard_init_lsm.go), which is the bucket the count bug affects.
func openCountTestBucket(t *testing.T, ctx context.Context, dir string) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithCalcCountNetAdditions(true),
		WithLazySegmentLoading(false))
	require.NoError(t, err)
	return b
}

// TestFlushDuringShutdown pins the flush-vs-shutdown count degrade
// (weaviate/weaviate#11980). A memtable flush that races bucket shutdown can no
// longer take a consistent view of the lower segments (the refusal that keeps
// shutdown's refcount wait race-free), so it precomputes the new segment against
// a nil baseline. For StrategyReplace, makeExistsOn(nil) reports every key as
// previously-unseen, so an overwrite is miscounted as a net-new insert. Before
// the fix that overstated count-net-additions was persisted and trusted on the
// next open, so Bucket.Count() stayed wrong across restart.
//
// The obvious fix (aborting the flush) is wrong: it strands the switched-out
// memtable in b.flushing and Bucket.Shutdown then waits forever for it. The fix
// completes the flush (b.flushing clears, shutdown does not hang) but skips
// persisting the CNA, so it is recomputed against the real segment neighbors on
// reopen.
func TestFlushDuringShutdown(t *testing.T) {
	// control_normal_flush proves the harness measures correctly: the identical
	// write pattern with a normal (non-racing) flush must count as 1 across a
	// close+reopen. It exercises the exact same reopen path as the racing case,
	// so if it is green the reopen mechanics are sound.
	t.Run("control_normal_flush", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()

		b := openCountTestBucket(t, ctx, dir)
		require.NoError(t, b.Put([]byte("a"), []byte("v1")))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Put([]byte("a"), []byte("v2"))) // overwrite once
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		b2 := openCountTestBucket(t, ctx, dir)
		defer func() { require.NoError(t, b2.Shutdown(ctx)) }()
		count, err := b2.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count,
			"one distinct key overwritten once must count as 1 after reopen")
	})

	// flush_racing_shutdown is the regression case: identical writes, but the
	// second flush runs with shutdownRequested already set — the degrade path.
	// Before the fix this persists CNA==2 and Count() reads 2 after reopen; with
	// the fix it reads 1. The bounded shutdown context turns a regression that
	// strands b.flushing into a fast DeadlineExceeded failure instead of hanging
	// the suite.
	t.Run("flush_racing_shutdown", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()

		b := openCountTestBucket(t, ctx, dir)
		require.NoError(t, b.Put([]byte("a"), []byte("v1")))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Put([]byte("a"), []byte("v2"))) // overwrite once, still in the active memtable

		// Simulate shutdown having begun: from this point getConsistentViewOfSegments
		// refuses with ErrShuttingDown, so the in-flight flush below can no longer
		// see the lower segments and hits the degrade path.
		b.disk.maintenanceLock.Lock()
		b.disk.shutdownRequested = true
		b.disk.maintenanceLock.Unlock()

		// The flush must still complete: it clears b.flushing so shutdown doesn't hang.
		require.NoError(t, b.FlushAndSwitch())

		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(shutdownCtx),
			"shutdown must complete promptly: the degrade completes the flush so "+
				"b.flushing is cleared and shutdown does not wait forever")

		b2 := openCountTestBucket(t, ctx, dir)
		defer func() { require.NoError(t, b2.Shutdown(ctx)) }()
		count, err := b2.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count,
			"a flush racing shutdown must not persist an overstated count: one key "+
				"overwritten once is 1, not 2, after reopen")
	})
}
