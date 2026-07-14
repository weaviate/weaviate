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
// (shard_init_lsm.go), which is the bucket the count bug affects. extraOpts lets
// a caller flip persistence-layout knobs (e.g. WithWriteMetadata) while keeping
// the count-relevant config fixed; it must be passed identically on reopen.
func openCountTestBucket(t *testing.T, ctx context.Context, dir string, extraOpts ...BucketOption) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	opts := append([]BucketOption{
		WithStrategy(StrategyReplace),
		WithCalcCountNetAdditions(true),
		WithLazySegmentLoading(false),
	}, extraOpts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	return b
}

// runFlushRacingShutdown drives the flush-vs-shutdown degrade end-to-end and
// asserts the reopened count is 1. One distinct key ("a") is written, flushed,
// overwritten once, then flushed again with shutdownRequested already set — the
// second flush hits the degrade path (no consistent view of the lower segments,
// so the new segment is precomputed against a nil baseline that miscounts the
// overwrite as net-new). The fix must complete that flush (so b.flushing clears
// and Shutdown does not hang) while refusing to persist the overstated CNA, so
// the count is recomputed against the real neighbors on reopen. extraOpts pins
// the persistence layout under test (nil = standalone .cna; WithWriteMetadata =
// consolidated .metadata) and is applied to both the racing bucket and the
// reopen.
func runFlushRacingShutdown(t *testing.T, extraOpts ...BucketOption) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	b := openCountTestBucket(t, ctx, dir, extraOpts...)
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

	b2 := openCountTestBucket(t, ctx, dir, extraOpts...)
	defer func() { require.NoError(t, b2.Shutdown(ctx)) }()
	count, err := b2.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"a flush racing shutdown must not persist an overstated count: one key "+
			"overwritten once is 1, not 2, after reopen")
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
	// the fix it reads 1. This variant runs with the default persistence layout
	// (standalone .cna file), so it guards the initCountNetAdditions gate.
	t.Run("flush_racing_shutdown", func(t *testing.T) {
		runFlushRacingShutdown(t)
	})

	// write_metadata_path is the same racing-flush-during-shutdown scenario, but
	// the bucket runs WithWriteMetadata(true) — the layout the objects bucket uses
	// when WriteMetadataFilesEnabled — so the CNA is bundled into the consolidated
	// .metadata file instead of a standalone .cna. That is a SECOND persistence
	// site: the degrade must also skip the .metadata write (initMetadata's
	// deferCountNetAdditions early-return), or the overstated count is persisted
	// there instead. flush_racing_shutdown above cannot catch a regression on this
	// path because with writeMetadata=false initMetadata never touches the CNA.
	// Without the initMetadata gate this reopens at Count==2; with it, Count==1.
	t.Run("write_metadata_path", func(t *testing.T) {
		runFlushRacingShutdown(t, WithWriteMetadata(true))
	})
}
