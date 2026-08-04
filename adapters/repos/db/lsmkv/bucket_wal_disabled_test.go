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
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestBucketWALDisabled_NoWALFilesCreated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write some data
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, b.Put([]byte("key2"), []byte("value2")))

	// Verify no .wal files exist
	assertNoWALFiles(t, dir)
}

func TestBucketWALDisabled_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write and read back
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, b.Put([]byte("key2"), []byte("value2")))

	val1, err := b.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := b.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), val2)
}

func TestBucketWALDisabled_WALEnabledRegression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Create a bucket WITH WAL enabled (default behavior)
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write data to trigger WAL creation
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))

	// Verify .wal files exist when WAL is enabled
	hasWAL := hasWALFiles(t, dir)
	assert.True(t, hasWAL, "expected .wal files to exist when WAL is enabled")
}

func TestBucketWALDisabled_RecoverySkipped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Create bucket with WAL disabled, write data, flush to segment, then reopen
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)

	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))

	// Force flush to segment
	require.NoError(t, b.FlushAndSwitch())

	// Shut down
	require.NoError(t, b.Shutdown(ctx))

	// Reopen the bucket with WAL disabled
	b2, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b2.Shutdown(ctx)

	// Data should be readable from flushed segments (no WAL recovery needed)
	val, err := b2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// TestBucketWALDisabled_FlushDrivers pins that the background flush drivers —
// memtable size threshold, dynamic memtable sizing, and the dirty-duration
// timeout — fire for WAL-disabled buckets. Durability for these buckets comes
// from the RAFT log, but flushing is still the only mechanism that releases
// memtable RAM and advances the flushed watermark that permits RAFT log
// compaction. A WAL-disabled bucket must therefore never be exempt from the
// size/dirty flush cadence (the WAL-reuse short-circuit is a WAL-only
// optimization).
func TestBucketWALDisabled_FlushDrivers(t *testing.T) {
	type testCase struct {
		name string
		opts []BucketOption
		// write fills the memtable according to the driver under test.
		write func(t *testing.T, b *Bucket)
		// waitBeforeCheck lets the dirty-duration driver become due.
		waitBeforeCheck time.Duration
		// wantThresholdAfter, when non-zero, asserts the dynamic-sizing
		// adjustment ran as part of the flush cycle.
		wantThresholdAfter uint64
	}

	putN := func(n, valSize int) func(t *testing.T, b *Bucket) {
		return func(t *testing.T, b *Bucket) {
			val := make([]byte, valSize)
			for i := 0; i < n; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%04d", i)), val))
			}
		}
	}

	tests := []testCase{
		{
			name: "memtable size threshold",
			opts: []BucketOption{WithMemtableThreshold(4 * 1024)},
			// ~16KiB of values, well past the 4KiB threshold
			write: putN(32, 512),
		},
		{
			name: "dirty duration timeout",
			opts: []BucketOption{WithDirtyThreshold(5 * time.Millisecond)},
			// far below any size threshold: only the dirty timer can flush this
			write:           putN(1, 64),
			waitBeforeCheck: 30 * time.Millisecond,
		},
		{
			name: "dynamic memtable sizing",
			// initial 1MiB, step 10MiB, max 21MiB, minDuration 1h: the flush
			// cycle is guaranteed shorter than minDuration, so NextTarget must
			// grow the threshold by one step after the flush.
			opts: []BucketOption{WithDynamicMemtableSizing(1, 21, 3600, 7200)},
			// ~2MiB of values, past the 1MiB initial threshold
			write:              putN(64, 32*1024),
			wantThresholdAfter: 11 * 1024 * 1024,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()

			opts := append([]BucketOption{
				WithStrategy(StrategyReplace),
				WithWALDisabled(),
			}, tc.opts...)
			b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				opts...)
			require.NoError(t, err)
			defer b.Shutdown(ctx)

			tc.write(t, b)
			if tc.waitBeforeCheck > 0 {
				time.Sleep(tc.waitBeforeCheck)
			}

			// The cycle-manager callback must flush and switch: this is what
			// runs periodically in production.
			b.flushAndSwitchIfThresholdsMet(nil)

			assert.Zero(t, b.active.Size(),
				"active memtable should be empty after a threshold-driven flush")
			assert.NotZero(t, countSegmentFiles(t, dir),
				"the flushed memtable should have produced a disk segment")
			if tc.wantThresholdAfter != 0 {
				assert.Equal(t, tc.wantThresholdAfter, b.memtableThreshold,
					"dynamic sizing should have adjusted the memtable threshold after the flush")
			}
		})
	}
}

// TestBucketWALDisabled_DurableRaftFloor pins the flush-watermark semantics
// that gate RAFT log compaction: the floor is the raft applied index captured
// at the last memtable seal whose flush has COMPLETED; a clean bucket imposes
// no cap; a dirty bucket that never flushed reports 0 (nothing durable).
func TestBucketWALDisabled_DurableRaftFloor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	var applied atomic.Uint64
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
		WithRaftIndexSource(applied.Load),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	assert.Equal(t, uint64(math.MaxUint64), b.DurableRaftFloor(),
		"a bucket with no un-flushed writes must impose no compaction cap")

	applied.Store(5)
	require.NoError(t, b.Put([]byte("k1"), []byte("v1")))
	assert.Zero(t, b.DurableRaftFloor(),
		"dirty but never flushed: nothing is durable yet")

	require.NoError(t, b.FlushAndSwitch())
	assert.Equal(t, uint64(math.MaxUint64), b.DurableRaftFloor(),
		"clean again after the flush: no cap")

	applied.Store(9)
	require.NoError(t, b.Put([]byte("k2"), []byte("v2")))
	assert.Equal(t, uint64(5), b.DurableRaftFloor(),
		"dirty again: the floor is the applied index sealed by the completed flush")

	require.NoError(t, b.FlushAndSwitch())
	applied.Store(12)
	require.NoError(t, b.Put([]byte("k3"), []byte("v3")))
	assert.Equal(t, uint64(9), b.DurableRaftFloor(),
		"the floor advances to the next completed seal")
}

// TestStoreWALDisabled_DurableRaftFloor pins the store-level aggregation: the
// snapshot cap is the MINIMUM across buckets, and clean buckets do not drag
// it down.
func TestStoreWALDisabled_DurableRaftFloor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	var applied atomic.Uint64
	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	opts := []BucketOption{
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
		WithRaftIndexSource(applied.Load),
	}
	require.NoError(t, store.CreateOrLoadBucket(ctx, "objects", opts...))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "props", opts...))

	assert.Equal(t, uint64(math.MaxUint64), store.DurableRaftFloor(),
		"all buckets clean: no cap")

	applied.Store(7)
	require.NoError(t, store.Bucket("objects").Put([]byte("k"), []byte("v")))
	require.NoError(t, store.Bucket("props").Put([]byte("k"), []byte("v")))
	require.NoError(t, store.Bucket("objects").FlushAndSwitch())
	applied.Store(11)
	require.NoError(t, store.Bucket("objects").Put([]byte("k2"), []byte("v2")))

	// objects: dirty with completed flush at 7; props: dirty, never flushed.
	assert.Zero(t, store.DurableRaftFloor(),
		"the never-flushed bucket must pin the store floor at 0")

	require.NoError(t, store.Bucket("props").FlushAndSwitch())
	// props is now clean (no cap); objects still dirty above its floor of 7.
	assert.Equal(t, uint64(7), store.DurableRaftFloor(),
		"the floor is the minimum across capped buckets only")
}

func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	var count int
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "segment-") && filepath.Ext(d.Name()) == ".db" {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

func assertNoWALFiles(t *testing.T, dir string) {
	t.Helper()
	assert.False(t, hasWALFiles(t, dir), "expected no .wal files in %s", dir)
}

func hasWALFiles(t *testing.T, dir string) bool {
	t.Helper()
	var found bool
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".wal" {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}
