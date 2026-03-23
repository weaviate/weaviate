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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func testLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
}

func TestCreateSnapshotAndOpen(t *testing.T) {
	tests := []struct {
		name       string
		objects    int
		flushEvery int // 0 means flush once at the end
	}{
		{name: "single segment", objects: 100, flushEvery: 0},
		{name: "multiple segments", objects: 100, flushEvery: 33},
		{name: "single object", objects: 1, flushEvery: 0},
		{name: "large batch", objects: 1000, flushEvery: 250},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			noopCB := cyclemanager.NewCallbackGroupNoop()

			bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
				WithStrategy(StrategyReplace))
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)

			for i := range tc.objects {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(i))
				require.NoError(t, bucket.Put(key, []byte("value")))

				if tc.flushEvery > 0 && (i+1)%tc.flushEvery == 0 && i+1 < tc.objects {
					require.NoError(t, bucket.FlushAndSwitch())
				}
			}
			require.NoError(t, bucket.FlushAndSwitch())

			snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "test")
			require.NoError(t, err)
			require.True(t, IsSnapshotDir(snapshotDir))

			snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
				WithStrategy(StrategyReplace))
			require.NoError(t, err)
			defer snapBucket.Shutdown(ctx)

			require.Equal(t, tc.objects, cursorCount(t, snapBucket))
		})
	}
}

func TestSnapshotBucketReadOnly(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "readonly-test")
	require.NoError(t, err)

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	require.Equal(t, 1, cursorCount(t, snapBucket))

	assert.ErrorIs(t, snapBucket.Put([]byte("k"), []byte("v")), ErrReadOnly)
	assert.ErrorIs(t, snapBucket.Delete([]byte("k")), ErrReadOnly)
	assert.ErrorIs(t, snapBucket.FlushAndSwitch(), ErrReadOnly)
}

func TestSnapshotDirValidation(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	t.Run("CreateSnapshot returns path with prefix", func(t *testing.T) {
		require.NoError(t, bucket.Put([]byte("k"), []byte("v")))
		require.NoError(t, bucket.FlushAndSwitch())

		snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "my-snap")
		require.NoError(t, err)
		defer func() {
			sb, _ := NewSnapshotBucket(ctx, snapshotDir, testLogger(), WithStrategy(StrategyReplace))
			if sb != nil {
				sb.Shutdown(ctx)
			}
		}()

		assert.True(t, strings.HasPrefix(filepath.Base(snapshotDir), SnapshotDirPrefix))
	})

	t.Run("NewSnapshotBucket rejects dir without prefix", func(t *testing.T) {
		badDir := filepath.Join(t.TempDir(), "not-a-snapshot")
		_, err := NewSnapshotBucket(ctx, badDir, testLogger(), WithStrategy(StrategyReplace))
		require.Error(t, err)
		assert.Contains(t, err.Error(), SnapshotDirPrefix)
	})

	t.Run("NewBucket rejects snapshot dir", func(t *testing.T) {
		snapDir := filepath.Join(t.TempDir(), SnapshotDirPrefix+"test")
		_, err := NewBucketCreator().NewBucket(ctx, snapDir, "", testLogger(), nil, noopCB, noopCB,
			WithStrategy(StrategyReplace))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot directory")
	})
}

func TestSnapshotIsolation(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	for i := range 50 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("original")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "isolation")
	require.NoError(t, err)

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	// Mutate the live bucket: overwrite, insert new, delete.
	for i := range 50 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("updated")))
	}
	for i := 50; i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("new")))
	}
	deleteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(deleteKey, 0)
	require.NoError(t, bucket.Delete(deleteKey))
	require.NoError(t, bucket.FlushAndSwitch())

	// Snapshot must be unaffected.
	require.Equal(t, 50, cursorCount(t, snapBucket))

	c := snapBucket.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		require.Equal(t, []byte("original"), v,
			"snapshot value for key %v should be the original", k)
	}

	require.Equal(t, 99, cursorCount(t, bucket))
}

func TestSnapshotsRootCleanup(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	snapshotsRoot := filepath.Join(t.TempDir(), SnapshotsRootDir)

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	require.NoError(t, bucket.Put([]byte("key"), []byte("val")))
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotDir, err := bucket.CreateSnapshot(ctx, snapshotsRoot, "leftover")
	require.NoError(t, err)

	_, err = os.Stat(snapshotDir)
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(snapshotsRoot))

	_, err = os.Stat(snapshotDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(snapshotsRoot)
	require.True(t, os.IsNotExist(err))
}

// TestSnapshotWALOnlyData verifies that HardlinkBucketFiles with
// includeWAL=true correctly captures data that lives only in the WAL (no
// segment files). This happens for small tenants where the bucket's Shutdown
// persists the memtable as a WAL rather than flushing to a segment (the
// shouldReuseWAL optimisation).
func TestSnapshotWALOnlyData(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	bucketDir := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, bucketDir, bucketDir,
		testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)

	for i := range 5 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("small")))
	}
	require.NoError(t, bucket.Shutdown(ctx))

	// Verify: .wal exists but no .db files.
	entries, err := os.ReadDir(bucketDir)
	require.NoError(t, err)
	hasWAL, hasDB := false, false
	for _, e := range entries {
		switch filepath.Ext(e.Name()) {
		case ".wal":
			hasWAL = true
		case ".db":
			hasDB = true
		}
	}
	require.True(t, hasWAL, "expected .wal file")
	require.False(t, hasDB, "expected no .db files — data should be WAL-only")

	// Snapshot from disk with includeWAL=true.
	snapshotDir := filepath.Join(t.TempDir(), SnapshotDirPrefix+"wal-only")
	require.NoError(t, HardlinkBucketFiles(bucketDir, snapshotDir, true))

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	require.Equal(t, 5, cursorCount(t, snapBucket))

	c := snapBucket.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		require.Equal(t, []byte("small"), v)
	}
}

// TestSnapshotConcurrentCreation verifies that two concurrent snapshot
// operations on the same bucket produce independent, correct snapshots.
func TestSnapshotConcurrentCreation(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	for i := range 100 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("value")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotsRoot := t.TempDir()
	errs := make([]error, 2)
	dirs := make([]string, 2)

	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dirs[idx], errs[idx] = bucket.CreateSnapshot(ctx, snapshotsRoot,
				fmt.Sprintf("concurrent-%d", idx))
		}(i)
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NotEqual(t, dirs[0], dirs[1])

	for i, dir := range dirs {
		snapBucket, err := NewSnapshotBucket(ctx, dir, testLogger(),
			WithStrategy(StrategyReplace))
		require.NoError(t, err, "snapshot %d", i)
		require.Equal(t, 100, cursorCount(t, snapBucket), "snapshot %d", i)
		require.NoError(t, snapBucket.Shutdown(ctx))
	}
}

// TestSnapshotNoFlushRaceDuringHardlink verifies that CreateSnapshot produces
// a point-in-time view even when the flush cycle is actively running. It uses
// a real CycleManager with a short tick interval and a low memtable threshold
// so the flush callback fires frequently — the same way it works in production.
//
// Concurrent writers feed data into the bucket while CreateSnapshot runs. The
// snapshot must contain all baseline data and must be a valid, openable bucket
// (no partial segments from a concurrent flush).
func TestSnapshotNoFlushRaceDuringHardlink(t *testing.T) {
	ctx := t.Context()

	logger := testLogger()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	flushCallbacks := cyclemanager.NewCallbackGroup("flush", logger, 1)

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		noopCB,         // compaction — noop is fine, CreateSnapshot pauses it anyway
		flushCallbacks, // flush — real group so Deactivate/Activate work
		WithStrategy(StrategyReplace),
		WithMemtableThreshold(1), // 1 byte — triggers flush on nearly every cycle
		WithDirtyThreshold(10*time.Millisecond), // very short — ensures the flush fires quickly
	)
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	// Start a CycleManager that drives the flush callback, just like production.
	flushCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(10*time.Millisecond),
		flushCallbacks.CycleCallback, logger,
	)
	flushCycle.Start()
	defer flushCycle.StopAndWait(ctx)

	// Write the baseline data and flush it to disk.
	const baselineObjects = 100
	for i := range baselineObjects {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("baseline")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// Start concurrent writers that continuously insert new keys.
	// These writes happen AFTER the baseline. The CycleManager will
	// trigger FlushAndSwitch via the callback — this is the operation
	// that used to race with CreateSnapshot's hardlink step.
	writersDone := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-writersDone:
					return
				default:
				}
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(baselineObjects+workerID*100_000+i))
				if err := bucket.Put(key, []byte("concurrent")); err != nil {
					return
				}
			}
		}(w)
	}

	// Take a snapshot while writers and the flush cycle are active.
	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "race-test")
	require.NoError(t, err)

	// Stop writers.
	close(writersDone)
	wg.Wait()

	// Open the snapshot and verify.
	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, logger,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	count := cursorCount(t, snapBucket)

	// The snapshot must contain at least the baseline. It may contain some
	// concurrent writes that landed before CreateSnapshot's internal flush,
	// but it must be a valid, openable bucket (no partial segments).
	require.GreaterOrEqual(t, count, baselineObjects,
		"snapshot must contain at least the baseline objects")

	// Verify all baseline keys are present with the correct value.
	c := snapBucket.Cursor()
	defer c.Close()
	baselineFound := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		id := binary.BigEndian.Uint64(k)
		if id < baselineObjects {
			require.Equal(t, []byte("baseline"), v,
				"baseline key %d should have original value", id)
			baselineFound++
		}
	}
	require.Equal(t, baselineObjects, baselineFound,
		"all baseline keys must be present in the snapshot")
}

func cursorCount(t *testing.T, b *Bucket) int {
	t.Helper()
	c := b.Cursor()
	defer c.Close()
	n := 0
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		n++
	}
	return n
}
