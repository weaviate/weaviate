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
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestCreateSnapshotAndOpenBucket(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	for i := range 100 {
		key := make([]byte, 8)
		key[7] = byte(i)
		require.NoError(t, bucket.Put(key, []byte("value")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "test")
	require.NoError(t, err)
	require.True(t, IsSnapshotDir(snapshotDir))

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	require.Equal(t, 100, cursorCount(t, snapBucket))
}

func TestCreateSnapshotMultipleSegments(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	batchSize := 33
	for i := range 100 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("value")))
		if (i+1)%batchSize == 0 && i+1 < 100 {
			require.NoError(t, bucket.FlushAndSwitch())
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())
	require.Equal(t, 100, cursorCount(t, bucket))

	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "test")
	require.NoError(t, err)

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	require.Equal(t, 100, cursorCount(t, snapBucket))
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

	// Reads must work.
	require.Equal(t, 1, cursorCount(t, snapBucket))

	// Writes must be rejected.
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
			// Clean up so GlobalBucketRegistry doesn't complain on next open.
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

	// Write 50 keys and flush to disk so the snapshot captures them.
	for i := range 50 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("original")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// Take a snapshot.
	snapshotDir, err := bucket.CreateSnapshot(ctx, t.TempDir(), "isolation")
	require.NoError(t, err)

	snapBucket, err := NewSnapshotBucket(ctx, snapshotDir, testLogger(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer snapBucket.Shutdown(ctx)

	// --- Mutate the live bucket after the snapshot was taken ---

	// 1. Overwrite existing keys with a new value.
	for i := range 50 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("updated")))
	}

	// 2. Insert 50 new keys (50-99).
	for i := 50; i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, []byte("new")))
	}

	// 3. Delete a key.
	deleteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(deleteKey, 0)
	require.NoError(t, bucket.Delete(deleteKey))

	// 4. Flush so all mutations are on disk segments of the live bucket.
	require.NoError(t, bucket.FlushAndSwitch())

	// --- Verify the snapshot is completely unaffected ---

	// Count: snapshot should still see exactly 50 keys.
	require.Equal(t, 50, cursorCount(t, snapBucket))

	// Values: every key in the snapshot should have the original value.
	c := snapBucket.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		require.Equal(t, []byte("original"), v,
			"snapshot value for key %v should be the original, not the updated value", k)
	}

	// The live bucket should reflect all mutations: 99 keys (50 updated +
	// 50 new − 1 deleted).
	require.Equal(t, 99, cursorCount(t, bucket))
}

func TestSnapshotsRootCleanup(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	// Simulate the on-disk layout: snapshots go into a shared root dir.
	snapshotsRoot := filepath.Join(t.TempDir(), SnapshotsRootDir)
	bucketDir := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, bucketDir, "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	require.NoError(t, bucket.Put([]byte("key"), []byte("val")))
	require.NoError(t, bucket.FlushAndSwitch())

	snapshotDir, err := bucket.CreateSnapshot(ctx, snapshotsRoot, "leftover")
	require.NoError(t, err)

	_, err = os.Stat(snapshotDir)
	require.NoError(t, err, "snapshot dir should exist before cleanup")

	// Simulate what NewIndex does on startup: remove the entire snapshots root.
	require.NoError(t, os.RemoveAll(snapshotsRoot))

	_, err = os.Stat(snapshotDir)
	require.True(t, os.IsNotExist(err), "snapshot dir should be gone after removing snapshots root")

	_, err = os.Stat(snapshotsRoot)
	require.True(t, os.IsNotExist(err), "snapshots root itself should be gone")
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
