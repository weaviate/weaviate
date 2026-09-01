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

//go:build integrationTest

package lsmkv

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStore_SyncWALs covers the durability barrier primitive: syncing a named
// bucket must flush that bucket's commit-log buffers to its WAL file on disk
// (plus fsync), while a bucket that was NOT named must not be required to
// flush. An unknown bucket name is an error (documented on SyncWALs): callers
// use this as a crash-safety barrier, so silently skipping a misnamed bucket
// would void the guarantee.
func TestStore_SyncWALs(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_a", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_b", WithStrategy(StrategyReplace)))

	bucketA := store.Bucket("bucket_a")
	bucketB := store.Bucket("bucket_b")
	require.NotNil(t, bucketA)
	require.NotNil(t, bucketB)

	// walSize reports the on-disk size of the bucket's active WAL file. A
	// missing file counts as 0: the commit logger is lazily initialized and
	// small writes sit in its in-memory buffer until flushed.
	walSize := func(t *testing.T, b *Bucket) int64 {
		t.Helper()
		info, err := os.Stat(b.active.commitlogWalPath())
		if os.IsNotExist(err) {
			return 0
		}
		require.NoError(t, err)
		return info.Size()
	}

	require.NoError(t, bucketA.Put([]byte("key-a"), []byte("value-a")))
	require.NoError(t, bucketB.Put([]byte("key-b"), []byte("value-b")))

	// Both writes are small enough to still sit in the commit loggers'
	// in-memory buffers, i.e. nothing has reached the WAL files yet. This is
	// the precondition that makes the flush observable below.
	require.Zero(t, walSize(t, bucketA), "write to bucket_a expected to still be buffered")
	require.Zero(t, walSize(t, bucketB), "write to bucket_b expected to still be buffered")

	t.Run("sync flushes exactly the named bucket's WAL to disk", func(t *testing.T) {
		require.NoError(t, store.SyncWALs(ctx, "bucket_a"))

		require.Positive(t, walSize(t, bucketA),
			"bucket_a's WAL must contain the record after SyncWALs")
		require.Zero(t, walSize(t, bucketB),
			"bucket_b was not named, its buffer must not be required to flush")
	})

	t.Run("unknown bucket name is an error", func(t *testing.T) {
		err := store.SyncWALs(ctx, "bucket_a", "no_such_bucket")
		require.ErrorIs(t, err, ErrBucketNotFound)
	})

	t.Run("canceled context aborts", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := store.SyncWALs(canceledCtx, "bucket_b")
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, walSize(t, bucketB))
	})

	t.Run("SyncAllWALs flushes every bucket", func(t *testing.T) {
		require.NoError(t, store.SyncAllWALs(ctx))
		require.Positive(t, walSize(t, bucketB))
	})
}

// TestStore_SyncWALs_ClosedStore pins the error contract on an already-closed
// store, matching WriteWALs.
func TestStore_SyncWALs_ClosedStore(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_a", WithStrategy(StrategyReplace)))
	require.NoError(t, store.Shutdown(ctx))

	require.ErrorIs(t, store.SyncWALs(ctx, "bucket_a"), ErrAlreadyClosed)
	require.ErrorIs(t, store.SyncAllWALs(ctx), ErrAlreadyClosed)
}
