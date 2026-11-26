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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// A bucket that is moved to another dir has to take its registration along: the
// dir it left must be free for a new bucket, and the dir it is in must be taken.
func TestStore_MovedBucketKeepsItsRegistration(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// move leaves the moved bucket at the "target" dir, under the returned
		// name, and nothing at the "moved" dir
		move func(t *testing.T, store *Store) (bucketName string)
	}{
		{
			name: "ReplaceBuckets",
			move: func(t *testing.T, store *Store) string {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
				require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
				require.NoError(t, store.ReplaceBuckets(ctx, "target", "moved"))
				return "target"
			},
		},
		{
			name: "RenameBucket",
			move: func(t *testing.T, store *Store) string {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
				store.Bucket("moved").UpdateStatus(storagestate.StatusReadOnly)
				require.NoError(t, store.RenameBucket(ctx, "moved", "target"))
				store.Bucket("target").UpdateStatus(storagestate.StatusReady)
				return "target"
			},
		},
		{
			name: "FinalizeBucketSwap",
			move: func(t *testing.T, store *Store) string {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
				require.NoError(t, store.FinalizeBucketSwap(ctx, "moved",
					store.bucketDir("target"), store.bucketDir("moved"), store.bucketDir("target_bak")))
				return "moved"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStoreForDrain(t)
			t.Cleanup(func() { _ = store.Shutdown(ctx) })
			bucketName := tt.move(t, store)
			targetDir := store.bucketDir("target")
			movedDir := store.bucketDir("moved")
			require.Equal(t, targetDir, store.Bucket(bucketName).GetDir())

			require.ErrorIs(t, GlobalBucketRegistry.TryAdd(targetDir), ErrBucketAlreadyRegistered,
				"the dir the bucket is in must be registered")

			require.NoError(t, GlobalBucketRegistry.TryAdd(movedDir), "the dir the bucket has left must be free")
			GlobalBucketRegistry.Remove(movedDir)

			require.NoError(t, store.ShutdownBucket(ctx, bucketName))
			require.NoError(t, GlobalBucketRegistry.TryAdd(targetDir), "a bucket shut down must free its dir")
			GlobalBucketRegistry.Remove(targetDir)
		})
	}
}

// A move onto a dir another bucket has registered must fail before anything is
// renamed, and leave the moved bucket where it was, still registered.
func TestStore_MoveOntoRegisteredDirFails(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// move tries to move the "moved" bucket to the "target" dir
		move func(store *Store) error
	}{
		{
			name: "RenameBucket",
			move: func(store *Store) error {
				store.Bucket("moved").UpdateStatus(storagestate.StatusReadOnly)
				return store.RenameBucket(ctx, "moved", "target")
			},
		},
		{
			name: "FinalizeBucketSwap",
			move: func(store *Store) error {
				return store.FinalizeBucketSwap(ctx, "moved",
					store.bucketDir("target"), store.bucketDir("moved"), store.bucketDir("target_bak"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStoreForDrain(t)
			t.Cleanup(func() { _ = store.Shutdown(ctx) })
			require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
			require.NoError(t, store.Bucket("moved").Put([]byte("key"), []byte("value")))
			targetDir := store.bucketDir("target")
			movedDir := store.bucketDir("moved")
			require.NoError(t, GlobalBucketRegistry.TryAdd(targetDir), "stands in for a bucket of another store")
			t.Cleanup(func() { GlobalBucketRegistry.Remove(targetDir) })

			require.ErrorIs(t, tt.move(store), ErrBucketAlreadyRegistered)

			_, err := os.Stat(targetDir)
			require.True(t, os.IsNotExist(err), "nothing must be moved onto the registered dir")
			bucket := store.Bucket("moved")
			require.NotNil(t, bucket)
			require.Equal(t, movedDir, bucket.GetDir())
			require.ErrorIs(t, GlobalBucketRegistry.TryAdd(movedDir), ErrBucketAlreadyRegistered,
				"the bucket must stay registered at its dir")
			require.ErrorIs(t, GlobalBucketRegistry.TryAdd(targetDir), ErrBucketAlreadyRegistered,
				"a failed move must not release a dir it did not claim")
		})
	}
}
