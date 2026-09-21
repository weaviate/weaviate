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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// A bucket that is moved to another dir has to take its registration along: the
// dir it left must be free for a new bucket, and the dir it is in must be taken.
func TestStore_MovedBucketKeepsItsRegistration(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// move leaves a bucket named "target", and nothing at the "moved" name
		move func(t *testing.T, store *Store)
	}{
		{
			name: "ReplaceBuckets",
			move: func(t *testing.T, store *Store) {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
				require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
				require.NoError(t, store.ReplaceBuckets(ctx, "target", "moved"))
			},
		},
		{
			name: "RenameBucket",
			move: func(t *testing.T, store *Store) {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
				store.Bucket("moved").UpdateStatus(storagestate.StatusReadOnly)
				require.NoError(t, store.RenameBucket(ctx, "moved", "target"))
				store.Bucket("target").UpdateStatus(storagestate.StatusReady)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStoreForDrain(t)
			t.Cleanup(func() { _ = store.Shutdown(ctx) })
			tt.move(t, store)
			targetDir := store.Bucket("target").GetDir()

			require.ErrorIs(t, GlobalBucketRegistry.TryAdd(targetDir), ErrBucketAlreadyRegistered,
				"the dir the bucket is in must be registered")

			require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)),
				"the dir the bucket has left must be free")
			require.NoError(t, store.Bucket("moved").Put([]byte("key"), []byte("value")))

			require.NoError(t, store.ShutdownBucket(ctx, "target"))
			require.NoError(t, GlobalBucketRegistry.TryAdd(targetDir), "a bucket shut down must free its dir")
			GlobalBucketRegistry.Remove(targetDir)
		})
	}
}
