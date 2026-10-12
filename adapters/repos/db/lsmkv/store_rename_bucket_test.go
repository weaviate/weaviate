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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// A rename that fails on disk must leave the bucket as it was: under its old
// name, in its old dir, serving its data, and renamable once the cause is gone.
func TestStore_RenameBucketFailingOnDiskLeavesBucketUnchanged(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
	require.NoError(t, store.Bucket("moved").Put([]byte("key"), []byte("value")))
	require.NoError(t, store.Bucket("moved").FlushAndSwitch())
	movedDir := store.bucketDir("moved")
	targetDir := store.bucketDir("target")
	blocker := filepath.Join(targetDir, "blocker")
	require.NoError(t, os.MkdirAll(blocker, 0o700), "a non-empty dir makes the rename fail")

	store.Bucket("moved").UpdateStatus(storagestate.StatusReadOnly)
	require.Error(t, store.RenameBucket(ctx, "moved", "target"))

	require.Nil(t, store.Bucket("target"))
	bucket := store.Bucket("moved")
	require.NotNil(t, bucket)
	require.Equal(t, movedDir, bucket.GetDir())
	val, err := bucket.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
	require.ErrorIs(t, GlobalBucketRegistry.TryAdd(movedDir), ErrBucketAlreadyRegistered)
	require.NoError(t, GlobalBucketRegistry.TryAdd(targetDir))
	GlobalBucketRegistry.Remove(targetDir)

	require.NoError(t, os.RemoveAll(targetDir))
	require.NoError(t, store.RenameBucket(ctx, "moved", "target"))
	bucket = store.Bucket("target")
	require.NotNil(t, bucket)
	require.Equal(t, targetDir, bucket.GetDir())
	val, err = bucket.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
}
