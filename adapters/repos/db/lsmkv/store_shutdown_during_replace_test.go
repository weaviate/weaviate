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

	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A ShutdownBucket that picks up the replacement by name while ReplaceBuckets
// is still moving it must not leave the dir the bucket ended up in registered.
func TestStore_ShutdownDuringReplaceFreesTargetDir(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })
	require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
	targetDir := store.bucketDir("target")
	replacement := store.Bucket("moved")

	// Holding a read on "target" stalls ReplaceBuckets in the old bucket's
	// Shutdown, after the replacement is under "target" and before its dir moves.
	_, release := store.AcquireBucketForRead("target")

	replaceErr := make(chan error, 1)
	enterrors.GoWrapper(func() { replaceErr <- store.ReplaceBuckets(ctx, "target", "moved") }, store.logger)
	require.Eventually(t, func() bool { return store.Bucket("moved") == nil }, 5*time.Second, time.Millisecond)

	shutdownErr := make(chan error, 1)
	enterrors.GoWrapper(func() { shutdownErr <- store.ShutdownBucket(ctx, "target") }, store.logger)
	require.Eventually(t, func() bool {
		if replacement.lifetimeLock.TryRLock() {
			replacement.lifetimeLock.RUnlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	release()
	require.NoError(t, <-replaceErr)
	require.NoError(t, <-shutdownErr)
	require.Nil(t, store.Bucket("target"))

	err := GlobalBucketRegistry.TryAdd(targetDir)
	GlobalBucketRegistry.Remove(targetDir)
	require.NoError(t, err, "no bucket is open at the target dir, so it must be free")
}
