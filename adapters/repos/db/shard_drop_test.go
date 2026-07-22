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

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Readers and writers hold a shard via preventShutdown while using it, without
// holding any lock. Shutdown honors that reference; drop used to ignore it, so a
// tenant or class delete would remove LSM segments out from under an in-flight
// read - garbage reads or SIGBUS, not a clean error.
func TestShardDropHonorsInUseRefcount(t *testing.T) {
	ctx := context.Background()

	t.Run("drop refuses while the shard is in use", func(t *testing.T) {
		shard, index := testShard(t, ctx, "TestClass")
		t.Cleanup(func() { index.Shutdown(ctx) })

		release, err := shard.preventShutdown()
		require.NoError(t, err)

		require.Error(t, shard.Shutdown(ctx),
			"sanity: Shutdown must refuse while the shard is held in use")

		dropErr := shard.drop(false)
		release()

		require.Error(t, dropErr, "drop must refuse while the shard is held in use")
	})

	t.Run("drop succeeds on an idle shard", func(t *testing.T) {
		shard, index := testShard(t, ctx, "TestClass")
		t.Cleanup(func() { index.Shutdown(ctx) })

		require.NoError(t, shard.drop(false))
	})

	// refCountSub must not start a second teardown behind the drop's back
	t.Run("drop drains a reference released while it waits", func(t *testing.T) {
		shard, index := testShard(t, ctx, "TestClass")
		t.Cleanup(func() { index.Shutdown(ctx) })

		release, err := shard.preventShutdown()
		require.NoError(t, err)

		dropErr := make(chan error, 1)
		go func() { dropErr <- shard.drop(false) }()

		require.Eventually(t, func() bool {
			rel, err := shard.preventShutdown()
			if err == nil {
				rel()
				return false
			}
			return errors.Is(err, errDropInProgress)
		}, 2*time.Second, 10*time.Millisecond, "drop must refuse new users while it drains")

		release()
		require.NoError(t, <-dropErr, "drop must complete once the last reference is gone")
	})

	t.Run("a refused drop reopens the shard and can be retried", func(t *testing.T) {
		shard, index := testShard(t, ctx, "TestClass")
		t.Cleanup(func() { index.Shutdown(ctx) })

		release, err := shard.preventShutdown()
		require.NoError(t, err)

		require.Error(t, shard.drop(false), "sanity: the held reference blocks the drop")
		release()

		// teardown never started, so the shard must still be serviceable
		secondRelease, err := shard.preventShutdown()
		require.NoError(t, err, "a refused drop must reopen the shard, not wedge it")
		secondRelease()

		require.NoError(t, shard.drop(false))
	})
}
