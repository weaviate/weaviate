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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// loadedShardForTest returns the sole *Shard of a freshly populated index.
func loadedShardForTest(t *testing.T, index *Index) *Shard {
	t.Helper()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})
	require.NotEmpty(t, shardName)

	shardLike, release, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, shardLike)
	release()

	lazy, ok := shardLike.(*LazyLoadShard)
	require.True(t, ok)
	require.NotNil(t, lazy.shard)
	return lazy.shard
}

// TestShardDropWaitsForInFlightUsers pins the drain: drop used to reach
// store.Shutdown without consulting the refcount Shutdown honours, so a batch
// already inside putObjectLSM kept writing to a store being torn down. Once
// Store.Shutdown deregisters buckets (v1.38+) the same window is a nil deref.
func TestShardDropWaitsForInFlightUsers(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	shard := loadedShardForTest(t, index)

	// stand in for a batch that is already inside storeObjectOfBatchInLSM
	release, err := shard.preventShutdown()
	require.NoError(t, err)

	dropped := make(chan error, 1)
	go func() { dropped <- shard.drop(false) }()

	// no progress while the batch is in flight...
	select {
	case err := <-dropped:
		t.Fatalf("drop completed while a user held a reference: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// ...and throughout it the bucket the batch is using stays reachable. This
	// is the exact dereference that used to panic.
	assert.Equal(t, shardDropping, shard.lifecycle.phase(),
		"drop must take the shard out of service immediately")
	require.NotNil(t, shard.store.Bucket(helpers.ObjectsBucketLSM),
		"objects bucket must stay reachable for an in-flight user")

	lateRelease, err := shard.preventShutdown()
	require.ErrorIs(t, err, errShutdownInProgress)
	require.NotNil(t, lateRelease, "release must never be nil, including on error")

	release()

	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("drop did not complete after the last user released")
	}

	assert.Equal(t, shardClosed, shard.lifecycle.phase())
	_, err = shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
}

// TestShardDropTimesOutRatherThanTearingDownUnderAUser pins the timeout
// trade-off: a shard that will not drain is quarantined and the error reported,
// rather than torn down underneath a running user.
func TestShardDropTimesOutRatherThanTearingDownUnderAUser(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	shard := loadedShardForTest(t, index)

	release, err := shard.preventShutdown()
	require.NoError(t, err)
	defer release()

	err = shard.drop(false)
	require.ErrorContains(t, err, "still in use")

	assert.Equal(t, shardDropping, shard.lifecycle.phase(),
		"a shard that failed to drain stays out of service")
	assert.NotNil(t, shard.store.Bucket(helpers.ObjectsBucketLSM),
		"the store must not be torn down under the surviving user")
}

// TestShardShutdownWaitsForInFlightUsers is the unload-path counterpart.
func TestShardShutdownWaitsForInFlightUsers(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	shard := loadedShardForTest(t, index)

	release, err := shard.preventShutdown()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- shard.Shutdown(context.Background()) }()

	select {
	case err := <-done:
		t.Fatalf("shutdown completed while a user held a reference: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	require.NotNil(t, shard.store.Bucket(helpers.ObjectsBucketLSM),
		"objects bucket must stay reachable for an in-flight user")

	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("shutdown did not complete after the last user released")
	}

	assert.Equal(t, shardClosed, shard.lifecycle.phase())
	_, err = shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
}
