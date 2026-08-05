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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const releaseGuardOpID = "00000000-0000-0000-0000-0000000000d0"

// IncomingReleaseReplicaSnapshot holds no shutdown pin, so an unload can take
// the shard out of i.shards while the release is resuming it. The unload's
// shardCreateLocks write lock has to fence the release: without it the release
// resumes a shard that is being torn down, and for a lazy shard the resume's
// Load rebuilds it outside i.shards, where nothing will ever shut it down.
func TestReleaseReplicaSnapshotFencedByUnload(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()

	require.NoError(t, shard.HaltForTransfer(ctx, replicaHaltOwner(releaseGuardOpID), false, 0))
	index.recordReplicaSnapshot(releaseGuardOpID, replicaSnapshotState{shardName: "shard1"})

	// Stand in for the window UnloadLocalShard holds this lock across.
	index.shardCreateLocks.Lock("shard1")

	released := make(chan error, 1)
	enterrors.GoWrapper(func() {
		released <- index.IncomingReleaseReplicaSnapshot(ctx, releaseGuardOpID)
	}, index.logger)

	select {
	case err := <-released:
		index.shardCreateLocks.Unlock("shard1")
		t.Fatalf("release reached the shard while an unload held its create lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	_, ok := index.shards.LoadAndDelete("shard1")
	require.True(t, ok)
	require.NoError(t, shard.Shutdown(ctx))
	index.shardCreateLocks.Unlock("shard1")

	require.NoError(t, <-released)
	require.Nil(t, index.shards.Load("shard1"),
		"release must not put a shard back after the unload removed it")
}

// A release landing after Index.Shutdown must report the shutdown rather than
// claim success: index teardown purges the replica-snapshot registry, so an
// unknown op on a closed index is ambiguous, not a completed release.
func TestReleaseReplicaSnapshotAfterIndexShutdown(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()

	require.NoError(t, shard.HaltForTransfer(ctx, replicaHaltOwner(releaseGuardOpID), false, 0))
	index.recordReplicaSnapshot(releaseGuardOpID, replicaSnapshotState{shardName: "shard1"})

	require.NoError(t, index.Shutdown(ctx))

	err := index.IncomingReleaseReplicaSnapshot(ctx, releaseGuardOpID)
	require.ErrorIs(t, err, errAlreadyShutdown)
}
