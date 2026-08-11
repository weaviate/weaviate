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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/replica"
)

// twoPhaseVerbs are the second-phase calls of a replicated write, each with the
// answer its shard already gives for a task it does not hold: a commit has no
// task to run and reports it gone, an abort has nothing to drop and is done.
var twoPhaseVerbs = []struct {
	name    string
	missing any
	call    func(ctx context.Context, idx *Index, shardName, requestID string) any
}{
	{
		name:    "commit",
		missing: nil,
		call: func(ctx context.Context, idx *Index, shardName, requestID string) any {
			return idx.CommitReplication(ctx, shardName, requestID)
		},
	},
	{
		name:    "abort",
		missing: replica.SimpleResponse{},
		call: func(ctx context.Context, idx *Index, shardName, requestID string) any {
			return idx.AbortReplication(ctx, shardName, requestID)
		},
	},
}

// A replicated write prepares on one call and commits or aborts on another. The
// prepared task lives only in the loaded shard, so a shard unloaded between the
// two phases has lost it, and building one to answer with would produce a shard
// that never held the task.
func TestTwoPhaseCommit_UnloadedShardIsNotReloaded(t *testing.T) {
	const requestID = "req-unloaded"

	for _, tc := range twoPhaseVerbs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, shardName, _ := setupReplayShard(t)

			prepared := idx.ReplicateObject(ctx, shardName, requestID, replayTestObject(uuid.NewString()), 0)
			require.Empty(t, prepared.Errors)

			require.NoError(t, idx.UnloadLocalShard(ctx, shardName))
			require.Nil(t, idx.shards.Load(shardName), "shard must be unloaded before the call")

			require.Equal(t, tc.missing, tc.call(ctx, idx, shardName, requestID))
			require.Nil(t, idx.shards.Load(shardName), "the call must not rebuild the shard")
		})
	}
}

// Those answers are not reserved for an unloaded shard: a loaded shard gives
// the same ones for a request id it does not hold, which is what makes them
// readable by a caller that never learns why the task is gone.
func TestTwoPhaseCommit_UnknownRequestAnswersTheSameOnALoadedShard(t *testing.T) {
	for _, tc := range twoPhaseVerbs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, shardName, _ := setupReplayShard(t)

			require.NotNil(t, idx.shards.Load(shardName), "shard must be loaded for this call")
			require.Equal(t, tc.missing, tc.call(ctx, idx, shardName, "req-never-prepared"))
		})
	}
}

// Reading the loaded shard rather than loading one may not cost a commit the
// write it was holding.
func TestTwoPhaseCommit_LoadedShardStillRunsTheTask(t *testing.T) {
	const requestID = "req-loaded"
	ctx := testCtx()
	_, idx, shardName, _ := setupReplayShard(t)

	id := uuid.NewString()
	prepared := idx.ReplicateObject(ctx, shardName, requestID, replayTestObject(id), 0)
	require.Empty(t, prepared.Errors)

	committed, ok := idx.CommitReplication(ctx, shardName, requestID).(replica.SimpleResponse)
	require.True(t, ok, "a commit that ran the task returns a replica.SimpleResponse")
	require.Empty(t, committed.Errors)

	found, err := idx.objectByID(ctx, strfmt.UUID(id), search.SelectProperties{}, additional.Properties{}, nil, "")
	require.NoError(t, err)
	require.NotNil(t, found, "the committed object must be readable")
}
