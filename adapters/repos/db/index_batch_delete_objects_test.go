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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
)

// TestBatchDeleteObjectsReportsEveryID asserts that every id handed to the call
// comes back, whether its shard group ran or panicked. An id left out reaches
// the client as a successful delete of fewer objects than were matched, and an
// id reported without its own value cannot be encoded into a gRPC reply.
func TestBatchDeleteObjectsReportsEveryID(t *testing.T) {
	className := "BatchDeleteEveryID"
	const panickingShardA, panickingShardB = "panicking-shard-a", "panicking-shard-b"

	tests := []struct {
		name string
		// panickingShards are groups whose write replica lookup panics
		panickingShards []string
		// withHealthyShard adds the index's own shard as a group that runs
		withHealthyShard bool
	}{
		{
			name:             "a panicking group beside one that runs",
			panickingShards:  []string{panickingShardA},
			withHealthyShard: true,
		},
		{
			name:            "every group panics",
			panickingShards: []string{panickingShardA, panickingShardB},
		},
		{
			name:            "the only group panics",
			panickingShards: []string{panickingShardA},
		},
		{
			name: "no groups",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// the integration suite exports this, which would let the panic kill the test
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			idx, shard := refCountTestIndex(t, className)

			router := types.NewMockRouter(t)
			groups := map[string][]strfmt.UUID{}
			var wantFailed, wantDeleted []string

			for _, shardName := range test.panickingShards {
				router.EXPECT().GetWriteReplicasLocation(className, mock.Anything, shardName).
					RunAndReturn(func(string, string, string) (types.WriteReplicaSet, error) {
						panic("write replicas lookup panicked")
					})
				// more than one id, so a group reported as a single failure fails the test
				for range 2 {
					id := strfmt.UUID(uuid.NewString())
					groups[shardName] = append(groups[shardName], id)
					wantFailed = append(wantFailed, id.String())
				}
			}

			if test.withHealthyShard {
				router.EXPECT().GetWriteReplicasLocation(className, mock.Anything, shard.name).Return(
					types.WriteReplicaSet{
						Replicas: []types.Replica{{NodeName: "node1", ShardName: shard.name, HostAddr: "127.0.0.1"}},
					}, nil,
				)
				id := strfmt.UUID(uuid.NewString())
				groups[shard.name] = []strfmt.UUID{id}
				wantDeleted = append(wantDeleted, id.String())
			}
			idx.router = router

			objs, err := idx.batchDeleteObjects(t.Context(), groups, time.Now(), false, nil, 0, "")
			require.NoError(t, err)

			var failed, deleted []string
			for _, obj := range objs {
				if obj.Err != nil {
					require.ErrorContains(t, obj.Err, "an unexpected error occurred")
					failed = append(failed, obj.UUID.String())
					continue
				}
				deleted = append(deleted, obj.UUID.String())
			}
			require.ElementsMatch(t, wantFailed, failed,
				"every id of a panicking group must be reported as failed, under its own id")
			require.ElementsMatch(t, wantDeleted, deleted,
				"a group that ran must report its own deletions")
		})
	}
}
