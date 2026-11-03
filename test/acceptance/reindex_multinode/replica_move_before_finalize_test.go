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

package reindex_multinode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// A replica move before the swap's finalize must adopt the moved record,
// or the target serves an empty index.
func TestMultiNode_ReplicaMoveBeforeFinalizeKeepsMigratedIndex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t,
		"USE_INVERTED_SEARCHABLE", "false",
		"REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "MoveBeforeFinalize"
	restURI := restURIOf(compose, 1)
	createCollection(t, compose, restURI, className, 1, 1, textProps("text"))
	importObjects(t, restURI, className, testDocuments)

	// Field tokenization matches a whole value, never a single word.
	requireFieldTokenized := func(t *testing.T, uri string) {
		t.Helper()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			whole, err := runBM25QueryOnNode(t, uri, className, testDocuments[0])
			require.NoError(c, err)
			word, err := runBM25QueryOnNode(t, uri, className, "alpha")
			require.NoError(c, err)
			require.Len(c, whole, 1, "the whole document via %s", uri)
			require.Empty(c, word, "one of its words via %s", uri)
		}, 60*time.Second, 200*time.Millisecond)
	}

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))
	requireFieldTokenized(t, restURI)

	var state models.ReplicationShardingStateResponse
	require.True(t, httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s",
		restURI, className), &state) && state.ShardingState != nil)
	shard, sourceNode := state.ShardingState.Shards[0].Shard, state.ShardingState.Shards[0].Replicas[0]
	targetNode, targetIdx := docker.Weaviate1, 2
	if sourceNode == targetNode {
		targetNode, targetIdx = docker.Weaviate2, 3
	}

	helper.SetupClient(restURI)
	moveType := "MOVE"
	res, err := helper.Client(t).Replication.Replicate(replicationclient.NewReplicateParams().
		WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &className, Shard: &shard, SourceNode: &sourceNode,
			TargetNode: &targetNode, Type: &moveType,
		}), nil)
	require.NoError(t, err)
	opID := *res.GetPayload().ID
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		details, err := helper.Client(t).Replication.ReplicationDetails(
			replicationclient.NewReplicationDetailsParams().WithID(opID), nil)
		require.NoError(c, err)
		require.Equal(c, "READY", details.Payload.Status.State)
	}, 180*time.Second, 200*time.Millisecond)

	for i := 1; i <= 3; i++ {
		requireFieldTokenized(t, restURIOf(compose, i))
	}

	// The renamed record is on disk, so the target's next load must agree.
	require.NoError(t, compose.RestartAt(ctx, targetIdx-1, nil))
	requireFieldTokenized(t, restURIOf(compose, targetIdx))
}
