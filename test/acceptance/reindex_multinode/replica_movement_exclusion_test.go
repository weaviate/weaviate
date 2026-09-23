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
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestMultiNode_ReindexAndReplicaMovementExcludeEachOther(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t,
		"USE_INVERTED_SEARCHABLE", "false",
		"REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	// 10,000 objects keep the reindex running past awaitReindexMidFlight and the two refused requests.
	const totalObjects = 10_000
	className, copyType := "ExclusiveOps", "COPY"
	restURI := restURIOf(compose, 1)

	createCollection(t, compose, restURI, className, 1, 1, textProps("title", "body"))
	batchImportMultiProp(t, restURI, className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"title": fmt.Sprintf("alpha bravo charlie %d", i),
			"body":  fmt.Sprintf("delta echo foxtrot %d", i),
		}
	})

	// createCollection waited for the class on every node, so one read suffices.
	var state models.ReplicationShardingStateResponse
	require.True(t, httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s",
		restURI, className), &state) && state.ShardingState != nil)
	require.Len(t, state.ShardingState.Shards, 1)
	require.Len(t, state.ShardingState.Shards[0].Replicas, 1, "fixture expects replication factor 1")
	shard, sourceNode := state.ShardingState.Shards[0].Shard, state.ShardingState.Shards[0].Replicas[0]

	// The target is never weaviate-0, which the test uses after the target stops, nor the shard's owner.
	targetNode, targetIdx := docker.Weaviate1, 1
	if sourceNode == targetNode {
		targetNode, targetIdx = docker.Weaviate2, 2
	}

	replicate := func(t *testing.T, uri string) error {
		helper.SetupClient(uri)
		_, err := helper.Client(t).Replication.Replicate(replicationclient.NewReplicateParams().
			WithBody(&models.ReplicationReplicateReplicaRequest{
				Collection: &className, Shard: &shard, SourceNode: &sourceNode,
				TargetNode: &targetNode, Type: &copyType,
			}), nil)
		return err
	}

	// The scale plan reaches Raft.ApplyReplicationScalePlan without the replicate handler, so it is checked too.
	scale := func(t *testing.T, uri string) error {
		helper.SetupClient(uri)
		_, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
			replicationclient.NewApplyReplicationScalePlanParams().
				WithBody(&models.ReplicationScalePlan{
					PlanID: strfmt.UUID(uuid.NewString()), Collection: className,
					ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{
						shard: {AddNodes: map[string]string{targetNode: sourceNode}},
					},
				}), nil)
		return err
	}

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "title", "searchable",
		`{"algorithm":"blockmax"}`)
	awaitReindexMidFlight(t, restURI, taskID, 120*time.Second)

	// A follower forwards the request, so the refusal must survive fromRPCError's string match.
	followerURI := restURIOf(compose, (raftLeaderIndex(t, compose)+1)%3+1)
	var conflict *replicationclient.ReplicateConflict
	require.ErrorAs(t, replicate(t, followerURI), &conflict,
		"a movement must be refused while the reindex runs")
	require.NotEmpty(t, conflict.Payload.Error)
	require.Contains(t, conflict.Payload.Error[0].Message, className)
	require.Contains(t, conflict.Payload.Error[0].Message, "retry after it completes")

	var scaleConflict *replicationclient.ApplyReplicationScalePlanConflict
	require.ErrorAs(t, scale(t, followerURI), &scaleConflict,
		"the scale plan must be refused too, and as a conflict rather than a server error")
	require.NotEmpty(t, scaleConflict.Payload.Error)
	require.Contains(t, scaleConflict.Payload.Error[0].Message, className)
	require.Contains(t, scaleConflict.Payload.Error[0].Message, "retry after it completes")

	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(300*time.Second))

	// The target is the only node that runs a movement, so stopping it keeps the op in flight.
	require.NoError(t, compose.StopNode(ctx, targetIdx, nil))
	// The stopped node may have been the leader.
	raftLeaderIndex(t, compose)
	require.NoError(t, replicate(t, restURI), "the movement must be admitted once the reindex is over")

	blocked := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, "body", "searchable",
		`{"algorithm":"blockmax"}`)
	require.Equal(t, http.StatusConflict, blocked.StatusCode, "body: %s", blocked.Body)
	require.Contains(t, blocked.Body, "replica movement")
}
