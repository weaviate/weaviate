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
	"errors"
	"fmt"
	"net/http"
	"slices"
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

// Pins an empty replica added after a cancel serving the migrated index across later schema changes.
func TestMultiNode_EmptyReplicaAfterCancel_ServesTheMigratedIndex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t, "REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className, tenant = "EmptyReplicaAfterCancel", "t1"
	restURI := compose.GetWeaviate().URI()
	// Factor 1: replicas can be removed only down to the factor, and the new one must answer alone.
	createMTCollection(t, restURI, className, 1, textProps("text"), map[string]interface{}{
		"asyncEnabled": true,
		"asyncConfig":  map[string]any{"frequency": 5000, "frequencyWhilePropagating": 1000, "propagationDelay": 1000},
	})
	createTenants(t, restURI, className, []string{tenant, "large"})
	ids := []string{uuid.NewString(), uuid.NewString()}
	postBatchToEveryReplica(t, restURI, []map[string]any{
		{"class": className, "id": ids[0], "tenant": tenant, "properties": map[string]any{"text": "The Hobbit"}},
		{"class": className, "id": ids[1], "tenant": tenant, "properties": map[string]any{"text": "other words"}},
	})
	importLargeTenant(t, restURI, className)
	migratedOn := func(uri string) bool { return tenantMigratedOn(t, uri, className, tenant, "The Hobbit", "Hobbit") }

	// The large tenant keeps the run cancellable after t1 swaps.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	require.Eventually(t, func() bool {
		return migratedOn(restURIOf(compose, 1)) && migratedOn(restURIOf(compose, 2)) && migratedOn(restURIOf(compose, 3))
	}, 2*time.Minute, 200*time.Millisecond, "t1 must swap")
	require.Equal(t, "STARTED", reindexTaskStatus(restURI, taskID), "t1 must swap while the large tenant reindexes")
	reindexhelpers.CancelIndex(t, restURI, className, "text", "searchable")
	require.Eventually(t, func() bool {
		for _, prop := range reindexhelpers.GetIndexes(t, restURI, className).Properties {
			for _, idx := range prop.Indexes {
				if prop.Name == "text" && idx.Type == "searchable" {
					return idx.Status == "partial" && idx.MigratedTenants == 1 && idx.TotalTenants == 2
				}
			}
		}
		return false
	}, time.Minute, 500*time.Millisecond, "the indexes endpoint must read partial, 1 of 2 tenants")

	replicasOfTenant := func(uri string) []string {
		var state models.ReplicationShardingStateResponse
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s&shard=%s",
			uri, className, tenant), &state) || state.ShardingState == nil || len(state.ShardingState.Shards) != 1 {
			return nil
		}
		return state.ShardingState.Shards[0].Replicas
	}
	scale := func(actions models.ReplicationScalePlanShardScaleActionsAnon) {
		helper.SetupClient(restURI)
		_, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
			replicationclient.NewApplyReplicationScalePlanParams().WithBody(&models.ReplicationScalePlan{
				PlanID: strfmt.UUID(uuid.NewString()), Collection: className,
				ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{tenant: actions},
			}), nil)
		var refused *replicationclient.ApplyReplicationScalePlanInternalServerError
		if errors.As(err, &refused) && refused.Payload != nil && len(refused.Payload.Error) > 0 {
			t.Fatalf("scale plan refused: %s", refused.Payload.Error[0].Message)
		}
		require.NoError(t, err)
	}
	// moveToNewReplica swaps t1 onto an empty replica on a fresh node and returns that node.
	held := replicasOfTenant(restURI)
	require.Len(t, held, 1)
	moveToNewReplica := func() int {
		owners := replicasOfTenant(restURI)
		node := slices.IndexFunc([]string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2},
			func(n string) bool { return !slices.Contains(held, n) })
		require.GreaterOrEqual(t, node, 0)
		name, uri := fmt.Sprintf("weaviate-%d", node), restURIOf(compose, node+1)
		held = append(held, name)
		scale(models.ReplicationScalePlanShardScaleActionsAnon{AddNodes: map[string]string{name: ""}})
		// Ask the new replica itself; a node that has not applied the add reads from the old owner.
		require.Eventually(t, func() bool {
			var nodes models.NodesStatusResponse
			if !httpGetJSON(fmt.Sprintf("http://%s/v1/nodes/%s?output=verbose", uri, className), &nodes) {
				return false
			}
			return slices.ContainsFunc(nodes.Nodes, func(n *models.NodeStatus) bool {
				return n.Name == name && slices.ContainsFunc(n.Shards, func(s *models.NodeShardStatus) bool {
					return s.Name == tenant
				})
			})
		}, 30*time.Second, 200*time.Millisecond, "the new replica must hold a shard of t1")
		require.Eventually(t, func() bool {
			return !slices.ContainsFunc(ids, func(id string) bool {
				resp, err := http.Get(fmt.Sprintf("http://%s/v1/objects/%s/%s?tenant=%s&node_name=%s",
					uri, className, id, tenant, name))
				if err != nil {
					return true
				}
				resp.Body.Close()
				return resp.StatusCode != http.StatusOK
			})
		}, 3*time.Minute, time.Second, "async replication must fill the new replica")
		scale(models.ReplicationScalePlanShardScaleActionsAnon{RemoveNodes: owners})
		require.Eventually(t, func() bool { return slices.Equal(replicasOfTenant(uri), []string{name}) },
			30*time.Second, 200*time.Millisecond, "only the new replica must be listed")
		return node
	}
	joined := moveToNewReplica()
	joinedURI := func() string { return restURIOf(compose, joined+1) }

	whole, err := runBM25QueryOnNodeTenant(t, joinedURI(), className, tenant, "The Hobbit")
	require.NoError(t, err)
	require.Len(t, whole, 1, "the new replica, alone, must hold t1's objects")
	require.True(t, migratedOn(joinedURI()),
		"the new replica, alone, must serve t1 with field tokenization before the schema changes")

	retry := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, retry, reindexhelpers.WithTimeout(10*time.Minute))
	reindexhelpers.AwaitTokenizationVisible(t, joinedURI(), className, "text", "field")
	require.True(t, migratedOn(joinedURI()), "the new replica must serve t1 after the schema changes")

	require.NoError(t, compose.StopNode(ctx, joined, nil))
	require.NoError(t, compose.StartNode(ctx, joined))
	restURI = restURIOf(compose, 1) // a restarted node gets a new port
	require.Eventually(t, func() bool { return migratedOn(joinedURI()) }, time.Minute, 500*time.Millisecond,
		"the new replica must serve t1 after it restarts")

	// Back to word, the tokenization the cancelled run started from, while that run is still listed.
	back := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"word"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, back, reindexhelpers.WithTimeout(10*time.Minute))
	last := moveToNewReplica()
	reindexhelpers.AwaitTokenizationVisible(t, restURIOf(compose, last+1), className, "text", "word")
	word, err := runBM25QueryOnNodeTenant(t, restURIOf(compose, last+1), className, tenant, "Hobbit")
	require.NoError(t, err)
	require.Len(t, word, 1, "a replica added after the change back to word must serve word")
}
