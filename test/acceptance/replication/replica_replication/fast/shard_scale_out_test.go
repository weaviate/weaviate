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

package replication

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestReplicaMovementShardScaleOutParallelWrites: COPY/single-tenant
// analogue of TestReplicaMovementTenantParallelWrites. Scales a 3-shard
// collection rf=1→3 (six COPY ops, two per source shard) while parallel
// writes hammer every node at CL=ALL. AsyncEnabled=false so the CCL is
// the only path bringing in-flight writes onto a fresh replica — any
// lost write surfaces directly.
func (suite *ReplicationHappyPathTestSuite) TestReplicaMovementShardScaleOutParallelWrites() {
	t := suite.T()
	mainCtx := context.Background()

	clusterSize := 3
	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateEnv("REPLICATION_ENGINE_MAX_WORKERS", "10").
		Start(mainCtx)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()
	require.Nil(t, err)
	cluster := newComposeNodeSource(compose, clusterSize)

	helper.SetupClient(cluster.URIFor(1))
	paragraphClass := articles.ParagraphsClass()

	t.Run("create thrice-sharded single-tenant class at rf=1", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		paragraphClass.ShardingConfig = map[string]any{"desiredCount": 3}
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("wait for eventual consistency of schema", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 0; i < clusterSize; i++ {
				helper.SetupClient(cluster.URIFor(i + 1))
				consistency := false
				respSchema, err := helper.Client(t).Schema.SchemaDump(
					&schema.SchemaDumpParams{Consistency: &consistency}, nil,
				)
				assert.Nil(ct, err)
				if respSchema == nil {
					continue
				}
				assert.Len(ct, respSchema.Payload.Classes, 1,
					"expected 1 class in schema dump from node %d, got %d", i, len(respSchema.Payload.Classes))
			}
		}, 10*time.Second, 1*time.Second, "schema not consistent across all nodes")
	})
	helper.SetupClient(cluster.URIFor(1))

	t.Run("insert initial paragraphs", func(t *testing.T) {
		objs := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			objs[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		all := "ALL"
		params := batch.NewBatchObjectsCreateParams().WithConsistencyLevel(&all).WithBody(batch.BatchObjectsCreateBody{Objects: objs})
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.NoError(t, err, "failed to create initial batch of paragraphs: %s", err)
		for _, r := range resp.Payload {
			require.Nil(t, r.Result.Errors, "expected no errors in batch create response for paragraph %s: %+v", r.ID, r.Result.Errors)
		}
	})

	// Seed the parallel writer with the initial paragraphs (single-tenant, so
	// tenant is ""). writes is populated once the writer is stopped, below.
	seed := map[strfmt.UUID]string{}
	for i, id := range paragraphIDs {
		seed[id] = fmt.Sprintf("paragraph#%d", i)
	}
	stopWrites := startParallelWrites(t, mainCtx, cluster, paragraphClass.Class, "", seed)
	// Guard against the orphan-writer race: if any require.XXX between
	// here and the explicit stopWrites() call below Goexits the test,
	// the writer goroutine would otherwise outlive the test's *testing.T
	// and race against tRunner.func1's deferred cleanup. stopWrites is
	// idempotent so the explicit call later is still effective.
	defer stopWrites()
	var writes parallelWriteResult

	// Discover the rf=1 sharding state and the node list, then plan the
	// scale-out: every shard's lone replica is COPYed onto the two nodes that
	// do not yet host it.
	type nodeAddr struct{ name, uri string }
	type scaleOutMovement struct{ shard, source, target string }
	var nodeAddrs []nodeAddr
	var movements []scaleOutMovement
	t.Run("plan rf=1 -> rf=3 scale-out", func(t *testing.T) {
		ns, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.NoError(t, err)
		require.Len(t, ns.Payload.Nodes, clusterSize)
		for idx, node := range ns.Payload.Nodes {
			nodeAddrs = append(nodeAddrs, nodeAddr{name: node.Name, uri: cluster.URIFor(idx + 1)})
		}

		shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(
			replication.NewGetCollectionShardingStateParams().WithCollection(&paragraphClass.Class), nil,
		)
		require.NoError(t, err)
		require.Len(t, shardingState.Payload.ShardingState.Shards, 3, "expected a thrice-sharded collection")

		for _, state := range shardingState.Payload.ShardingState.Shards {
			require.Len(t, state.Replicas, 1, "expected rf=1: shard %s should have exactly one replica", state.Shard)
			source := state.Replicas[0]
			for _, n := range nodeAddrs {
				if n.name == source {
					continue
				}
				movements = append(movements, scaleOutMovement{shard: state.Shard, source: source, target: n.name})
			}
		}
		require.Len(t, movements, 6, "3 shards x 2 new replicas each = 6 COPY ops")
	})

	// Issue all six COPY ops. The two ops for a given shard share a source,
	// so they exercise concurrent finalize fences on one source shard.
	opIDs := make([]strfmt.UUID, 0, len(movements))
	t.Run("start COPY ops to scale rf=1 -> rf=3", func(t *testing.T) {
		copyType := api.COPY.String()
		for _, m := range movements {
			source, target, shard := m.source, m.target, m.shard
			resp, err := helper.Client(t).Replication.Replicate(
				replication.NewReplicateParams().WithBody(
					&models.ReplicationReplicateReplicaRequest{
						Collection: &paragraphClass.Class,
						SourceNode: &source,
						TargetNode: &target,
						Shard:      &shard,
						Type:       &copyType,
					},
				),
				nil,
			)
			require.NoError(t, err, "failed to start COPY for shard %s %s->%s", shard, source, target)
			require.Equal(t, http.StatusOK, resp.Code(), "replicate did not return 200 OK")
			require.NotNil(t, resp.Payload)
			require.NotNil(t, resp.Payload.ID)
			require.NotEmpty(t, *resp.Payload.ID)
			opIDs = append(opIDs, *resp.Payload.ID)
			time.Sleep(10 * time.Millisecond) // don't overwhelm the leader with bursts
		}
		require.Len(t, opIDs, 6)
	})

	t.Run("wait for all COPY ops to reach READY", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, opID := range opIDs {
				details, err := helper.Client(t).Replication.ReplicationDetails(
					replication.NewReplicationDetailsParams().WithID(opID), nil,
				)
				if !assert.NoError(ct, err, "failed to get replication details for %s", opID) {
					return
				}
				if !assert.NotNil(ct, details.Payload) || !assert.NotNil(ct, details.Payload.Status) {
					return
				}
				assert.Equal(ct, "READY", details.Payload.Status.State, "op %s not yet READY", opID)
			}
		}, 300*time.Second, 2*time.Second, "not all COPY ops reached READY in time")
		// now stop the writes and capture the writer's final tracked state
		writes = stopWrites()
	})

	t.Run("every shard has a replica on every node", func(t *testing.T) {
		shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(
			replication.NewGetCollectionShardingStateParams().WithCollection(&paragraphClass.Class), nil,
		)
		require.NoError(t, err)
		require.Len(t, shardingState.Payload.ShardingState.Shards, 3)
		for _, state := range shardingState.Payload.ShardingState.Shards {
			assert.Len(t, state.Replicas, clusterSize,
				"shard %s should be replicated to all %d nodes after scale-out, got %v", state.Shard, clusterSize, state.Replicas)
		}
	})

	t.Run("all parallel writes present on every shard of every node", func(t *testing.T) {
		// Counts first: a fast, whole-collection check that every node's
		// replica set agrees on cardinality before the per-object sweep.
		for _, n := range nodeAddrs {
			assert.EventuallyWithT(t, func(ct *assert.CollectT) {
				count, err := countObjectsThreadSafe(n.uri, paragraphClass.Class, "")
				if !assert.NoError(ct, err, "count aggregate failed against node %s", n.name) {
					return
				}
				assert.Equal(ct, int64(len(writes.liveIDs)), count,
					"object count on node %s should equal the writer's live set", n.name)
			}, 60*time.Second, 2*time.Second, "object count never converged on node %s", n.name)
		}

		// Every surviving write must be present, with its most recent
		// contents, on every node — node_name pins the read to that node's
		// local replica of the owning shard.
		for id, expectedContents := range writes.liveIDs {
			for _, n := range nodeAddrs {
				var obj *models.Object
				assert.EventuallyWithT(t, func(ct *assert.CollectT) {
					o, err := getObjectThreadSafe(n.uri, paragraphClass.Class, id, n.name, "")
					assert.NoError(ct, err, "error getting live id %s from node %s", id, n.name)
					assert.NotNil(ct, o, "live id %s not yet present on node %s", id, n.name)
					obj = o
				}, 30*time.Second, 2*time.Second, "live id %s missing on node %s", id, n.name)
				if !assert.NotNil(t, obj, "live id %s missing on node %s", id, n.name) {
					continue
				}
				props, ok := obj.Properties.(map[string]any)
				if !assert.True(t, ok, "object %s on node %s has unexpected properties shape", id, n.name) {
					continue
				}
				assert.Equal(t, expectedContents, props["contents"],
					"contents mismatch for id %s on node %s (lost or stale write during scale-out?)", id, n.name)
			}
		}

		// Every deleted id must be absent from every node.
		for id := range writes.deletedIDs {
			for _, n := range nodeAddrs {
				assert.EventuallyWithT(t, func(ct *assert.CollectT) {
					_, err := getObjectThreadSafe(n.uri, paragraphClass.Class, id, n.name, "")
					assert.ErrorIs(ct, err, errObjectNotFound, "deleted id %s unexpectedly present on node %s", id, n.name)
				}, 30*time.Second, 2*time.Second, "deleted id %s still present on node %s", id, n.name)
			}
		}
	})
}
