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

package selfrecovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchclient "github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// hasSelfRecoveryOp reports whether the replication-op list for targetNode
// contains a SELF_RECOVERY op.
func hasSelfRecoveryOp(t *testing.T, targetNode string) (bool, error) {
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	if err != nil {
		return false, err
	}
	for _, op := range body.Payload {
		if op.Type != nil && *op.Type == "SELF_RECOVERY" {
			return true, nil
		}
	}
	return false, nil
}

// TestSelfRecoveryViaLogReplay verifies self-recovery fires for a wiped node
// that rejoins purely via RAFT log replay — no snapshot is ever produced
// (default RAFT_SNAPSHOT_THRESHOLD/RAFT_TRAILING_LOGS, no /debug/raft/snapshot
// call). It also pins the negatives: creating a fresh collection on a healthy
// cluster, and a plain restart with data intact, must never trigger recovery.
func TestSelfRecoveryViaLogReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", "2").
		// Required to expose the /replication/* observability API.
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		// NOTE: deliberately no RAFT_TRAILING_LOGS override and no forced
		// snapshot. With defaults the leader keeps the whole (tiny,
		// schema-only) RAFT log, so a wiped node rejoins by replaying
		// committed log entries — never InstallSnapshot. This is exactly
		// the path the older e2e test could not cover.
		WithWeaviateTmpfsData(). // tmpfs /data so a stop+start truly wipes the dir
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		objCount = 500
		wipedIdx = 2 // container index; node name docker.Weaviate2
	)
	wipedNodeName := docker.Weaviate2
	paragraphClass := articles.ParagraphsClass()

	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
			}
		}, 3*time.Minute, 1*time.Second)
	})

	t.Run("create RF=3 single-shard collection and ingest", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		// Async replication OFF: SELF_RECOVERY is then the only path that
		// can restore the wiped node's data.
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
		paragraphClass.Vectorizer = "none"
		helper.CreateClass(t, paragraphClass)

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Len(ct, n.Shards, 1, "node %s shard count", n.Name)
			}
		}, 30*time.Second, 500*time.Millisecond)

		batch := make([]*models.Object, objCount)
		for i := 0; i < objCount; i++ {
			batch[i] = articles.NewParagraph().
				WithID(strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			params := batchclient.NewBatchObjectsCreateParams().WithBody(
				batchclient.BatchObjectsCreateBody{Objects: batch})
			resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
			require.NoError(ct, err)
			require.NotNil(ct, resp)
			for _, o := range resp.Payload {
				if o.Result != nil && o.Result.Errors != nil && len(o.Result.Errors.Error) > 0 {
					require.Failf(ct, "batch ingest had per-object errors", "%v", o.Result.Errors.Error[0].Message)
				}
			}
		}, 30*time.Second, 1*time.Second, "batch ingest never succeeded")
	})

	// NEGATIVE: creating a fresh collection on a healthy running cluster is a
	// runtime create — every replica builds an empty shard folder, so the
	// load-time "folder missing" signal never appears. No node may register
	// a SELF_RECOVERY op for it.
	t.Run("creating a collection on a healthy cluster does not recover", func(t *testing.T) {
		fresh := articles.ArticlesClass()
		fresh.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		fresh.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
		fresh.Vectorizer = "none"
		helper.CreateClass(t, fresh)

		for _, node := range []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2} {
			node := node
			assert.Never(t, func() bool {
				found, _ := hasSelfRecoveryOp(t, node)
				return found
			}, 10*time.Second, 1*time.Second,
				"unexpected SELF_RECOVERY op for %s after a runtime collection create", node)
		}
	})

	t.Run("wipe node-3 data and restart (rejoins via log replay)", func(t *testing.T) {
		common.WipeNodeDataAt(ctx, t, compose, wipedIdx)
		common.StartNodeAt(ctx, t, compose, wipedIdx)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	// POSITIVE: the wiped node rejoined via log replay (no snapshot). The
	// startup DB-load pass must see the missing shard folder and recover it.
	t.Run("a SELF_RECOVERY op fires for the wiped node", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			found, err := hasSelfRecoveryOp(t, wipedNodeName)
			require.NoError(ct, err)
			assert.True(ct, found, "expected a SELF_RECOVERY op for %s", wipedNodeName)
		}, 3*time.Minute, 1*time.Second, "no SELF_RECOVERY op observed for the wiped node")
	})

	t.Run("recovery completes and the wiped node reports full object count", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			for _, n := range body.Payload.Nodes {
				if n.Name != wipedNodeName {
					continue
				}
				require.Len(ct, n.Shards, 1)
				assert.True(ct, n.Shards[0].Loaded, "wiped node shard loaded after recovery")
				assert.Equal(ct, int64(objCount), n.Shards[0].ObjectCount,
					"wiped node object count after recovery")
			}
		}, 5*time.Minute, 2*time.Second, "wiped node did not report full object count after recovery")
	})

	// NOTE: a "plain restart with data intact does not recover" negative
	// cannot be expressed here: this cluster mounts /data as tmpfs (see
	// WithWeaviateTmpfsData), so every container stop drops the data — there
	// is no intact restart. That a node with prior RAFT state is never
	// treated as a wiped joiner is covered by the unit test
	// TestNoteWipedJoinerProgress ("node with prior state is excluded").
}

// TestSelfRecoveryViaLogReplayMultiTenant verifies the same log-replay
// recovery for multi-tenant tenant shards: the startup DB-load pass rebuilds
// the index from the caught-up schema and iterates every HOT tenant shard, so
// a wiped node recovers all of its tenant shards.
func TestSelfRecoveryViaLogReplayMultiTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", "2").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateTmpfsData().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		wipedIdx      = 2
		objsPerTenant = 50
	)
	wipedNodeName := docker.Weaviate2
	tenants := []string{"tenantA", "tenantB", "tenantC"}
	mtClass := articles.ParagraphsClass()

	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
			}
		}, 3*time.Minute, 1*time.Second)
	})

	t.Run("create RF=3 multi-tenant collection with HOT tenants", func(t *testing.T) {
		mtClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		mtClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
		mtClass.Vectorizer = "none"
		helper.CreateClass(t, mtClass)

		ts := make([]*models.Tenant, len(tenants))
		for i, name := range tenants {
			ts[i] = &models.Tenant{Name: name, ActivityStatus: "HOT"}
		}
		helper.CreateTenants(t, mtClass.Class, ts)
	})

	t.Run("ingest objects per tenant", func(t *testing.T) {
		for _, tenant := range tenants {
			batch := make([]*models.Object, objsPerTenant)
			for i := 0; i < objsPerTenant; i++ {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("%s#%d", tenant, i)).
					WithTenant(tenant).
					Object()
			}
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				params := batchclient.NewBatchObjectsCreateParams().WithBody(
					batchclient.BatchObjectsCreateBody{Objects: batch})
				resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
				require.NoError(ct, err)
				require.NotNil(ct, resp)
				for _, o := range resp.Payload {
					if o.Result != nil && o.Result.Errors != nil && len(o.Result.Errors.Error) > 0 {
						require.Failf(ct, "batch ingest errors", "%v", o.Result.Errors.Error[0].Message)
					}
				}
			}, 30*time.Second, 1*time.Second, "tenant %s ingest never succeeded", tenant)
		}
	})

	// NEGATIVE: creating and activating tenants on a healthy cluster is a
	// runtime create — each replica builds the tenant-shard folder, so the
	// load-time "folder missing" signal never appears. No node may register
	// a SELF_RECOVERY op for it.
	t.Run("creating tenants on a healthy cluster does not recover", func(t *testing.T) {
		for _, node := range []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2} {
			node := node
			assert.Never(t, func() bool {
				found, _ := hasSelfRecoveryOp(t, node)
				return found
			}, 10*time.Second, 1*time.Second,
				"unexpected SELF_RECOVERY op for %s after runtime tenant creation", node)
		}
	})

	t.Run("wipe node-3 data and restart (rejoins via log replay)", func(t *testing.T) {
		common.WipeNodeDataAt(ctx, t, compose, wipedIdx)
		common.StartNodeAt(ctx, t, compose, wipedIdx)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	t.Run("SELF_RECOVERY ops fire for the wiped node's tenant shards", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			found, err := hasSelfRecoveryOp(t, wipedNodeName)
			require.NoError(ct, err)
			assert.True(ct, found, "expected SELF_RECOVERY ops for %s tenant shards", wipedNodeName)
		}, 3*time.Minute, 1*time.Second, "no SELF_RECOVERY op observed for the wiped node")
	})

	t.Run("recovery completes and the wiped node reports all tenant shards", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(mtClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			for _, n := range body.Payload.Nodes {
				if n.Name != wipedNodeName {
					continue
				}
				require.Len(ct, n.Shards, len(tenants), "wiped node tenant-shard count")
				for _, s := range n.Shards {
					assert.True(ct, s.Loaded, "tenant shard %s loaded after recovery", s.Name)
					assert.Equal(ct, int64(objsPerTenant), s.ObjectCount,
						"tenant shard %s object count after recovery", s.Name)
				}
			}
		}, 5*time.Minute, 2*time.Second, "wiped node did not recover all tenant shards")
	})
}
