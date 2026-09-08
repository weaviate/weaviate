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
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// AsyncCheckpointConvergenceTestSuite asserts the single-node-backup claim:
// when a checkpoint converges, the bounded-hashtree root is bit-identical
// on every replica, so the backup orchestrator can take its snapshot from
// any single node. Drives the cluster-internal endpoint
// (CLUSTER_DATA_BIND_PORT) — there is no public REST surface today.
type AsyncCheckpointConvergenceTestSuite struct {
	suite.Suite
	compose *docker.DockerCompose
	cancel  context.CancelFunc
}

func (suite *AsyncCheckpointConvergenceTestSuite) SetupSuite() {
	t := suite.T()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	suite.cancel = cancel

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	suite.compose = compose
}

func (suite *AsyncCheckpointConvergenceTestSuite) TearDownSuite() {
	if suite.compose != nil {
		require.NoError(suite.T(), suite.compose.Terminate(context.Background()))
	}
	if suite.cancel != nil {
		suite.cancel()
	}
}

func (suite *AsyncCheckpointConvergenceTestSuite) TearDownTest() {
	helper.DeleteClassEventually(suite.T(), "Paragraph", suite.compose.GetWeaviate().URI())
}

func TestAsyncCheckpointConvergenceTestSuite(t *testing.T) {
	suite.Run(t, new(AsyncCheckpointConvergenceTestSuite))
}

// TestAsyncCheckpoint_ConvergenceAcrossReplicas asserts the load-bearing
// claim for the backup-orchestrator use case: the bounded-tree root is
// bit-identical across replicas, post-cutoff writes don't move it
// (frozen-clone invariant), and delete clears every node.
func (suite *AsyncCheckpointConvergenceTestSuite) TestAsyncCheckpoint_ConvergenceAcrossReplicas() {
	t := suite.T()
	compose := suite.compose

	nodeRESTs := []string{
		compose.GetWeaviate().URI(),
		compose.GetWeaviateNode(2).URI(),
		compose.GetWeaviateNode(3).URI(),
	}
	nodeClusters := []string{
		compose.GetWeaviate().ClusterURI(),
		compose.GetWeaviateNode(2).ClusterURI(),
		compose.GetWeaviateNode(3).ClusterURI(),
	}

	helper.SetupClient(nodeRESTs[0])
	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema with async replication enabled", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
	})

	const seedObjects = 25
	t.Run("seed paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, seedObjects)
		for i := 0; i < seedObjects; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("seed-paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, nodeRESTs[0], batch)
	})

	shards := common.DiscoverShards(t, nodeRESTs[0], paragraphClass.Class)
	require.NotEmpty(t, shards, "class must have at least one shard")
	t.Logf("class %q hosts %d shard(s): %v", paragraphClass.Class, len(shards), shards)

	// Single createdAt, propagated unchanged: replicas reject one another via the strict-greater-than guard.
	createdAt := time.Now().UTC()
	// Cutoff must be in every node's future at create (past-cutoff guard); short so the frozen subtest can outwait it.
	cutoff := createdAt.Add(20 * time.Second)
	cutoffMs := cutoff.UnixMilli()

	t.Run("create checkpoint on every node with the same createdAt", func(t *testing.T) {
		for i, cluster := range nodeClusters {
			t.Logf("creating checkpoint on node %d (%s)", i+1, cluster)
			common.CreateAsyncCheckpoint(t, cluster, paragraphClass.Class, shards, cutoffMs, createdAt.UnixMilli())
		}
	})

	rootsByShard := map[string]string{}

	t.Run("checkpoint root converges across all nodes per shard", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			perShard := map[string]map[string]string{} // shard → node → root
			for i, cluster := range nodeClusters {
				statuses := common.AsyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
				for shard, entry := range statuses {
					if entry.CutoffMs == 0 {
						// Record asymmetry so the all-nodes-agree check fails loudly.
						if perShard[shard] == nil {
							perShard[shard] = map[string]string{}
						}
						perShard[shard][fmt.Sprintf("node%d", i+1)] = "<inactive>"
						continue
					}
					if perShard[shard] == nil {
						perShard[shard] = map[string]string{}
					}
					perShard[shard][fmt.Sprintf("node%d", i+1)] = base64.StdEncoding.EncodeToString(entry.Root)
				}
			}

			for shard, byNode := range perShard {
				if !assert.Len(ct, byNode, 3, "shard %q must report on all 3 nodes", shard) {
					return
				}
				var canonical string
				for node, root := range byNode {
					if root == "<inactive>" {
						ct.Errorf("shard %q on %s reports inactive; expected an active checkpoint", shard, node)
						return
					}
					if canonical == "" {
						canonical = root
						continue
					}
					if !assert.Equal(ct, canonical, root,
						"shard %q root differs across nodes (this breaks the convergence claim)", shard) {
						return
					}
				}
				rootsByShard[shard] = canonical
			}
		}, 60*time.Second, 1*time.Second,
			"checkpoint roots did not converge across replicas; backup adoption depends on this")

		require.Len(t, rootsByShard, len(shards),
			"converged-roots map must have one entry per shard")
		for shard, root := range rootsByShard {
			t.Logf("converged root for shard %q: %s", shard, root)
		}
	})

	t.Run("post-cutoff writes do NOT change the checkpoint root", func(t *testing.T) {
		// Wait until wall-clock passes the cutoff so these writes are genuinely post-cutoff.
		if d := time.Until(cutoff); d > 0 {
			time.Sleep(d + time.Second)
		}
		batch := make([]*models.Object, 10)
		for i := 0; i < 10; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("post-cutoff-paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, nodeRESTs[0], batch)

		// Gate on real propagation so a broken frozen-clone invariant would have moved the BOUNDED root by now.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for ni, restURI := range nodeRESTs {
				for _, obj := range batch {
					got, err := common.GetObjectCL(t, restURI, paragraphClass.Class,
						obj.ID, types.ConsistencyLevelOne)
					require.NoError(ct, err,
						"post-cutoff object %s not yet replicated to node%d", obj.ID, ni+1)
					require.NotNil(ct, got)
				}
			}
		}, 90*time.Second, 2*time.Second,
			"post-cutoff writes never propagated to all replicas")

		for i, cluster := range nodeClusters {
			statuses := common.AsyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
			for shard, entry := range statuses {
				if entry.CutoffMs == 0 {
					continue
				}
				gotRoot := base64.StdEncoding.EncodeToString(entry.Root)
				assert.Equal(t, rootsByShard[shard], gotRoot,
					"checkpoint root for shard %q on node%d moved after a post-cutoff write — frozen-clone invariant broken",
					shard, i+1)
			}
		}
	})

	t.Run("delete clears the checkpoint on every node", func(t *testing.T) {
		for _, cluster := range nodeClusters {
			common.DeleteAsyncCheckpoint(t, cluster, paragraphClass.Class, shards)
		}

		// Inactive wire contract: CutoffMs == 0, empty root, zero created_at_ms.
		for i, cluster := range nodeClusters {
			statuses := common.AsyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
			for shard, entry := range statuses {
				assert.Equal(t, int64(0), entry.CutoffMs,
					"shard %q on node%d should be inactive after delete", shard, i+1)
				assert.Empty(t, entry.Root,
					"inactive shard %q on node%d should encode root as empty", shard, i+1)
				assert.Equal(t, int64(0), entry.CreatedAtMs,
					"inactive shard %q on node%d should encode created_at_ms as 0", shard, i+1)
			}
		}
	})
}

// TestAsyncCheckpoint_RestartDropsLocalCheckpoint covers the in-memory
// durability contract: a restart drops only that node's checkpoint;
// recreate is the operator's responsibility.
func (suite *AsyncCheckpointConvergenceTestSuite) TestAsyncCheckpoint_RestartDropsLocalCheckpoint() {
	t := suite.T()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose := suite.compose

	node1REST := compose.GetWeaviate().URI()
	node1Cluster := compose.GetWeaviate().ClusterURI()
	node2Cluster := compose.GetWeaviateNode(2).ClusterURI()
	node3Cluster := compose.GetWeaviateNode(3).ClusterURI()

	helper.SetupClient(node1REST)
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	paragraphClass.Vectorizer = "text2vec-contextionary"
	helper.CreateClass(t, paragraphClass)

	batch := make([]*models.Object, 5)
	for i := range batch {
		batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("p#%d", i)).Object()
	}
	common.CreateObjects(t, node1REST, batch)
	shards := common.DiscoverShards(t, node1REST, paragraphClass.Class)

	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()
	for _, c := range []string{node1Cluster, node2Cluster, node3Cluster} {
		common.CreateAsyncCheckpoint(t, c, paragraphClass.Class, shards, cutoffMs, createdAt.UnixMilli())
	}

	// Pre-restart: every node has an active checkpoint.
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, c := range []string{node1Cluster, node2Cluster, node3Cluster} {
			st := common.AsyncCheckpointStatus(t, c, paragraphClass.Class, shards)
			for _, e := range st {
				assert.NotZero(ct, e.CutoffMs, "all nodes should have active checkpoints before restart")
			}
		}
	}, 30*time.Second, 500*time.Millisecond)

	t.Run("restart node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
		common.StartNodeAt(ctx, t, compose, 3)
	})

	postNode3Cluster := compose.GetWeaviateNode(3).ClusterURI()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		st := common.AsyncCheckpointStatus(t, postNode3Cluster, paragraphClass.Class, shards)
		for shard, e := range st {
			assert.Equal(ct, int64(0), e.CutoffMs,
				"shard %q on the restarted node must report inactive (in-memory durability contract)", shard)
		}
	}, 30*time.Second, 1*time.Second)

	for _, c := range []string{node1Cluster, node2Cluster} {
		st := common.AsyncCheckpointStatus(t, c, paragraphClass.Class, shards)
		for shard, e := range st {
			assert.NotZero(t, e.CutoffMs,
				"shard %q on a non-restarted node must still have its checkpoint", shard)
		}
	}
}
