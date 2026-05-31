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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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
}

func (suite *AsyncCheckpointConvergenceTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestAsyncCheckpointConvergenceTestSuite(t *testing.T) {
	suite.Run(t, new(AsyncCheckpointConvergenceTestSuite))
}

// asyncCheckpointStatusEntry is defined locally (not imported) to keep the
// acceptance test wire-coupled rather than source-coupled.
type asyncCheckpointStatusEntry struct {
	Root        []byte `json:"root"`
	CutoffMs    int64  `json:"cutoff_ms"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// asyncCheckpointCreate takes caller-supplied createdAtMs so the same value can be pinned across nodes.
func asyncCheckpointCreate(t *testing.T, clusterURI, className string, shards []string, cutoffMs, createdAtMs int64) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"shards":        shards,
		"cutoff_ms":     cutoffMs,
		"created_at_ms": createdAtMs,
	})
	require.NoError(t, err)
	resp, err := http.Post(asyncCheckpointURL(clusterURI, className), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

func asyncCheckpointDelete(t *testing.T, clusterURI, className string, shards []string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"shards": shards})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodDelete,
		asyncCheckpointURL(clusterURI, className), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

// asyncCheckpointStatus returns an empty map when the node hosts none of the requested shards.
func asyncCheckpointStatus(t *testing.T, clusterURI, className string, shards []string) map[string]asyncCheckpointStatusEntry {
	t.Helper()
	u, err := url.Parse(asyncCheckpointURL(clusterURI, className))
	require.NoError(t, err)
	if len(shards) > 0 {
		q := u.Query()
		for _, s := range shards {
			q.Add("shards", s)
		}
		u.RawQuery = q.Encode()
	}
	resp, err := http.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status returned %d: %s", resp.StatusCode, body)
	}
	var out map[string]asyncCheckpointStatusEntry
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func asyncCheckpointURL(clusterURI, className string) string {
	// docker's clusterURI is host:port (no scheme); cluster API speaks plain HTTP.
	uri := clusterURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	return fmt.Sprintf("%s/replicas/indices/%s/async-checkpoint", uri, className)
}

// discoverShards uses the public REST API: the cluster API has no shard-enumeration endpoint.
func discoverShards(t *testing.T, restURI, className string) []string {
	t.Helper()
	uri := restURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	resp, err := http.Get(fmt.Sprintf("%s/v1/schema/%s/shards", uri, className))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var shards []struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&shards))
	out := make([]string, len(shards))
	for i, s := range shards {
		out[i] = s.Name
	}
	return out
}

// TestAsyncCheckpoint_ConvergenceAcrossReplicas asserts the load-bearing
// claim for the backup-orchestrator use case: the bounded-tree root is
// bit-identical across replicas, post-cutoff writes don't move it
// (frozen-clone invariant), and delete clears every node.
func (suite *AsyncCheckpointConvergenceTestSuite) TestAsyncCheckpoint_ConvergenceAcrossReplicas() {
	t := suite.T()
	mainCtx := context.Background()

	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

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

	shards := discoverShards(t, nodeRESTs[0], paragraphClass.Class)
	require.NotEmpty(t, shards, "class must have at least one shard")
	t.Logf("class %q hosts %d shard(s): %v", paragraphClass.Class, len(shards), shards)

	// Single createdAt, propagated unchanged: replicas reject one another via the strict-greater-than guard.
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.UnixMilli()

	t.Run("create checkpoint on every node with the same createdAt", func(t *testing.T) {
		for i, cluster := range nodeClusters {
			t.Logf("creating checkpoint on node %d (%s)", i+1, cluster)
			asyncCheckpointCreate(t, cluster, paragraphClass.Class, shards, cutoffMs, createdAt.UnixMilli())
		}
	})

	rootsByShard := map[string]string{}

	t.Run("checkpoint root converges across all nodes per shard", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			perShard := map[string]map[string]string{} // shard → node → root
			for i, cluster := range nodeClusters {
				statuses := asyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
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
			statuses := asyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
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
			asyncCheckpointDelete(t, cluster, paragraphClass.Class, shards)
		}

		// Inactive wire contract: CutoffMs == 0, empty root, zero created_at_ms.
		for i, cluster := range nodeClusters {
			statuses := asyncCheckpointStatus(t, cluster, paragraphClass.Class, shards)
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
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

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
	shards := discoverShards(t, node1REST, paragraphClass.Class)

	createdAt := time.Now().UTC()
	for _, c := range []string{node1Cluster, node2Cluster, node3Cluster} {
		asyncCheckpointCreate(t, c, paragraphClass.Class, shards, createdAt.UnixMilli(), createdAt.UnixMilli())
	}

	// Pre-restart: every node has an active checkpoint.
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, c := range []string{node1Cluster, node2Cluster, node3Cluster} {
			st := asyncCheckpointStatus(t, c, paragraphClass.Class, shards)
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
		st := asyncCheckpointStatus(t, postNode3Cluster, paragraphClass.Class, shards)
		for shard, e := range st {
			assert.Equal(ct, int64(0), e.CutoffMs,
				"shard %q on the restarted node must report inactive (in-memory durability contract)", shard)
		}
	}, 30*time.Second, 1*time.Second)

	for _, c := range []string{node1Cluster, node2Cluster} {
		st := asyncCheckpointStatus(t, c, paragraphClass.Class, shards)
		for shard, e := range st {
			assert.NotZero(t, e.CutoffMs,
				"shard %q on a non-restarted node must still have its checkpoint", shard)
		}
	}
}
