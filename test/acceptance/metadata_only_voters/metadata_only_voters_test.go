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

package metadataonlyvoters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	voterCount = 3
	dataCount  = 2
	totalNodes = voterCount + dataCount

	className     = "VoterSepDoc"
	desiredShards = 4
	replicaFactor = 2
)

// voterNames / dataNames are derived from the builder's hostname scheme:
// the first voterCount nodes (weaviate-0..2) are the metadata-only voters,
// the rest (weaviate-3..4) are the non-voter data nodes.
func voterNames() []string { return []string{"weaviate-0", "weaviate-1", "weaviate-2"} }
func dataNames() []string  { return []string{"weaviate-3", "weaviate-4"} }

// TestMetadataOnlyVoters proves the voter/data-node separation on a 5-node
// cluster (3 metadata-only voters + 2 data nodes) with a shards=4, rf=2
// collection:
//
//  1. all 8 shard replicas live on the 2 data nodes, none on the voters;
//  2. reads and writes can enter through any node (incl. voters) and are
//     forwarded correctly;
//  3. each voter can be restarted one at a time (quorum preserved) and a data
//     node is never the leader;
//  4. creating an rf=3 collection fails (only two data nodes exist).
func TestMetadataOnlyVoters(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateClusterWithVoters(voterCount, dataCount).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	// nodeURI fetches the current host URI for a node by index. It must be called
	// fresh after a restart, because testcontainers remaps the host port when a
	// container is stopped and started.
	nodeURI := func(i int) string {
		c, err := compose.ContainerAt(i)
		require.NoError(t, err)
		require.NotNil(t, c, "missing container at index %d", i)
		return c.URI()
	}

	uris := make([]string, totalNodes)
	for i := range uris {
		uris[i] = nodeURI(i)
	}
	voterURIs := uris[:voterCount]

	// Sanity: every node reports all 5 members healthy.
	for _, uri := range uris {
		uri := uri
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			names, statuses := clusterNodes(ct, uri)
			require.Len(ct, names, totalNodes, "node %s should see all %d members", uri, totalNodes)
			for n, s := range statuses {
				require.Equal(ct, "HEALTHY", s, "member %s should be HEALTHY via %s", n, uri)
			}
		}, 60*time.Second, 2*time.Second)
	}

	// Create the shards=4, rf=2 collection. vectorizer "none" so no module is
	// needed; objects carry explicit vectors.
	helper.SetupClient(voterURIs[0])
	helper.DeleteClass(t, className)
	helper.CreateClass(t, &models.Class{
		Class:             className,
		Vectorizer:        "none",
		ShardingConfig:    map[string]interface{}{"desiredCount": desiredShards},
		ReplicationConfig: &models.ReplicationConfig{Factor: replicaFactor},
		Properties:        []*models.Property{{Name: "title", DataType: []string{"text"}}},
	})

	t.Run("all shard replicas live on data nodes, none on voters", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			byNode := shardsByNode(ct, voterURIs[0], className)

			for _, v := range voterNames() {
				require.Empty(ct, byNode[v], "voter %s must host no shards", v)
			}

			total := 0
			distinct := map[string]struct{}{}
			for _, d := range dataNames() {
				require.NotEmpty(ct, byNode[d], "data node %s should host shards", d)
				for _, s := range byNode[d] {
					total++
					distinct[s] = struct{}{}
				}
			}
			require.Equal(ct, desiredShards*replicaFactor, total,
				"all %d shard replicas must live on the data nodes", desiredShards*replicaFactor)
			require.Len(ct, distinct, desiredShards, "expected %d distinct shards", desiredShards)
		}, 90*time.Second, 2*time.Second)
	})

	t.Run("reads and writes are forwarded from any node", func(t *testing.T) {
		const perNode = 4
		// Write a batch THROUGH every node — including the voters, which own no
		// shards and must proxy the writes to the data nodes.
		for i, uri := range uris {
			batch := make([]*models.Object, perNode)
			for j := range batch {
				batch[j] = &models.Object{
					Class:      className,
					Properties: map[string]interface{}{"title": fmt.Sprintf("node%d-obj%d", i, j)},
					Vector:     []float32{float32(i), float32(j), 1, 2},
				}
			}
			common.CreateObjects(t, uri, batch)
		}
		want := perNode * totalNodes

		// Read THROUGH every node: each must return the full count, proving the
		// coordinator on every node (voters included) fans out to the data nodes.
		for _, uri := range uris {
			uri := uri
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				got, err := aggregateCount(uri, className)
				require.NoError(ct, err)
				require.Equal(ct, want, got, "count read via %s", uri)
			}, 30*time.Second, time.Second)
		}
	})

	t.Run("rf=3 collection is rejected with only two data nodes", func(t *testing.T) {
		body := `{"class":"VoterSepRF3","vectorizer":"none",` +
			`"shardingConfig":{"desiredCount":1},"replicationConfig":{"factor":3},` +
			`"properties":[{"name":"title","dataType":["text"]}]}`
		resp, err := http.Post("http://"+voterURIs[0]+"/v1/schema", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)

		require.GreaterOrEqual(t, resp.StatusCode, 400,
			"rf=3 must be rejected: only %d data nodes exist (body: %s)", dataCount, string(raw))
		require.Contains(t, strings.ToLower(string(raw)),
			"could not find enough weaviate nodes for replication",
			"rejection should cite insufficient data nodes")
	})

	t.Run("voters restart one at a time and a data node is never leader", func(t *testing.T) {
		timeout := 30 * time.Second

		requireVoterLeader(t, nodeURI(0))

		for v := 0; v < voterCount; v++ {
			// Query a different, still-running voter (quorum: 2 of 3 remain up).
			// Fetch its URI fresh — earlier iterations may have restarted it.
			survivor := nodeURI((v + 1) % voterCount)
			stoppedName := fmt.Sprintf("weaviate-%d", v)

			require.NoError(t, compose.StopAt(ctx, v, &timeout), "stop voter %s", stoppedName)

			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				leader := leaderName(ct, survivor)
				require.NotEmpty(ct, leader, "cluster should re-elect a leader after %s stops", stoppedName)
				require.NotEqual(ct, stoppedName, leader, "stopped voter must not be leader")
				require.Contains(ct, voterNames(), leader, "leader must be a voter")
				require.NotContains(ct, dataNames(), leader, "a data node must NEVER be the leader")
			}, 60*time.Second, time.Second)

			require.NoError(t, compose.StartAt(ctx, v), "restart voter %s", stoppedName)

			// After the voter rejoins, the cluster is stable and the leader is
			// still a voter. Re-fetch the survivor URI in case it was restarted.
			survivor = nodeURI((v + 1) % voterCount)
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				leader := leaderName(ct, survivor)
				require.Contains(ct, voterNames(), leader, "leader must be a voter after %s rejoins", stoppedName)
				require.NotContains(ct, dataNames(), leader, "a data node must NEVER be the leader")
			}, 60*time.Second, time.Second)
		}

		// Data is still fully readable from a data node after the voter churn.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			got, err := aggregateCount(nodeURI(voterCount), className) // first data node
			require.NoError(ct, err)
			require.Equal(ct, 4*totalNodes, got, "data must survive voter restarts")
		}, 30*time.Second, time.Second)
	})
}

// requireVoterLeader asserts (eventually) that the cluster has a leader and it
// is one of the voters.
func requireVoterLeader(t *testing.T, uri string) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		leader := leaderName(ct, uri)
		require.NotEmpty(ct, leader, "cluster must have a leader")
		require.Contains(ct, voterNames(), leader, "leader must be a voter")
	}, 30*time.Second, time.Second)
}

// leaderName returns the current RAFT leader id as reported by the node at uri.
func leaderName(ct require.TestingT, uri string) string {
	helper.SetupClient(uri)
	resp, err := helper.Client(nil).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), nil)
	require.NoError(ct, err)
	require.NotNil(ct, resp.Payload)
	require.NotEmpty(ct, resp.Payload.Statistics)
	if lid := resp.Payload.Statistics[0].LeaderID; lid != nil {
		return fmt.Sprintf("%v", lid)
	}
	return ""
}

// shardsByNode returns, per node name, the physical shard names it hosts for the
// given class, read from /v1/nodes?output=verbose on the node at uri.
func shardsByNode(ct require.TestingT, uri, class string) map[string][]string {
	resp, err := http.Get("http://" + uri + "/v1/nodes?output=verbose")
	require.NoError(ct, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Name   string `json:"name"`
			Shards []struct {
				Name  string `json:"name"`
				Class string `json:"class"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(ct, json.NewDecoder(resp.Body).Decode(&out))
	res := make(map[string][]string)
	for _, n := range out.Nodes {
		for _, s := range n.Shards {
			if s.Class == class {
				res[n.Name] = append(res[n.Name], s.Name)
			}
		}
	}
	return res
}

// aggregateCount returns the object count for a class via GraphQL Aggregate,
// executed against the node at uri (proxied if uri is a voter).
func aggregateCount(uri, class string) (int, error) {
	query := fmt.Sprintf(`{"query":"{ Aggregate { %s { meta { count } } } }"}`, class)
	resp, err := http.Post("http://"+uri+"/v1/graphql", "application/json", strings.NewReader(query))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Data struct {
			Aggregate map[string][]struct {
				Meta struct {
					Count int `json:"count"`
				} `json:"meta"`
			} `json:"Aggregate"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if len(out.Errors) > 0 {
		return 0, fmt.Errorf("graphql: %s", out.Errors[0].Message)
	}
	rows := out.Data.Aggregate[class]
	if len(rows) == 0 {
		return 0, fmt.Errorf("no aggregate result for %s", class)
	}
	return rows[0].Meta.Count, nil
}

// clusterNodes returns the member names and their statuses from /v1/nodes on uri.
func clusterNodes(ct require.TestingT, uri string) ([]string, map[string]string) {
	resp, err := http.Get("http://" + uri + "/v1/nodes")
	require.NoError(ct, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"nodes"`
	}
	require.NoError(ct, json.NewDecoder(resp.Body).Decode(&out))
	names := make([]string, 0, len(out.Nodes))
	statuses := make(map[string]string, len(out.Nodes))
	for _, n := range out.Nodes {
		names = append(names, n.Name)
		statuses[n.Name] = n.Status
	}
	slices.Sort(names)
	return names, statuses
}
