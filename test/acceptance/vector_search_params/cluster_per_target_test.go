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

package vector_search_params

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testShardedPerTarget is the per-target counterpart of the Sharded subtest
// of TestVectorSearchParamsEF_Cluster: 3 shards with RF=1 on 3 nodes, so every
// query reaches at least two shards through the node-to-node search payload.
// The collection has two HNSW named vectors; "graph" gets the query and the
// other the decoy -q with weight 0 (see isolate). Each request sets a
// request-wide ef OPPOSITE to the per-target ef on graph, so if a hop dropped
// the per-target entries the remote shards would search graph with the
// request-wide value and the recall gap would invert.
func testShardedPerTarget(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		const class = "EfShardedPerTarget"
		helper.SetupClient(compose.GetWeaviate().URI())
		ds := newDataset(54, numObjects, statQueries)
		indexes := map[string]namedIndex{graphTarget: {"hnsw", rqConfig()}, ivfTarget: {"hnsw", rqConfig()}}
		createNamedVectorCollection(t, class, indexes, func(c *models.Class) {
			c.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
			c.ShardingConfig = map[string]any{"desiredCount": 3}
		}, ds, numObjects, types.ConsistencyLevelOne)

		req := func(requestEF, graphEF uint32) func(q []float32) *pb.SearchRequest {
			return func(q []float32) *pb.SearchRequest {
				return multiTargetRequest(class, k, manual, isolate(graphTarget, q),
					&pb.VectorSearchParams{Ef: u32(requestEF)},
					forTarget(graphTarget, &pb.VectorSearchParams{Ef: u32(graphEF)}))
			}
		}
		for node := 1; node <= 3; node++ {
			t.Run(fmt.Sprintf("node %d", node), func(t *testing.T) {
				c := newGRPC(t, compose.GetWeaviateNode(node).GrpcURI())
				high := meanRecallOf(t, c, ds, req(10, 1000))
				low := meanRecallOf(t, c, ds, req(1000, 10))
				t.Logf("node %d: mean recall@%d per-target graph ef=1000 %.3f, ef=10 %.3f", node, k, high, low)
				assert.GreaterOrEqual(t, high, 0.95)
				assert.LessOrEqual(t, low, high-0.03, "per-target ef=10 must have clearly lower mean recall than ef=1000")
			})
		}
	}
}
