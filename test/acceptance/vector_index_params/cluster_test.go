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

package vector_index_params

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// shardedObjects keeps about 1000 objects per shard, enough for ef=10 to lose
// recall on each shard's graph.
const shardedObjects = 3000

// TestVectorIndexParamsEF_Sharded pins the cross-node path: 3 shards with RF=1
// on a 3-node cluster, so the node answering a query holds one shard and
// fetches the other two through remoteShardSearch -> SearchShard ->
// postSearchObjects -> IncomingSearch. If a hop dropped the params, the remote
// shards would search with the default beam and the low-ef recall would not
// degrade. The collection has two HNSW named vectors so that the per-target
// entries cross the hop too: "graph" gets the query and the other the decoy
// -q with weight 0 (see isolate). In the per-target subtest the request-wide
// ef is set OPPOSITE to the per-target ef on graph, so a hop that kept only
// the request-wide value would invert the gap. The handler-level hop is
// additionally pinned by the clusterapi unit test.
func TestVectorIndexParamsEF_Sharded(t *testing.T) {
	const class = "EfSharded"
	ctx := context.Background()
	compose, err := docker.New().WithWeaviateClusterWithGRPC().Start(ctx)
	require.NoError(t, err)
	// registered before the collection cleanup so it runs after it (LIFO)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	helper.SetupClient(compose.GetWeaviate().URI())
	c := newGRPC(t, compose.GetWeaviate().GrpcURI())
	ds := newDataset(46, shardedObjects, statQueries)
	indexes := map[string]namedIndex{graphTarget: {"hnsw", rqConfig()}, ivfTarget: {"hnsw", rqConfig()}}
	createNamedVectorCollection(t, class, indexes, func(cls *models.Class) {
		cls.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
		cls.ShardingConfig = map[string]any{"desiredCount": 3}
	}, ds, shardedObjects, types.ConsistencyLevelOne)

	graph := func(params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			return multiTargetRequest(class, k, manual, isolate(graphTarget, q), params, perTarget...)
		}
	}
	// assertGap checks that mean recall@10 is >= 0.95 with the wide beam and
	// at least 0.03 lower (about 1.5 standard errors over 40 queries, on gaps
	// measured at 0.1-0.3) with the narrow one.
	assertGap := func(t *testing.T, what string, high, low float64) {
		t.Logf("%s: mean recall@%d ef=1000 %.3f, ef=10 %.3f", what, k, high, low)
		assert.GreaterOrEqual(t, high, 0.95)
		assert.LessOrEqual(t, low, high-0.03, "%s: ef=10 must have clearly lower mean recall than ef=1000", what)
	}

	t.Run("request-wide ef", func(t *testing.T) {
		high := meanRecallOf(t, c, ds, graph(efP(1000)))
		low := meanRecallOf(t, c, ds, graph(efP(10)))
		assertGap(t, "request-wide", high, low)
	})

	t.Run("per-target ef", func(t *testing.T) {
		high := meanRecallOf(t, c, ds, graph(efP(10), forTarget(graphTarget, efP(1000))))
		low := meanRecallOf(t, c, ds, graph(efP(1000), forTarget(graphTarget, efP(10))))
		assertGap(t, "per-target graph", high, low)
	})
}
