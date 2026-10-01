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

package lazy_cold_replica

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	multiShardClass  = "LazyColdMultiShard"
	singleShardClass = "LazyColdSingleShard"
	burstSize        = 40
	burstDeadline    = 2 * time.Second
	vectorDim        = 48
)

// TestLazyColdReplicaFastFailover asserts that reads never wait on a lazily
// unloaded replica after a node restart: the readiness gate rejects it, the
// coordinator fails over to a warm replica, and the cold shard loads in the
// background. The bursts run inside the post-restart window before the 1/sec
// background shard walker warms the restarted node, with the load limiter
// pinned low so waiting on a load (the pre-fix behavior) blows the deadline.
func TestLazyColdReplicaFastFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node lazy-loading test in short mode")
	}
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		WithWeaviateEnv("LAZY_LOAD_SHARD_COUNT_THRESHOLD", "0").
		WithWeaviateEnv("ASYNC_REPLICATION_DISABLED", "true").
		WithWeaviateEnv("MAXIMUM_CONCURRENT_SHARD_LOADS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	defer grpcConn.Close()
	grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

	helper.CreateClass(t, &models.Class{
		Class:      multiShardClass,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "contents", DataType: schema.DataTypeText.PropString()},
		},
		ShardingConfig:    map[string]interface{}{"desiredCount": float64(27)},
		ReplicationConfig: &models.ReplicationConfig{Factor: 2},
	})
	helper.CreateClass(t, &models.Class{
		Class:      singleShardClass,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "contents", DataType: schema.DataTypeText.PropString()},
		},
		ShardingConfig:    map[string]interface{}{"desiredCount": float64(1)},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	})

	rnd := rand.New(rand.NewSource(42))
	randomVector := func() []float32 {
		vec := make([]float32, vectorDim)
		for d := range vec {
			vec[d] = rnd.Float32()
		}
		return vec
	}

	batch := make([]*models.Object, 0, 2000)
	for i := 0; i < 24000; i++ {
		batch = append(batch, &models.Object{
			Class:      multiShardClass,
			Properties: map[string]interface{}{"contents": fmt.Sprintf("document %d about weaviate lazy loading", i)},
			Vector:     randomVector(),
		})
		if len(batch) == cap(batch) {
			helper.CreateObjectsBatch(t, batch)
			batch = batch[:0]
		}
	}
	for i := 0; i < 5000; i++ {
		batch = append(batch, &models.Object{
			Class:      singleShardClass,
			Properties: map[string]interface{}{"contents": fmt.Sprintf("single shard document %d", i)},
			Vector:     randomVector(),
		})
		if len(batch) == cap(batch) {
			helper.CreateObjectsBatch(t, batch)
			batch = batch[:0]
		}
	}
	helper.CreateObjectsBatch(t, batch)

	remoteShard, victimNode := findFullyRemoteShard(t, compose.GetWeaviate().URI())
	victimN, err := strconv.Atoi(strings.TrimPrefix(victimNode, "weaviate-"))
	require.NoError(t, err)
	t.Logf("coordinator=weaviate-0 remoteShard=%s victim=%s", remoteShard, victimNode)

	queryVec := randomVector()
	vectorReq := &pb.SearchRequest{
		Collection: multiShardClass,
		Limit:      10,
		NearVector: &pb.NearVector{Vectors: []*pb.Vectors{{
			VectorBytes: byteops.Fp32SliceToBytes(queryVec),
			Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
		}}},
		Uses_127Api: true,
	}
	bm25Req := &pb.SearchRequest{
		Collection:  multiShardClass,
		Limit:       10,
		Bm25Search:  &pb.BM25{Query: "weaviate", Properties: []string{"contents"}},
		Uses_127Api: true,
	}

	burst := func(req *pb.SearchRequest, deadline time.Duration) (latencies []time.Duration, errs []error) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := 0; i < burstSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				qctx, cancel := context.WithTimeout(ctx, deadline)
				defer cancel()
				start := time.Now()
				_, err := grpcClient.Search(qctx, req)
				elapsed := time.Since(start)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				latencies = append(latencies, elapsed)
			}()
		}
		wg.Wait()
		return latencies, errs
	}

	t.Run("healthy cluster baseline", func(t *testing.T) {
		latencies, errs := burst(vectorReq, 10*time.Second)
		require.Empty(t, errs)
		require.Len(t, latencies, burstSize)
	})

	require.NoError(t, compose.StopNode(ctx, victimN, nil))
	require.NoError(t, compose.StartNode(ctx, victimN))
	waitAllHealthy(t, compose.GetWeaviate().URI())
	t.Logf("victim loaded shards at burst start: %d", loadedShardCountOnNode(t, compose.GetWeaviate().URI(), victimNode))

	t.Run("vector burst fails over instead of waiting on cold replicas", func(t *testing.T) {
		latencies, errs := burst(vectorReq, burstDeadline)
		logLatencies(t, "vector-cold-replica", latencies, errs)
		require.Emptyf(t, errs, "queries waited on cold replicas: %v", firstError(errs))
		assert.Less(t, percentile(latencies, 0.99), time.Second)
	})

	t.Run("bm25 burst fails over instead of waiting on cold replicas", func(t *testing.T) {
		latencies, errs := burst(bm25Req, burstDeadline)
		logLatencies(t, "bm25-cold-replica", latencies, errs)
		require.Emptyf(t, errs, "queries waited on cold replicas: %v", firstError(errs))
		assert.Less(t, percentile(latencies, 0.99), time.Second)
	})

	t.Run("single-shard vector search on the cold node falls through to a replica", func(t *testing.T) {
		victimConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviateNode(victimN + 1).GrpcURI())
		require.NoError(t, err)
		defer victimConn.Close()
		victimClient := helper.CreateGrpcWeaviateClient(victimConn)

		qctx, cancel := context.WithTimeout(ctx, burstDeadline)
		defer cancel()
		start := time.Now()
		resp, err := victimClient.Search(qctx, &pb.SearchRequest{
			Collection: singleShardClass,
			Limit:      10,
			NearVector: &pb.NearVector{Vectors: []*pb.Vectors{{
				VectorBytes: byteops.Fp32SliceToBytes(queryVec),
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
			}}},
			Uses_127Api: true,
		})
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		t.Logf("single-shard cold-local search took %v", elapsed)
	})

	t.Run("the cold replica warms in the background", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.True(collect, shardLoadedOnNode(t, compose.GetWeaviate().URI(), victimNode, multiShardClass, remoteShard))
		}, 120*time.Second, 2*time.Second)
	})
}

func firstError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func findFullyRemoteShard(t *testing.T, coordinatorURI string) (shard, victim string) {
	t.Helper()
	shardNodes := map[string][]string{}
	for _, node := range common.GetNodes(t, coordinatorURI).Nodes {
		for _, s := range node.Shards {
			if s.Class == multiShardClass {
				shardNodes[s.Name] = append(shardNodes[s.Name], node.Name)
			}
		}
	}
	require.Len(t, shardNodes, 27)
	for name, nodes := range shardNodes {
		require.Lenf(t, nodes, 2, "shard %s must have RF=2", name)
		if nodes[0] != "weaviate-0" && nodes[1] != "weaviate-0" {
			sort.Strings(nodes)
			return name, nodes[1]
		}
	}
	t.Fatal("no shard is fully remote to the coordinator weaviate-0")
	return "", ""
}

func loadedShardCountOnNode(t *testing.T, coordinatorURI, nodeName string) int {
	t.Helper()
	count := 0
	for _, node := range common.GetNodes(t, coordinatorURI).Nodes {
		if node.Name != nodeName {
			continue
		}
		for _, s := range node.Shards {
			if s.Loaded {
				count++
			}
		}
	}
	return count
}

func shardLoadedOnNode(t *testing.T, coordinatorURI, nodeName, className, shardName string) bool {
	t.Helper()
	for _, node := range common.GetNodes(t, coordinatorURI).Nodes {
		if node.Name != nodeName {
			continue
		}
		for _, s := range node.Shards {
			if s.Class == className && s.Name == shardName {
				return s.Loaded
			}
		}
	}
	return false
}

func waitAllHealthy(t *testing.T, coordinatorURI string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		healthy := 0
		for _, node := range common.GetNodes(t, coordinatorURI).Nodes {
			if node.Status != nil && *node.Status == models.NodeStatusStatusHEALTHY {
				healthy++
			}
		}
		assert.Equal(collect, 3, healthy)
	}, 90*time.Second, time.Second)
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted))*p) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func logLatencies(t *testing.T, label string, latencies []time.Duration, errs []error) {
	t.Helper()
	var max time.Duration
	for _, l := range latencies {
		if l > max {
			max = l
		}
	}
	t.Logf("%s: ok=%d err=%d p50=%v p90=%v p99=%v max=%v",
		label, len(latencies), len(errs),
		percentile(latencies, 0.50), percentile(latencies, 0.90),
		percentile(latencies, 0.99), max)
}
