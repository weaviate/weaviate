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

package hung_replica

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	className = "HungReplicaRepro"
	burstSize = 40
)

// TestHungReplica reproduces the coordinator-side symptoms of one replica that
// hangs (accepts TCP, never responds) instead of failing fast: sequential
// replica fallback burns the whole client deadline (DEADLINE_EXCEEDED) or the
// full 20s inner RPC timeout (tail latency). HUNG_REPLICA_HEDGED=true runs the
// same scenarios on a cluster with QUERY_HEDGED_TIMEOUT=100ms and flips the
// assertions to the hedged expectations.
func TestHungReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node fault-injection test in short mode")
	}
	hedged := os.Getenv("HUNG_REPLICA_HEDGED") == "true"
	ctx := context.Background()

	builder := docker.New().WithWeaviateClusterWithGRPC()
	if hedged {
		builder = builder.WithWeaviateEnv("QUERY_HEDGED_TIMEOUT", "100ms")
	}
	compose, err := builder.Start(ctx)
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

	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "contents", DataType: schema.DataTypeText.PropString()},
		},
		ShardingConfig:    map[string]interface{}{"desiredCount": float64(3)},
		ReplicationConfig: &models.ReplicationConfig{Factor: 2},
	}
	helper.CreateClass(t, class)

	objects := make([]*models.Object, 500)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"contents": fmt.Sprintf("document %d about weaviate replication behavior", i),
			},
		}
	}
	helper.CreateObjectsBatch(t, objects)

	victimNode := findFullyRemoteShardReplica(t, compose.GetWeaviate().URI())
	victimN, err := strconv.Atoi(strings.TrimPrefix(victimNode, "weaviate-"))
	require.NoError(t, err)
	t.Logf("coordinator=weaviate-0 victim=%s hedged=%v", victimNode, hedged)

	searchReq := &pb.SearchRequest{
		Collection:  className,
		Limit:       10,
		Bm25Search:  &pb.BM25{Query: "weaviate", Properties: []string{"contents"}},
		Uses_127Api: true,
	}

	burst := func(deadline time.Duration) (latencies []time.Duration, errs []error) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := 0; i < burstSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				qctx, cancel := context.WithTimeout(ctx, deadline)
				defer cancel()
				start := time.Now()
				_, err := grpcClient.Search(qctx, searchReq)
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
		latencies, errs := burst(10 * time.Second)
		require.Empty(t, errs)
		require.Len(t, latencies, burstSize)
		assert.Less(t, percentile(latencies, 0.99), 2*time.Second)
	})

	t.Run("hang with short deadline", func(t *testing.T) {
		require.NoError(t, compose.PauseNode(ctx, victimN))
		time.Sleep(500 * time.Millisecond)
		latencies, errs := burst(5 * time.Second)
		require.NoError(t, compose.UnpauseNode(ctx, victimN))
		waitAllHealthy(t, compose.GetWeaviate().URI())

		logLatencies(t, "hang-5s-deadline", latencies, errs)
		if hedged {
			require.Empty(t, errs)
			assert.Less(t, percentile(latencies, 0.99), 2*time.Second)
			return
		}
		for _, err := range errs {
			require.Equalf(t, codes.DeadlineExceeded, status.Code(err), "unexpected failure kind: %v", err)
		}
		assert.GreaterOrEqual(t, len(errs), 8, "expected ~half the burst to start on the hung replica")
		assert.LessOrEqual(t, len(errs), 32, "expected ~half the burst to survive")
		for _, l := range latencies {
			assert.Less(t, l, 2*time.Second)
		}
	})

	t.Run("hang with generous deadline", func(t *testing.T) {
		require.NoError(t, compose.PauseNode(ctx, victimN))
		time.Sleep(500 * time.Millisecond)
		latencies, errs := burst(35 * time.Second)
		require.NoError(t, compose.UnpauseNode(ctx, victimN))
		waitAllHealthy(t, compose.GetWeaviate().URI())

		logLatencies(t, "hang-35s-deadline", latencies, errs)
		require.Empty(t, errs)
		require.Len(t, latencies, burstSize)
		if hedged {
			assert.Less(t, maxLatency(latencies), 3*time.Second)
			return
		}
		fast, slow := 0, 0
		for _, l := range latencies {
			if l <= 2*time.Second {
				fast++
			}
			if l >= 15*time.Second {
				slow++
			}
		}
		assert.GreaterOrEqual(t, fast, burstSize/4, "expected a fast mode from healthy-first queries")
		assert.GreaterOrEqual(t, slow, burstSize/4, "expected a ~20s mode from hung-first queries")
		assert.LessOrEqual(t, maxLatency(latencies), 30*time.Second)
	})

	t.Run("stopped node is benign", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, victimN, nil))
		latencies, errs := burst(5 * time.Second)
		require.NoError(t, compose.StartNode(ctx, victimN))
		waitAllHealthy(t, compose.GetWeaviate().URI())

		logLatencies(t, "stopped-node", latencies, errs)
		require.Empty(t, errs, "connection-refused failover must not eat the deadline")
		assert.Less(t, percentile(latencies, 0.99), 3*time.Second)
	})
}

func findFullyRemoteShardReplica(t *testing.T, coordinatorURI string) string {
	t.Helper()
	shardNodes := map[string][]string{}
	for _, node := range common.GetNodes(t, coordinatorURI).Nodes {
		for _, shard := range node.Shards {
			if shard.Class == className {
				shardNodes[shard.Name] = append(shardNodes[shard.Name], node.Name)
			}
		}
	}
	require.Len(t, shardNodes, 3)
	for shard, nodes := range shardNodes {
		require.Lenf(t, nodes, 2, "shard %s must have RF=2", shard)
		if nodes[0] != "weaviate-0" && nodes[1] != "weaviate-0" {
			sort.Strings(nodes)
			return nodes[1]
		}
	}
	t.Fatal("no shard is fully remote to the coordinator weaviate-0")
	return ""
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

func maxLatency(latencies []time.Duration) time.Duration {
	var max time.Duration
	for _, l := range latencies {
		if l > max {
			max = l
		}
	}
	return max
}

func logLatencies(t *testing.T, label string, latencies []time.Duration, errs []error) {
	t.Helper()
	t.Logf("%s: ok=%d err=%d p50=%v p90=%v p99=%v max=%v",
		label, len(latencies), len(errs),
		percentile(latencies, 0.50), percentile(latencies, 0.90),
		percentile(latencies, 0.99), maxLatency(latencies))
}
