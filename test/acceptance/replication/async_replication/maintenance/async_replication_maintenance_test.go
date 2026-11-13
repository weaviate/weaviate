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

package maintenance

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	maintenanceNode = "weaviate-2"
	objectCount     = 300
)

// TestAsyncReplicationIntoMaintenanceNode: a node in maintenance mode is refilled by async replication only when ASYNC_REPLICATION_IN_MAINTENANCE_ENABLED is set.
func TestAsyncReplicationIntoMaintenanceNode(t *testing.T) {
	tests := []struct {
		name        string
		grpc        bool
		flagEnabled bool
	}{
		{name: "REST transport, flag on", grpc: false, flagEnabled: true},
		{name: "gRPC transport, flag on", grpc: true, flagEnabled: true},
		{name: "REST transport, flag off", grpc: false, flagEnabled: false},
		{name: "gRPC transport, flag off", grpc: true, flagEnabled: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runMaintenanceScenario(t, tt.grpc, tt.flagEnabled)
		})
	}
}

func runMaintenanceScenario(t *testing.T, grpcTransport, flagEnabled bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("MAINTENANCE_NODES", maintenanceNode).
		WithWeaviateEnv("ASYNC_REPLICATION_IN_MAINTENANCE_ENABLED", fmt.Sprintf("%t", flagEnabled)).
		WithWeaviateEnv("REPLICATION_GRPC_ENABLED", fmt.Sprintf("%t", grpcTransport)).
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	class := articles.ParagraphsClass()
	frequency, whilePropagating, delay := int64(1000), int64(500), int64(500)
	class.Vectorizer = "none"
	class.ReplicationConfig = &models.ReplicationConfig{
		Factor: 3,
		AsyncConfig: &models.ReplicationAsyncConfig{
			Frequency:                 &frequency,
			FrequencyWhilePropagating: &whilePropagating,
			PropagationDelay:          &delay,
		},
	}
	helper.CreateClass(t, class)

	batch := make([]*models.Object, objectCount)
	for i := range batch {
		batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("paragraph#%d", i)).Object()
	}
	common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelQuorum)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		counts, err := nodeObjectCounts(t, class.Class)
		require.NoError(ct, err)
		require.Equal(ct, int64(objectCount), counts["weaviate-0"])
		require.Equal(ct, int64(objectCount), counts["weaviate-1"])
	}, 2*time.Minute, time.Second, "healthy replicas must hold every object")

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		n, err := common.ShardsAsyncReplicationLen(t, class.Class)
		require.NoError(ct, err)
		require.Greater(ct, n, 0)
	}, 2*time.Minute, time.Second, "async replication must be active on the healthy nodes")

	if flagEnabled {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			counts, err := nodeObjectCounts(t, class.Class)
			require.NoError(ct, err)
			require.Equal(ct, int64(objectCount), counts[maintenanceNode])
		}, 3*time.Minute, time.Second, "async replication must refill the maintenance node")
	} else {
		require.Never(t, func() bool {
			counts, err := nodeObjectCounts(t, class.Class)
			return err == nil && counts[maintenanceNode] > 0
		}, 20*time.Second, time.Second, "async replication must not reach the maintenance node")
	}

	require.Equal(t, http.StatusServiceUnavailable, readyStatus(t, compose.GetWeaviateNode(3).URI()),
		"the maintenance node must stay out of client traffic")
}

func nodeObjectCounts(t *testing.T, class string) (map[string]int64, error) {
	verbose := verbosity.OutputVerbose
	params := nodes.NewNodesGetClassParams().WithClassName(class).WithOutput(&verbose)
	body, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
	if err != nil {
		return nil, err
	}
	if body.Payload == nil {
		return nil, fmt.Errorf("nil payload from NodesGetClass")
	}
	counts := make(map[string]int64, len(body.Payload.Nodes))
	for _, n := range body.Payload.Nodes {
		for _, s := range n.Shards {
			counts[n.Name] += s.ObjectCount
		}
	}
	return counts, nil
}

func readyStatus(t *testing.T, uri string) int {
	t.Helper()
	resp, err := http.Get("http://" + uri + "/v1/.well-known/ready")
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}
