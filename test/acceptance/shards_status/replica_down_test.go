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

package shards_status

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	clusterSize = 5
	stoppedNode = 4 // weaviate-4, not a RAFT voter
	className   = "ShardsStatusReplicaDown"
	mtClassName = "ShardsStatusTenants"
)

func TestShardsStatus_ReplicaDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(clusterSize).Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Errorf("terminate test containers: %v", err)
		}
	})
	helper.SetupClient(compose.GetWeaviate().URI())

	helper.CreateClass(t, &models.Class{
		Class:             className,
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		ShardingConfig:    map[string]any{"desiredCount": clusterSize},
	})
	helper.CreateClass(t, &models.Class{
		Class:              mtClassName,
		Vectorizer:         "none",
		ReplicationConfig:  &models.ReplicationConfig{Factor: 3},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true, AutoTenantActivation: true},
	})
	helper.CreateTenants(t, mtClassName, []*models.Tenant{
		{Name: "hot", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "cold", ActivityStatus: models.TenantActivityStatusCOLD},
	})

	uris := make([]string, clusterSize)
	for n := range uris {
		uris[n] = compose.GetWeaviateNode(n + 1).URI()
	}

	t.Run("every node answers with all replicas up", func(t *testing.T) {
		for n, uri := range uris {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assertShardsAnswered(ctx, c, uri, "")
			}, time.Minute, 250*time.Millisecond, "weaviate-%d", n)
		}
		requireTenantCold(t)
	})

	require.NoError(t, compose.StopNode(ctx, stoppedNode, nil))

	t.Run("every other node answers with one node down", func(t *testing.T) {
		for round := range 20 {
			for n, uri := range uris {
				if n == stoppedNode {
					continue
				}
				assertShardsAnswered(ctx, t, uri, fmt.Sprintf("weaviate-%d", stoppedNode))
				if t.Failed() {
					t.Fatalf("weaviate-%d answered wrong in round %d", n, round)
				}
			}
		}
		requireTenantCold(t)
	})
}

// assertShardsAnswered expects every replica READY except down, and the COLD tenant UNAVAILABLE.
func assertShardsAnswered(ctx context.Context, c assert.TestingT, uri, down string) {
	statusOn := func(node string) string {
		if node == down {
			return "UNAVAILABLE"
		}
		return "READY"
	}

	shards, ok := getShards(ctx, c, uri, className, "")
	if ok && assert.Len(c, shards, clusterSize) {
		for _, shard := range shards {
			assert.Len(c, shard.PerNodeStatus, 3, "shard %s", shard.Name)
			for node, status := range shard.PerNodeStatus {
				assert.Equal(c, statusOn(node), status, "shard %s on %s", shard.Name, node)
			}
			assert.Zero(c, shard.VectorQueueSize, "shard %s", shard.Name) //nolint:staticcheck // older clients still read it
		}
	}

	shards, ok = getShards(ctx, c, uri, mtClassName, "")
	if ok && assert.Len(c, shards, 2) {
		for _, shard := range shards {
			assert.Len(c, shard.PerNodeStatus, 3, "tenant %s", shard.Name)
			for node, status := range shard.PerNodeStatus {
				want := "UNAVAILABLE"
				if shard.Name == "hot" {
					want = statusOn(node)
				}
				assert.Equal(c, want, status, "tenant %s on %s", shard.Name, node)
			}
		}
	}

	shards, ok = getShards(ctx, c, uri, mtClassName, "cold")
	if ok && assert.Len(c, shards, 1) {
		for node, status := range shards[0].PerNodeStatus {
			assert.Equal(c, "UNAVAILABLE", status, "tenant cold on %s", node)
		}
	}
}

func getShards(ctx context.Context, c assert.TestingT, uri, class, tenant string) (models.ShardStatusList, bool) {
	u := fmt.Sprintf("http://%s/v1/schema/%s/shards", uri, class)
	if tenant != "" {
		u += "?tenant=" + url.QueryEscape(tenant)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if !assert.NoError(c, err) {
		return nil, false
	}
	resp, err := http.DefaultClient.Do(req)
	if !assert.NoError(c, err, u) {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if !assert.NoError(c, err, u) || !assert.Equal(c, http.StatusOK, resp.StatusCode, "%s: %s", u, body) {
		return nil, false
	}
	var shards models.ShardStatusList
	return shards, assert.NoError(c, json.Unmarshal(body, &shards), u)
}

// requireTenantCold fails if a status read activated the COLD tenant.
func requireTenantCold(t *testing.T) {
	t.Helper()
	resp, err := helper.GetTenants(t, mtClassName)
	require.NoError(t, err)
	for _, tenant := range resp.Payload {
		if tenant.Name == "cold" {
			require.Equal(t, models.TenantActivityStatusCOLD, tenant.ActivityStatus)
			return
		}
	}
	t.Fatal("tenant cold not found")
}
