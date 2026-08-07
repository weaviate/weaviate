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

package asyncrep_battle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
)

const overridePath = "/etc/weaviate/runtime-overrides.yaml"

// sentinelID is excluded from every churn/delete id space so the CL=ALL
// readiness probe can never be deleted out from under a readiness check.
const sentinelID = strfmt.UUID("00000000-feed-face-0000-000000000001")

// buildCompose starts a 3-node cluster with the battle env recipe: fastest
// allowed hashbeat cadence, runtime overrides polled at 1s, metrics enabled,
// and panics fatal so any drop-guard regression crashes a node loudly.
func buildCompose(ctx context.Context, t *testing.T, extraEnv map[string]string) *docker.DockerCompose {
	t.Helper()
	builder := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_REPLICATION_FREQUENCY", "5s").
		WithWeaviateEnv("ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING", "1s").
		WithWeaviateEnv("ASYNC_REPLICATION_PROPAGATION_DELAY", "1s").
		WithWeaviateEnv("ASYNC_REPLICATION_LOGGING_FREQUENCY", "1s").
		WithWeaviateEnv("ASYNC_REPLICATION_SCHEDULER_WORKERS", "10").
		WithWeaviateEnv("ASYNC_REPLICATION_ROOT_PREFILTER_BATCH_SIZE", "8").
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("DISABLE_RECOVERY_ON_PANIC", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(""),
			ContainerFilePath: overridePath,
			FileMode:          0o644,
		})
	for k, v := range extraEnv {
		builder = builder.WithWeaviateEnv(k, v)
	}
	compose, err := builder.Start(ctx)
	require.NoError(t, err, "battle cluster failed to start")
	return compose
}

// battleClass returns an RF=3 class with time-based deletion resolution; the
// per-class cadence overrides are clamped to the 5s/1s floors server-side.
func battleClass(name string, shards int64, mt bool) *models.Class {
	class := &models.Class{
		Class:      name,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{"text"}},
			{Name: "ver", DataType: []string{"int"}},
			{Name: "wid", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor:           3,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			AsyncConfig: &models.ReplicationAsyncConfig{
				Frequency:                 i64(5000),
				FrequencyWhilePropagating: i64(1000),
				PropagationDelay:          i64(1000),
			},
		},
	}
	if mt {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	} else {
		class.ShardingConfig = map[string]interface{}{"desiredCount": shards}
	}
	return class
}

// ensureAllRunning restores any node a failed sibling subtest left stopped.
func ensureAllRunning(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	for n := 1; n <= 3; n++ {
		require.NoError(t, compose.EnsureRunning(ctx, n-1))
	}
}

// upsertObjectEventually PUTs (idempotent create-or-replace) until it lands,
// riding out transient failures right after a node was stopped.
func upsertObjectEventually(t *testing.T, uri string, obj *models.Object, cl types.ConsistencyLevel) {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{"class": obj.Class, "id": obj.ID, "properties": obj.Properties})
	require.NoError(t, err)
	putURL := fmt.Sprintf("http://%s/v1/objects/%s/%s?consistency_level=%s", uri, obj.Class, obj.ID, cl)
	postURL := fmt.Sprintf("http://%s/v1/objects?consistency_level=%s", uri, cl)
	client := &http.Client{Timeout: 10 * time.Second}
	attempt := func(method, url string) (int, string) {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(raw)
	}
	var lastStatus int
	var lastBody string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		// PUT is update-only (404 on unknown ids); fall back to POST-create.
		status, respBody := attempt(http.MethodPut, putURL)
		if status == http.StatusNotFound {
			status, respBody = attempt(http.MethodPost, postURL)
			if status == http.StatusUnprocessableEntity && strings.Contains(respBody, "already exists") {
				return
			}
		}
		if status == http.StatusOK {
			return
		}
		lastStatus, lastBody = status, respBody
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("upsert of %s never succeeded on %s (last status %d: %.300s)", obj.ID, uri, lastStatus, lastBody)
}

func i64(v int64) *int64 { return &v }

// seedSentinel writes the readiness probe object at CL=ALL.
func seedSentinel(t *testing.T, uri, class string) strfmt.UUID {
	t.Helper()
	obj := &models.Object{
		ID:         sentinelID,
		Class:      class,
		Properties: map[string]interface{}{"contents": "sentinel"},
	}
	require.NoError(t, common.CreateObjectCL(t, uri, obj, types.ConsistencyLevelAll))
	return sentinelID
}

// stopNode/startNode/ensureRunning wrap the compose helpers, translating this
// package's 1-based node numbers (GetWeaviateNode convention) to the 0-based
// container-name suffix the compose node APIs resolve.
func stopNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, timeout *time.Duration) {
	t.Helper()
	require.NoError(t, compose.StopNode(ctx, n-1, timeout))
}

// cycleNode stops (nil = graceful SIGTERM, &zero = SIGKILL) then starts node n
// and returns its NEW URI — host ports remap on every start.
func cycleNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, timeout *time.Duration, class string, probeID strfmt.UUID) string {
	t.Helper()
	stopNode(ctx, t, compose, n, timeout)
	return startNodeAndWait(ctx, t, compose, n, class, probeID)
}

// startNodeAndWait starts an already-stopped node, waits for readiness of the
// given class, and returns the fresh URI.
func startNodeAndWait(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, class string, probeID strfmt.UUID) string {
	t.Helper()
	require.NoError(t, compose.StartNode(ctx, n-1))
	require.NoError(t, compose.EnsureRunning(ctx, n-1))
	uri := compose.GetWeaviateNode(n).URI()
	common.WaitForNodeReadyForClass(t, uri, class, probeID)
	return uri
}

// nodeURIs re-reads all three node URIs; never cache these across restarts.
func nodeURIs(compose *docker.DockerCompose) []string {
	return []string{
		compose.GetWeaviateNode(1).URI(),
		compose.GetWeaviateNode(2).URI(),
		compose.GetWeaviateNode(3).URI(),
	}
}

// clusterURIs returns the cluster-internal API endpoints for all three nodes.
func clusterURIs(compose *docker.DockerCompose) []string {
	return []string{
		compose.GetWeaviateNode(1).ClusterURI(),
		compose.GetWeaviateNode(2).ClusterURI(),
		compose.GetWeaviateNode(3).ClusterURI(),
	}
}

// peersOf returns the 1-based node indexes other than n.
func peersOf(n int) []int {
	peers := make([]int, 0, 2)
	for i := 1; i <= 3; i++ {
		if i != n {
			peers = append(peers, i)
		}
	}
	return peers
}

var sigkill = time.Duration(0)
