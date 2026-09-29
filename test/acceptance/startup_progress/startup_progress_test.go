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

package startup_progress

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestStartupProgressLogsShardLoading restarts a node that owns several shards
// and asserts the shard-loading progress reaches the logs and the startup
// metrics reach /metrics.
//
// Only the final "local DB loaded from schema" line is asserted. It is emitted
// whenever the reload runs, with counts freshly scanned from the restored
// schema, so the assertion holds however fast the load finishes. The periodic
// "loading local DB from schema" line needs a load slower than its 5s ticker
// and is left unasserted.
func TestStartupProgressLogsShardLoading(t *testing.T) {
	ctx := context.Background()

	const (
		classCount     = 3
		shardsPerClass = 2
		// Spread over both shards of a class, so every shard has vector state
		// to restore and a cache to prefill after the restart.
		objectsPerClass = 20
	)

	compose, err := docker.New().
		WithWeaviate().
		// Lazy-loaded shards are discounted from the progress totals; force
		// eager loading so every shard counts.
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		// The startup metrics are read off /metrics after the restart.
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	for i := 0; i < classCount; i++ {
		className := fmt.Sprintf("StartupProgress%d", i)
		helper.CreateClass(t, &models.Class{
			Class:      className,
			Vectorizer: "none",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{"desiredCount": shardsPerClass},
		})

		objects := make([]*models.Object, objectsPerClass)
		for j := range objects {
			objects[j] = &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", j)},
				Vector:     []float32{float32(j), float32(i), 1},
			}
		}
		helper.CreateObjectsBatch(t, objects)
	}

	// The first boot of an empty node never reloads the DB, so everything
	// asserted below can only come from this restart.
	require.NoError(t, compose.RestartAt(ctx, 0, nil))

	assertStartupMetrics(t, ctx, compose, classCount*shardsPerClass)

	reader, err := compose.GetWeaviate().Container().Logs(ctx)
	require.NoError(t, err)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	logs := string(raw)

	require.Contains(t, logs, "local DB loaded from schema",
		"the reload's progress tracker must report the load")

	total := classCount * shardsPerClass
	assert.True(t,
		strings.Contains(logs, fmt.Sprintf("shards_total=%d", total)) ||
			strings.Contains(logs, fmt.Sprintf(`"shards_total":%d`, total)),
		"progress fields must carry the full shard count %d, logs:\n%s", total, tail(logs, 40))
	assert.True(t,
		strings.Contains(logs, "progress=100%") ||
			strings.Contains(logs, `"progress":"100%"`),
		"the final progress line must report 100%%, logs:\n%s", tail(logs, 40))
}

func tail(logs string, n int) string {
	lines := strings.Split(logs, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// assertStartupMetrics pins what a restart exposes on /metrics: the node
// reports when it became ready, every boot phase has run and finished, and
// each eager shard was loaded, restored and prefilled exactly once.
func assertStartupMetrics(t *testing.T, ctx context.Context, compose *docker.DockerCompose, shards int) {
	t.Helper()
	container := compose.GetWeaviate().Container()
	phases := []string{"modules_init", "cluster_open", "raft_open", "raft_bootstrap", "db_reload"}

	// The readiness tracker polls the raft store after the DB restore, and the
	// cluster_open phase ends just after it starts, so both can trail the
	// restart by a poll interval.
	var families map[string]*dto.MetricFamily
	require.Eventually(t, func() bool {
		var err error
		families, err = helper.ScrapeMetrics(ctx, container)
		if err != nil {
			return false
		}
		ready, ok := helper.FindMetric(families, "weaviate_startup_duration_seconds", nil)
		if !ok || ready.GetGauge().GetValue() <= 0 {
			return false
		}
		for _, phase := range phases {
			active, ok := helper.FindMetric(families, "weaviate_startup_phase_active", map[string]string{"phase": phase})
			if !ok || active.GetGauge().GetValue() != 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "the node must report ready with every startup phase finished")

	gauge := func(name string, labels map[string]string) float64 {
		m, ok := helper.FindMetric(families, name, labels)
		require.True(t, ok, "missing %s%v", name, labels)
		return m.GetGauge().GetValue()
	}
	sampleCount := func(name string, labels map[string]string) uint64 {
		m, ok := helper.FindMetric(families, name, labels)
		require.True(t, ok, "missing %s%v", name, labels)
		return m.GetSummary().GetSampleCount()
	}

	assert.Greater(t, gauge("weaviate_startup_ready_timestamp_seconds", nil), float64(0))
	for _, phase := range phases {
		assert.Greater(t, gauge("weaviate_startup_phase_duration_seconds", map[string]string{"phase": phase}), float64(0),
			"phase %s must have run", phase)
	}

	want := uint64(shards)
	assert.Equal(t, want, sampleCount("weaviate_shard_load_duration_seconds", map[string]string{"registration": "eager"}),
		"every shard is loaded eagerly on this restart")
	assert.Zero(t, sampleCount("weaviate_shard_load_duration_seconds", map[string]string{"registration": "lazy"}))
	assert.Equal(t, want, sampleCount("weaviate_vector_index_restore_duration_seconds", map[string]string{"index_type": "hnsw"}),
		"every shard's HNSW index had commit-log state to restore")
	hnswSync := map[string]string{"index_type": "hnsw", "mode": "sync"}
	assert.Equal(t, want, sampleCount("weaviate_vector_cache_prefill_duration_seconds", hnswSync),
		"eager collections prefill their vector cache inside the shard load")
	assert.Zero(t, gauge("weaviate_vector_cache_prefill_active", hnswSync))
}
