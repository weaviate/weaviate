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
	"regexp"
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

// TestStartupProgressLogsShardLoading restarts a node that owns several
// tenant shards and asserts the shard-loading progress reaches the logs and
// the startup metrics reach /metrics.
//
// Only the final "local DB loaded from schema" line is asserted. It is emitted
// whenever the reload runs, with counts freshly scanned from the restored
// schema, so the assertion holds however fast the load finishes. The periodic
// "loading local DB from schema" line needs a load slower than its 5s ticker
// and is left unasserted.
//
// The node also recounts property lengths at startup, which must count the
// objects of every shard.
func TestStartupProgressLogsShardLoading(t *testing.T) {
	ctx := context.Background()

	const (
		classCount = 3
		// tenants rather than shards: the test controls what each tenant holds,
		// so every shard has vector state to restore after the restart
		tenantsPerClass  = 2
		objectsPerTenant = 20
	)

	compose, err := docker.New().
		WithWeaviate().
		// Lazy-loaded shards are discounted from the progress totals; force
		// eager loading so every shard counts.
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("RECOUNT_PROPERTIES_AT_STARTUP", "true").
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
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		})

		tenants := make([]*models.Tenant, tenantsPerClass)
		for j := range tenants {
			tenants[j] = &models.Tenant{Name: fmt.Sprintf("tenant%d", j)}
		}
		helper.CreateTenants(t, className, tenants)

		for j, tenant := range tenants {
			objects := make([]*models.Object, objectsPerTenant)
			for k := range objects {
				objects[k] = &models.Object{
					Class:      className,
					Tenant:     tenant.Name,
					Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", k)},
					Vector:     []float32{float32(k), float32(j), 1},
				}
			}
			helper.CreateObjectsBatch(t, objects)
		}
	}

	// The first boot of an empty node never reloads the DB, so everything
	// asserted below can only come from this restart.
	require.NoError(t, compose.RestartAt(ctx, 0, nil))

	assertStartupMetrics(t, ctx, compose, classCount*tenantsPerClass)

	readLogs := func(t require.TestingT) string {
		reader, err := compose.GetWeaviate().Container().Logs(ctx)
		require.NoError(t, err)
		defer reader.Close()
		raw, err := io.ReadAll(reader)
		require.NoError(t, err)
		return string(raw)
	}
	logs := readLogs(t)

	require.Contains(t, logs, "local DB loaded from schema",
		"the reload's progress tracker must report the load")

	total := classCount * tenantsPerClass
	assert.True(t,
		strings.Contains(logs, fmt.Sprintf("shards_total=%d", total)) ||
			strings.Contains(logs, fmt.Sprintf(`"shards_total":%d`, total)),
		"progress fields must carry the full shard count %d, logs:\n%s", total, tail(logs, 40))
	assert.True(t,
		strings.Contains(logs, "progress=100%") ||
			strings.Contains(logs, `"progress":"100%"`),
		"the final progress line must report 100%%, logs:\n%s", tail(logs, 40))

	// reported once the shards have loaded, after the restart returns
	objects := classCount * tenantsPerClass * objectsPerTenant
	objectsField := regexp.MustCompile(fmt.Sprintf(`\bobjects"?[=:]%d\b`, objects))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logs := readLogs(c)
		var recounted bool
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, "Recounting property lengths complete") && objectsField.MatchString(line) {
				recounted = true
			}
		}
		assert.True(c, recounted, "the recount must count all %d objects, logs:\n%s", objects, tail(logs, 40))
	}, time.Minute, time.Second)
}

func tail(logs string, n int) string {
	lines := strings.Split(logs, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// assertStartupMetrics checks /metrics after a restart: ready reported, every shard restored and prefilled once.
func assertStartupMetrics(t *testing.T, ctx context.Context, compose *docker.DockerCompose, shards int) {
	t.Helper()
	container := compose.GetWeaviate().Container()

	// the ready gauge can trail the restart by one readiness poll
	var families map[string]*dto.MetricFamily
	var lastScrapeErr error
	ready := assert.Eventually(t, func() bool {
		families, lastScrapeErr = helper.ScrapeMetrics(ctx, container)
		if lastScrapeErr != nil {
			return false
		}
		ready, ok := helper.FindMetric(families, "weaviate_startup_duration_seconds", nil)
		return ok && ready.GetGauge().GetValue() > 0
	}, 30*time.Second, 500*time.Millisecond)
	if !ready {
		t.Fatalf("the node must report ready; last scrape error: %v", lastScrapeErr)
	}

	sampleCount := func(name string) uint64 {
		m, ok := helper.FindMetric(families, name, nil)
		require.True(t, ok, "missing %s", name)
		return m.GetSummary().GetSampleCount()
	}

	want := uint64(shards)
	assert.Equal(t, want, sampleCount("weaviate_vector_index_restore_duration_seconds"),
		"every shard's HNSW index had commit-log state to restore")
	assert.Equal(t, want, sampleCount("weaviate_vector_cache_prefill_duration_seconds"),
		"eager collections prefill their vector cache inside the shard load")
}
