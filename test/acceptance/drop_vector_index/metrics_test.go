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

package drop_vector_index

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// metricsPort is Weaviate's default PROMETHEUS_MONITORING_PORT.
const metricsPort = 2112

// TestDropVectorIndex_Metrics_SingleNode covers the two edit-op metric
// journeys nothing below the acceptance layer can: that a drop publishes the
// per-shard series through the real wiring (env gate -> NewMetrics -> refresh
// call sites), and that deleting the tenant reaps them. The unit tests pin
// both behaviours against hand-built metrics; only a scrape of /metrics proves
// the production plumbing exports and retires the series end to end.
func TestDropVectorIndex_Metrics_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		dumpLogsOnFailure(ctx, t, compose)
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()
	container := compose.GetWeaviate().Container()

	const (
		className = "DropVectorIndexMetrics"
		dropped   = "vec"
		sibling   = "sibling"
		tenantA   = "tenant-metrics-a"
		tenantB   = "tenant-metrics-b"
		objCount  = 20
	)

	deleteParams := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
	helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
	defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

	createMTDropClass(t, className, dropped, sibling, tenantA, tenantB)

	for ten, tenant := range []string{tenantA, tenantB} {
		batch := make([]*models.Object, objCount)
		for i := range batch {
			batch[i] = &models.Object{
				ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000090%02d", ten, i)),
				Class:      className,
				Tenant:     tenant,
				Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
				Vectors: models.Vectors{
					dropped: randVec(16, float32(i)),
					sibling: randVec(16, float32(i)),
				},
			}
		}
		helper.CreateObjectsBatch(t, batch)
	}
	time.Sleep(3 * time.Second) // past the 1s dirty-flush, so the strip has segments to owe

	dropTargetVector(t, className, dropped)
	eventuallyTargetVectorRemoved(t, className, dropped)
	waitForNoActiveDropTask(t)

	t.Run("the drop published per-shard series for every tenant", func(t *testing.T) {
		// Asserted after completion, not during: post-drop state is stable. The
		// op-id gauges are forgotten with the op, so what survives is the
		// per-shard active series — one PER TENANT, which is what makes the
		// shard label load-bearing rather than decorative. A single tenant
		// could not tell a correct implementation from one writing every
		// shard's value into one shared series.
		lines := editOpsMetricLines(ctx, t, container, className)
		require.Contains(t, lines, "weaviate_lsm_segment_edit_ops_active",
			"a drop on this shard must export the active gauge through the real wiring")
		for _, tenant := range []string{tenantA, tenantB} {
			require.Contains(t, lines, fmt.Sprintf("shard_name=%q", tenant),
				"every participating tenant must get its own series")
		}
		require.Equal(t, 2, strings.Count(lines, "weaviate_lsm_segment_edit_ops_active{"),
			"two tenants, two active series")
	})

	t.Run("the transformer histogram is exported", func(t *testing.T) {
		// Scraped WITHOUT the class filter on purpose: the histogram carries
		// op_type only — deliberately node-wide, no shard dimension — so a
		// class_name= grep excludes it by construction and would report it
		// missing however well it worked.
		lines := editOpsMetricLinesNoClassFilter(ctx, t, container)
		require.Contains(t, lines, "weaviate_lsm_segment_edit_ops_transformer_duration_seconds_count",
			"the rewrites this drop performed must be timed")
		require.NotContains(t, lines, "transformer_duration_seconds_count{class_name",
			"the histogram must stay node-wide, without a shard dimension")
	})

	t.Run("deleting the tenants reaps every series", func(t *testing.T) {
		require.NoError(t, helper.DeleteTenants(t, className, []string{tenantA, tenantB}))

		// Eventually: the reap runs inside the shard teardown the delete kicks
		// off, which finishes shortly after the HTTP call returns. A scrape
		// error retries rather than hard-failing — asserting on the OUTER t
		// from testify's polling goroutine would abort the whole test on one
		// transient exec hiccup.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			lines, err := editOpsMetricLinesErr(ctx, container, className)
			if err != nil {
				assert.Fail(collect, "scrape /metrics", err.Error())
				return
			}
			assert.Empty(collect, lines,
				"a deleted tenant must leave no edit-op series behind")
		}, time.Minute, time.Second)
	})
}

// execSentinel separates the docker exec stream's multiplex header from the
// command's own output; everything before it is discarded.
const execSentinel = "===EDITOPS-METRICS==="

// editOpsMetricLines is editOpsMetricLinesErr with the error asserted on t;
// for call sites outside a retry loop.
func editOpsMetricLines(ctx context.Context, t require.TestingT, c testcontainers.Container, className string) string {
	lines, err := editOpsMetricLinesErr(ctx, c, className)
	require.NoError(t, err)
	return lines
}

// editOpsMetricLinesNoClassFilter scrapes every edit-op line regardless of
// labels, for the series that carry no class dimension.
func editOpsMetricLinesNoClassFilter(ctx context.Context, t require.TestingT, c testcontainers.Container) string {
	lines, err := editOpsMetricLinesErr(ctx, c, "")
	require.NoError(t, err)
	return lines
}

// editOpsMetricLinesErr scrapes /metrics inside the container and returns the
// edit-op series lines for className. Filtered in-container so only the
// relevant handful of lines cross the exec stream. The scrape's own exit code
// is preserved separately from grep's: "|| true" only on the grep leg, so a
// dead endpoint surfaces as exit 9 instead of reading as zero series — an
// "empty means reaped" assertion must not be satisfiable by a broken scrape.
func editOpsMetricLinesErr(ctx context.Context, c testcontainers.Container, className string) (string, error) {
	// An empty className skips the class filter, for the series that carry no
	// class dimension at all.
	filter := "cat"
	if className != "" {
		filter = fmt.Sprintf("grep 'class_name=\"%s\"'", className)
	}
	cmd := fmt.Sprintf(
		"echo %s; page=$(wget -qO- http://localhost:%d/metrics) || exit 9; "+
			"printf '%%s\\n' \"$page\" | grep lsm_segment_edit_ops | %s || true",
		execSentinel, metricsPort, filter)
	code, reader, err := c.Exec(ctx, []string{"sh", "-c", cmd})
	if err != nil {
		return "", fmt.Errorf("exec scrape: %w", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("read scrape output: %w", err)
	}
	s := string(out)
	if code != 0 {
		return "", fmt.Errorf("scrape exited %d: %s", code, s)
	}
	_, lines, found := strings.Cut(s, execSentinel)
	if !found {
		return "", fmt.Errorf("exec sentinel missing from output: %s", s)
	}
	return strings.TrimSpace(lines), nil
}
