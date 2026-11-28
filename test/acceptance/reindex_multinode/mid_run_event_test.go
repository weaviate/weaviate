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

package reindex_multinode

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

type tenantCount struct {
	tenant, query string
	filter        bool
	want          int
}

func TestMultiNode_RollingRestartAfterMidRunEvent(t *testing.T) {
	changeTokenizationCounts := []tenantCount{
		{"small", "needle phrase", false, 1},
		{"small", "needle", false, 0},
		{"small", "needle phrase", true, 1},
		{"small", "needle", true, 0},
		{"large", "bulk 42", false, 1},
		{"large", "42", false, 0},
		{"large", "bulk 42", true, 1},
		{"large", "42", true, 0},
	}
	tests := []struct {
		name       string
		filterable bool
		indexType  string
		body       string
		midRun     func(ctx context.Context, t *testing.T, compose *docker.DockerCompose, uri, className, taskID string)
		counts     []tenantCount
	}{
		{
			name: "node killed during tenant switches", filterable: true,
			indexType: "searchable", body: `{"tokenization":"field"}`,
			midRun: func(ctx context.Context, t *testing.T, compose *docker.DockerCompose, uri, className, taskID string) {
				require.Eventually(t, func() bool {
					return smallTenantMigratedOnEveryNode(t, compose, className)
				}, 2*time.Minute, 200*time.Millisecond, "the small tenant must switch on every replica")
				require.Equal(t, "STARTED", reindexTaskStatus(uri, taskID), "the kill must land while the large tenant builds")
				cycleNodeFastKill(ctx, t, compose, (raftLeaderIndex(t, compose)+1)%3)
			},
			counts: changeTokenizationCounts,
		},
		{
			name: "rolling restart during enable-filterable", filterable: false,
			indexType: "filterable", body: `{}`,
			midRun: func(ctx context.Context, t *testing.T, compose *docker.DockerCompose, uri, className, taskID string) {
				awaitReindexMidFlight(t, uri, taskID, time.Minute)
				rollingRestartCluster(ctx, t, compose)
			},
			counts: []tenantCount{
				{"small", "needle", true, 1},
				{"small", "other", true, 1},
				{"small", "phrase words", true, 0},
				{"large", "42", true, 1},
				{"large", "59999", true, 1},
			},
		},
		{
			name: "tenant created mid-run", filterable: true,
			indexType: "searchable", body: `{"tokenization":"field"}`,
			midRun: func(ctx context.Context, t *testing.T, compose *docker.DockerCompose, uri, className, taskID string) {
				awaitReindexMidFlight(t, uri, taskID, time.Minute)
				createTenants(t, uri, className, []string{"born"})
				batchImportTenant(t, uri, className, "born", []string{"The Hobbit", "Hobbit"})
			},
			counts: append([]tenantCount{
				{"born", "The Hobbit", true, 1},
				{"born", "Hobbit", true, 1},
				{"born", "The", true, 0},
				{"born", "The Hobbit", false, 1},
				{"born", "Hobbit", false, 1},
			}, changeTokenizationCounts...),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			compose, cleanup := start3NodeReindexCluster(ctx, t)
			defer cleanup()
			defer dumpContainerLogs(ctx, t, compose)

			const className = "MidRunEvent"
			perTenantSwapFixture(t, compose, className, []*models.Property{{
				Name: "text", DataType: []string{"text"}, Tokenization: "word",
				IndexFilterable: reindexhelpers.BoolPtr(tc.filterable),
			}})
			uri := restURIOf(compose, raftLeaderIndex(t, compose)+1)
			taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "text", tc.indexType, tc.body)
			tc.midRun(ctx, t, compose, uri, className, taskID)

			// Fresh URIs, since a restart moves a node's port, and the indexes endpoint, since the
			// run can end during the event and the task is removed when it ends.
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				tasks, ok := reindexhelpers.TryFetchTasks(restURIOf(compose, 1))
				if !assert.True(ct, ok) {
					return
				}
				for _, task := range tasks["reindex"] {
					assert.NotEqualf(ct, taskID, task.ID, "task %s is still %s: %s", taskID, task.Status, task.Error)
				}
				indexes, ok := reindexhelpers.TryGetIndexes(restURIOf(compose, 1), className)
				if !assert.True(ct, ok) || !assert.Len(ct, indexes.Properties, 1) {
					return
				}
				ready := slices.ContainsFunc(indexes.Properties[0].Indexes, func(idx reindexhelpers.IndexEntry) bool {
					return idx.Type == tc.indexType && idx.Status == "ready" && idx.TaskID == ""
				})
				assert.Truef(ct, ready, "indexes: %+v", indexes.Properties[0].Indexes)
			}, 10*time.Minute, time.Second, "the run must finish")
			if tc.indexType == "searchable" {
				awaitTokenizationOnAllNodes(t, compose, className, "text", "field")
			}
			requireTenantCounts(t, compose, className, tc.counts, "after the run")
			rollingRestartCluster(ctx, t, compose)
			requireTenantCounts(t, compose, className, tc.counts, "after a rolling restart")
		})
	}
}

func requireTenantCounts(t *testing.T, compose *docker.DockerCompose, className string, counts []tenantCount, phase string) {
	t.Helper()
	for node := 1; node <= 3; node++ {
		uri := restURIOf(compose, node)
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			for _, c := range counts {
				var ids []string
				var err error
				if c.filter {
					ids, err = executeGraphQL(uri, className, fmt.Sprintf(
						`{ Get { %s(tenant: %q, where: {path: ["text"], operator: Equal, valueText: %q}) { _additional { id } } } }`,
						className, c.tenant, c.query))
				} else {
					ids, err = runBM25QueryOnNodeTenant(t, uri, className, c.tenant, c.query)
				}
				if assert.NoErrorf(ct, err, "%+v", c) {
					assert.Lenf(ct, ids, c.want, "node %d %+v", node, c)
				}
			}
		}, time.Minute, 500*time.Millisecond, "node %d must answer exact counts %s", node, phase)
	}
}
