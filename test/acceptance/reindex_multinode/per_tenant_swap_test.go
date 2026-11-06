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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// The large tenant has to outlast the small one's prepare and swap by a wide
// margin, so the small tenant's swap is observable while the task still runs.
const perTenantLargeObjects = 60_000

// perTenantSwapFixture builds a small and large tenant whose objects are
// two-word phrases, so only a field-tokenized query matches them whole.
func perTenantSwapFixture(t *testing.T, compose *docker.DockerCompose, className string) {
	t.Helper()
	restURI := compose.GetWeaviate().URI()
	createMTCollection(t, restURI, className, 3, textProps("text"))
	createTenants(t, restURI, className, []string{"small", "large"})
	importObjectsTenant(t, restURI, className, "small", []string{"needle phrase", "other words"})

	const batchSize = 1000
	for start := 0; start < perTenantLargeObjects; start += batchSize {
		objects := make([]map[string]any, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			objects = append(objects, map[string]any{
				"class": className, "tenant": "large",
				"properties": map[string]any{"text": fmt.Sprintf("bulk %d", i)},
			})
		}
		body, err := json.Marshal(map[string]any{"objects": objects})
		require.NoError(t, err)
		resp, err := http.Post(fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json", bytes.NewReader(body))
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "batch import: %s", string(respBody))
	}
}

// smallTenantMigratedOnEveryNode reports whether every node answers the small
// tenant with field tokenization: the whole phrase matches, a single word of
// it does not.
func smallTenantMigratedOnEveryNode(t *testing.T, compose *docker.DockerCompose, className string) bool {
	for i := 1; i <= 3; i++ {
		uri := compose.GetWeaviateNode(i).URI()
		whole, err := runBM25QueryOnNodeTenant(t, uri, className, "small", "needle phrase")
		if err != nil || len(whole) != 1 {
			return false
		}
		word, err := runBM25QueryOnNodeTenant(t, uri, className, "small", "needle")
		if err != nil || len(word) != 0 {
			return false
		}
	}
	return true
}

func reindexTaskStatus(restURI, taskID string) string {
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	if !ok {
		return ""
	}
	for _, byNamespace := range tasks {
		for _, task := range byNamespace {
			if task.ID == taskID {
				return task.Status
			}
		}
	}
	return ""
}

// Pins that a tenant swaps once its own replicas are ready: the small tenant
// serves the new tokenization while the large one reindexes and after a node
// restart, and the schema flips once the large tenant finishes.
func TestMultiNode_PerTenantSwap_SmallTenantServesBeforeLargeFinishes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "PerTenantSwap"
	perTenantSwapFixture(t, compose, className)
	restURI := compose.GetWeaviate().URI()

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	require.Eventually(t, func() bool {
		return smallTenantMigratedOnEveryNode(t, compose, className)
	}, 2*time.Minute, 200*time.Millisecond, "the small tenant must swap on every replica")
	require.Equal(t, "STARTED", reindexTaskStatus(restURI, taskID),
		"the small tenant must swap while the large one is still reindexing")

	cycleNodeFast(ctx, t, compose, 1)
	require.Eventually(t, func() bool {
		return smallTenantMigratedOnEveryNode(t, compose, className)
	}, time.Minute, 500*time.Millisecond, "a restarted replica must keep serving the small tenant's migrated index")

	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(10*time.Minute))
	for i := 1; i <= 3; i++ {
		reindexhelpers.AwaitTokenizationVisible(t, compose.GetWeaviateNode(i).URI(), className, "text", "field")
	}
	require.True(t, smallTenantMigratedOnEveryNode(t, compose, className))
}

// Pins the cancel and status rulings: cancelling while the large tenant
// still runs keeps the small tenant's swap, reports partial with tenant
// counts, and a resubmit skips the small tenant and finishes the rest.
func TestMultiNode_PerTenantSwap_CancelLeavesPartialThenResubmitFinishes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "PerTenantPartial"
	perTenantSwapFixture(t, compose, className)
	restURI := compose.GetWeaviate().URI()

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	require.Eventually(t, func() bool {
		return smallTenantMigratedOnEveryNode(t, compose, className)
	}, 2*time.Minute, 200*time.Millisecond, "the small tenant must swap on every replica")

	cancelled := reindexhelpers.CancelIndex(t, restURI, className, "text", "searchable")
	require.Equal(t, "CANCELLED", cancelled.Status, "a cancel is never refused while the reindex runs")
	require.Equal(t, "CANCELLED", reindexTaskStatus(restURI, taskID))

	require.Eventually(t, func() bool {
		for _, prop := range reindexhelpers.GetIndexes(t, restURI, className).Properties {
			for _, idx := range prop.Indexes {
				if prop.Name == "text" && idx.Type == "searchable" {
					return idx.Status == "partial" && idx.MigratedTenants == 1 && idx.TotalTenants == 2
				}
			}
		}
		return false
	}, time.Minute, 500*time.Millisecond, "the indexes endpoint must read partial, 1 of 2 tenants")
	require.True(t, smallTenantMigratedOnEveryNode(t, compose, className),
		"the decided tenant keeps its swap after the cancel")

	retry := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, retry, reindexhelpers.WithTimeout(10*time.Minute))
	for i := 1; i <= 3; i++ {
		reindexhelpers.AwaitTokenizationVisible(t, compose.GetWeaviateNode(i).URI(), className, "text", "field")
		ids, err := runBM25QueryOnNodeTenant(t, compose.GetWeaviateNode(i).URI(), className, "large", "bulk 42")
		require.NoError(t, err)
		require.Len(t, ids, 1, "node %d: the large tenant must answer with field tokenization", i)
	}
	require.True(t, smallTenantMigratedOnEveryNode(t, compose, className))
}
