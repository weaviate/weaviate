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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

func TestMultiNode_RestartInsideMergedBarrier_CommitsAndServes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className     = "MergedBarrierRestart"
		totalObjects  = 50_000
		restartedNode = 2
	)
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path"}
	const expectedPerPath = totalObjects / 4

	trueVal := true
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{"path": paths[i%len(paths)]}
	})

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "pre-migration")

	uri := restURIOf(compose, 1)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — killing node %d inside that window", taskID, observed, restartedNode)
	require.Equalf(t, "PREPARING", observed,
		"the merged window closed before the kill, so the scenario never ran; "+
			"raise totalObjects (currently %d) to widen it", totalObjects)

	cycleNodeFastKill(ctx, t, compose, restartedNode-1)
	t.Logf("node %d restarted", restartedNode)

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID,
		reindexhelpers.WithTimeout(240*time.Second))

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after the task finished")

	rollingRestartCluster(ctx, t, compose)
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after a second restart")

	awaitTokenizationOnAllNodes(t, compose, className, "path", "field")
}

func requireEveryReplicaServes(t *testing.T, compose *docker.DockerCompose,
	className, propName, value string, want int, phase string,
) {
	t.Helper()
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		node := nodeIdx
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			got, err := equalCount(restURIOf(compose, node), className, propName, value)
			if !assert.NoErrorf(ct, err, "node %d could not be queried", node) {
				return
			}
			assert.Equalf(ct, want, got, "node %d serves the wrong count for %s=%q", node, propName, value)
		}, 90*time.Second, 200*time.Millisecond,
			"node %d must serve %d objects for %s=%q %s", node, want, propName, value, phase)
	}
}
