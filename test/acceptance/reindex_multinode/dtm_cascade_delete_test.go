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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// Pins weaviate/0-weaviate-issues#231 end-to-end against a real 3-node
// cluster: a reindex task record produced by `change-tokenization` must
// not survive its class being dropped, must not contaminate a same-name
// recreate, and must not resurrect after a rolling restart replays the
// RAFT log. The cascade lives in the FSM apply path, so it deterministically
// re-fires on every node during replay — the rolling-restart phase is the
// crash-safety axis.
func TestMultiNode_DeleteRecreateCleansReindexTasks(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "DTMCascadeDelete"

	uri := restURIOf(compose, 1)

	trueVal := true
	createCollection(t, uri, className, 3, 3, []*models.Property{
		{
			Name:            "text",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})

	// Small dataset — we only need migration to produce a task record.
	texts := make([]string, 30)
	for i := range texts {
		texts[i] = fmt.Sprintf("alpha beta gamma %d", i)
	}
	importObjects(t, uri, className, texts)

	// Run a migration → first task record under "reindex" namespace.
	task1 := reindexhelpers.SubmitIndexUpdate(t, uri, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	reindexhelpers.AwaitReindexFinished(t, uri, task1,
		reindexhelpers.WithTimeout(180*time.Second))
	t.Logf("first migration FINISHED: task=%s", task1)

	// Spot-check pre-delete: task record is present on this node's view.
	existsPre, okPre := taskExists(t, uri, task1)
	require.Truef(t, okPre && existsPre,
		"pre-delete: reindex task %s must be in /v1/tasks (fetch ok=%v)", task1, okPre)

	deleteCollection(t, uri, className)
	t.Logf("deleted class %s", className)

	// Cascade ran inside the DELETE_CLASS apply. Every replica is the
	// same RAFT FSM, so the task must be gone from every node's task list.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		assertTaskGone(t, restURIOf(compose, nodeIdx), task1,
			fmt.Sprintf("node-%d post-delete", nodeIdx))
	}

	// Same-name recreate must not inherit FAILED / FINISHED pills.
	createCollection(t, uri, className, 3, 3, []*models.Property{
		{
			Name:            "text",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	t.Logf("recreated class %s with fresh schema", className)

	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		assertIndexesReady(t, restURIOf(compose, nodeIdx), className,
			fmt.Sprintf("node-%d post-recreate", nodeIdx))
	}

	// Crash-safety axis: a rolling restart re-applies the RAFT log on
	// every node. If the cascade weren't part of the deterministic FSM
	// apply, replay would resurrect the dead task records on (at least
	// one of) the nodes.
	rollingRestartCluster(ctx, t, compose)
	t.Logf("rolling restart complete; re-verifying clean state")

	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		nodeURI := restURIOf(compose, nodeIdx)
		assertTaskGone(t, nodeURI, task1,
			fmt.Sprintf("node-%d post-restart", nodeIdx))
		assertIndexesReady(t, nodeURI, className,
			fmt.Sprintf("node-%d post-restart", nodeIdx))
	}

	// The recreated class must accept its own fresh migration — proves the
	// extractor opt-in didn't break unrelated tasks and the new collection
	// has a real working DTM lifecycle, not just a clean static view.
	// Re-resolve the node URI because testcontainers picks a new
	// ephemeral host port on every container start.
	postRestartURI := restURIOf(compose, 1)
	importObjects(t, postRestartURI, className, texts[:10])
	task2 := reindexhelpers.SubmitIndexUpdate(t, postRestartURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	require.NotEqual(t, task1, task2,
		"recreated class must produce a fresh task ID, not the old one")
	reindexhelpers.AwaitReindexFinished(t, postRestartURI, task2,
		reindexhelpers.WithTimeout(180*time.Second))
	t.Logf("post-recreate migration FINISHED: task=%s", task2)
}

// taskExists returns true if the reindex namespace contains a task with
// the given ID on the queried node. Per-node scope (no /v1/tasks fan-out)
// so callers can pin per-replica behaviour.
func taskExists(t *testing.T, restURI, taskID string) (exists, ok bool) {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	if !ok {
		return false, false
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return true, true
		}
	}
	return false, true
}

func assertTaskGone(t *testing.T, restURI, taskID, label string) {
	t.Helper()
	// /v1/tasks is served from the coordinator's FSM-replicated view — the
	// cascade applies inside the same FSM, so absence is visible immediately.
	// The tolerant read retries post-restart transient non-200s (gRPC
	// reconnect); only a successful read with the task absent passes.
	require.Eventuallyf(t, func() bool {
		exists, ok := taskExists(t, restURI, taskID)
		return ok && !exists
	}, 20*time.Second, 50*time.Millisecond,
		"%s: reindex task %s must NOT be in /v1/tasks (cascade deleted on DELETE_CLASS)",
		label, taskID)
}

func assertIndexesReady(t *testing.T, restURI, className, label string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		indexes, ok := reindexhelpers.TryGetIndexes(restURI, className)
		if !ok {
			return false // transient post-restart read (gRPC reconnect) — retry
		}
		for _, p := range indexes.Properties {
			for _, idx := range p.Indexes {
				// Anything other than "ready" on a freshly-recreated class
				// is the bug shape from #231: stale task records joining
				// the index-status materialisation by class name.
				if idx.Status != "ready" {
					t.Logf("%s: property %s/index %s status=%s (not ready)",
						label, p.Name, idx.Type, idx.Status)
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond,
		"%s: every index on the recreated class %s must report status=ready",
		label, className)

	// One more synchronous read to capture an assertable shape for the
	// failure message (Eventually only stops at success).
	indexes := reindexhelpers.GetIndexes(t, restURI, className)
	for _, p := range indexes.Properties {
		for _, idx := range p.Indexes {
			assert.Equalf(t, "ready", idx.Status,
				"%s: property %s/index %s should be ready, got %s",
				label, p.Name, idx.Type, idx.Status)
		}
	}
}
