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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_EnableRangeable_SchemaFlagStaysFalseUntilAllUnitsTerminal
// pins weaviate/weaviate#12189: with staggered shard completion,
// indexRangeFilters must stay false until every unit is COMPLETED
// (AllUnitsTerminal), not flip early on the first shard to finish. The
// gate is unit-level, not task.Status=="FINISHED" - the legitimate flip
// runs during the brief SWAPPING window before finalize-to-FINISHED commits.
func TestMultiNode_EnableRangeable_SchemaFlagStaysFalseUntilAllUnitsTerminal(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "StaggeredRangeableFlip"
	const propName = "score"
	// 50_000 keeps the COMPLETED+IN_PROGRESS overlap window measured in
	// seconds, not milliseconds, so the 50ms poll below reliably samples
	// it; a smaller fixture risks a spurious failure on a fast runner.
	const totalObjects = 50_000

	defer createPreMigrationScoreCollection(t, compose, className, propName, totalObjects)()

	var (
		sawSchemaTrueBeforeAllUnitsDone atomic.Bool
		sawStaggeredCompletion          atomic.Bool
		sampleRuns                      atomic.Int64
		stopCh                          = make(chan struct{})
		wg                              sync.WaitGroup
	)

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, propName,
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)

	// Schema is read before tasks: both are monotonic (false->true,
	// non-terminal->COMPLETED), so any ordering skew can only under-count
	// violations, never fabricate one. Both reads use this node's local
	// state to avoid a leader-vs-local clock skew between the two calls.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		wg.Add(1)
		uri := restURIOf(compose, nodeIdx)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
				}
				cls, ok := reindexhelpers.FetchClass(uri, className, true)
				if !ok {
					continue
				}
				var flagTrue bool
				for _, prop := range cls.Properties {
					if prop.Name == propName && prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
						flagTrue = true
					}
				}

				tasks, ok := reindexhelpers.TryFetchTasks(uri)
				if !ok {
					continue
				}
				var task *models.DistributedTask
				for _, tk := range tasks["reindex"] {
					if tk.ID == taskID {
						taskCopy := tk
						task = &taskCopy
						break
					}
				}
				if task == nil {
					continue
				}
				sampleRuns.Add(1)

				allCompleted := len(task.Units) > 0
				anyCompleted := false
				anyInProgress := false
				for _, u := range task.Units {
					switch u.Status {
					case "COMPLETED":
						anyCompleted = true
					case "IN_PROGRESS", "PENDING":
						anyInProgress = true
						allCompleted = false
					default:
						allCompleted = false
					}
				}
				// Gate is unit-level (AllUnitsTerminal), not
				// task.Status=="FINISHED": the flip legitimately runs
				// during the SWAPPING window before finalize-to-FINISHED
				// commits (see needsClusterWideFlipAtCompletion).
				allUnitsDone := allCompleted
				if anyCompleted && anyInProgress {
					sawStaggeredCompletion.Store(true)
				}

				if flagTrue && !allUnitsDone {
					sawSchemaTrueBeforeAllUnitsDone.Store(true)
					t.Logf("GH #12189 REPRO: node's schema already says indexRangeFilters=true "+
						"while at least one unit is not yet COMPLETED (task status=%s units=%v)",
						task.Status, unitStatuses(task.Units))
				}
			}
		}()
	}
	// LIFO: fires before the cluster-teardown defers, so an early exit
	// doesn't leave these polling goroutines running against a
	// tearing-down cluster.
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	// Guards against a vacuous pass: without this, a too-small fixture
	// could finish before any goroutine samples it.
	awaitReindexMidFlight(t, restURIOf(compose, 1), taskID, 120*time.Second)

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Keep sampling briefly past FINISHED to catch the converged state too.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			cls, ok := reindexhelpers.FetchClass(restURIOf(compose, nodeIdx), className, false)
			if !ok {
				return false
			}
			for _, prop := range cls.Properties {
				if prop.Name == propName {
					if prop.IndexRangeFilters == nil || !*prop.IndexRangeFilters {
						return false
					}
				}
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond,
		"indexRangeFilters should be true on every node once the task is FINISHED")

	t.Logf("sampled %d (task,schema) pairs across 3 nodes; staggered-completion observed=%v",
		sampleRuns.Load(), sawStaggeredCompletion.Load())

	require.True(t, sawStaggeredCompletion.Load(),
		"test fixture should produce a genuine staggered window (some units COMPLETED while "+
			"others still IN_PROGRESS/PENDING) - without one the deferred-flip assertion is vacuous; "+
			"increase totalObjects or reduce concurrency if this flakes")

	assert.False(t, sawSchemaTrueBeforeAllUnitsDone.Load(),
		"GH #12189: indexRangeFilters must stay false until every unit is COMPLETED. Any "+
			"true-while-a-unit-is-still-building observation reproduces the per-shard early-flip "+
			"bug (the first shard's OnMigrationComplete flipping the flag cluster-wide while "+
			"siblings were still building).")
}

// unitStatuses formats unit statuses for the log line above.
func unitStatuses(units []*models.DistributedTaskUnit) map[string]string {
	out := make(map[string]string, len(units))
	for _, u := range units {
		out[u.ID] = u.Status
	}
	return out
}
