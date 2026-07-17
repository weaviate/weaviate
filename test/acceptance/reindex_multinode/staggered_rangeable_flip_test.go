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
// pins GH weaviate/weaviate#12189: with staggered shard completion,
// indexRangeFilters must stay false until every unit is COMPLETED, not
// flip early on the first shard to finish. The guarantee is unit-level
// (AllUnitsTerminal, the actual gate the schema flip waits for), not
// task.Status=="FINISHED": the flip runs while the task is still
// SWAPPING (a real, non-terminal, externally-visible status - see
// distributedtask.TaskStatusSwapping), the same brief window every
// other deferred migration type already has. Requiring FINISHED here
// would flag that harmless window as a false early-flip.
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

	// Schema fetched BEFORE tasks, on purpose (Copilot review round on
	// PR #12206): unit progress and the schema flag are both monotonic
	// (non-terminal -> COMPLETED; false -> true), so if the legitimate
	// flip lands between these two requests, the ordering below can only
	// under-count violations near that boundary (false negative, safe),
	// never manufacture one (false positive). Concretely: schema is read
	// at the EARLIER instant T1, tasks at the LATER instant T2 (T2>T1).
	// If schema is true at T1 and tasks show not-all-done at T2 (later,
	// so "more done" if anything), that's a genuine violation - the flag
	// was already true before the units were done, full stop. The
	// reverse order (tasks-then-schema, tasks OLDER) can fabricate a
	// false positive: a legitimate post-completion flip observed at the
	// later schema read could pair with a stale not-all-done tasks
	// snapshot taken earlier, before the units actually finished,
	// falsely looking like an early flip.
	//
	// Schema read uses local=true (consistency:false), same-node as the
	// tasks read below (QA re-verify hardening item on PR #12206): a
	// leader-linearizable schema read and a node-local tasks read are
	// two different clocks, opening a theoretical skew window in the T1
	// monotonicity argument above (a stale-leader schema answer paired
	// with a fresher local tasks answer, or vice versa, on a node that
	// isn't the leader). Reading both from this node's own FSM state
	// closes that window: T1 and T2 are now both this node's local
	// clock, so the "T2 can only be more done than T1" ordering argument
	// holds without cross-clock skew.
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
				// The guarantee this test enforces is "the flag stays
				// false while ANY unit is non-terminal, and flips only
				// once ALL units are COMPLETED" - that is the actual
				// gate OnTaskCompleted's flip waits for (AllUnitsTerminal),
				// not task.Status=="FINISHED". task.Status legitimately
				// reaches SWAPPING (a real, non-terminal, externally-
				// visible status - see distributedtask.TaskStatusSwapping's
				// own godoc) after every unit is COMPLETED but before the
				// separate finalize-to-FINISHED RAFT write commits; the
				// flip runs during that SWAPPING window by design, same
				// as every other deferred migration type (see
				// needsClusterWideFlipAtCompletion). Requiring FINISHED
				// here would flag that harmless, bounded window as a
				// false "early flip" (observed in practice: all units
				// COMPLETED, status SWAPPING, flag already true).
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
	// Registered after the goroutines start (and, since defers run LIFO,
	// AFTER the cluster cleanup()/dumpContainerLogs() defers above) so
	// this one fires FIRST on every exit path - including an early
	// require.* failure below, which would otherwise leave these 3
	// polling goroutines running against the cluster during its
	// teardown. A success-only close(stopCh)/wg.Wait() before the
	// cluster stopped covering that path; this covers all of them.
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
			"others still IN_PROGRESS/PENDING) - otherwise this test can't distinguish the fix "+
			"from the pre-fix bug; increase totalObjects or reduce concurrency if this flakes")

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
