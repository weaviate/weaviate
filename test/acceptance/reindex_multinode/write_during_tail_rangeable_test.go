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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_EnableRangeable_WriteDuringTailLandsInRangeableIndex pins
// the architect-escalated blocker on PR #12206 (see the design-position
// doc in Conversations/2026-07/): deferring the rangeable flip to
// OnTaskCompleted (GH weaviate/weaviate#12189) reopens a per-shard
// write-loss window that the old per-shard early flip inadvertently
// closed for every shard after the first (weaviate/0-weaviate-issues#319,
// rangeable instance).
//
// Mechanism under test: a shard's double-write callbacks (the only write
// coverage during the active migration) are torn down at that shard's
// local swap. With the flip deferred to all-units-terminal, the schema
// flag is still false at that point, so a live write to the property in
// the window [this-shard-swap, cluster-flip] is analyzed under the
// pre-flip schema (HasRangeableIndex=false) and never reaches the
// already-canonical rangeable bucket - silently and permanently, since
// the bucket has already been swapped in. On a staggered multi-shard
// migration this window is minutes per early-completing shard.
//
// The fix (Shard.rangeableForceIndexOverlay, see shard_write_inverted.go)
// forces the write path to treat the property as rangeable while this
// shard is locally ready but the cluster flag hasn't flipped. This test
// writes continuously (not a single well-timed shot) from right after
// task submission until the task finishes, so it doesn't depend on
// catching one exact instant: as long as the staggered window is open for
// a non-trivial duration (the sibling
// TestMultiNode_EnableRangeable_SchemaFlagStaysFalseUntilAllUnitsTerminal
// test already establishes it is, on the same fixture size), several
// writes land inside it with overwhelming probability, and ANY of them
// missing from the post-migration rangeable index fails the test.
func TestMultiNode_EnableRangeable_WriteDuringTailLandsInRangeableIndex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RangeableWriteDuringTail"
	const propName = "score"
	// Matches the staggered-flip sibling test's fixture size: large
	// enough that the COMPLETED+IN_PROGRESS overlap window is measured in
	// seconds, giving the continuous writer below many opportunities to
	// land a write inside it.
	const totalObjects = 50_000
	// Sentinel band disjoint from the main import's [0, totalObjects)
	// range, so a plain range query over the band counts exactly (and
	// only) the writes this test injected.
	const tailSentinelBase = 9_000_000

	defer createPreMigrationScoreCollection(t, compose, className, propName, totalObjects)()

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, propName,
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)

	// Continuous single-object writer: fires every 100ms against node 1,
	// round-robin-independent (single node is enough - the object routes
	// to whichever shard its ID hashes to, and with dozens of writes
	// across 3 shards at least one lands on an already-completed shard
	// with overwhelming probability). Runs until stopCh closes; every
	// write attempted is counted so the final assertion checks an exact
	// number, not just "some writes landed".
	var (
		written  atomic.Int64
		stopCh   = make(chan struct{})
		stopOnce sync.Once
		writerWg sync.WaitGroup
	)
	stopWriter := func() {
		stopOnce.Do(func() { close(stopCh) })
		writerWg.Wait()
	}
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
			}
			n := written.Load()
			score := tailSentinelBase + int(n)
			if postSingleNumericObject(t, restURIOf(compose, 1), className, score) {
				written.Add(1)
			}
		}
	}()
	// Registered right after the writer starts (LIFO: fires before the
	// cluster-teardown defers above) so any early require/timeout exit
	// stops the writer instead of leaving it running against a
	// tearing-down cluster. stopWriter is safe to call again on the
	// normal path below (sync.Once guards the channel close).
	defer stopWriter()

	// Guards against a vacuous pass: without this, a too-small fixture
	// could finish before the writer goroutine has produced any samples.
	awaitReindexMidFlight(t, restURIOf(compose, 1), taskID, 120*time.Second)

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Stop the writer now that the migration has finished; further
	// writes would land under the ordinary (post-flip) path and add
	// nothing to the window this test targets.
	stopWriter()
	tailWriteCount := int(written.Load())
	t.Logf("injected %d tail writes with sentinel scores in [%d, %d) across the migration lifecycle",
		tailWriteCount, tailSentinelBase, tailSentinelBase+tailWriteCount)
	require.Greater(t, tailWriteCount, 0,
		"the continuous writer should have produced at least one successful write during the migration")

	// Every replica must eventually report every tail write via a range
	// query on the sentinel band. A write silently missing from the
	// rangeable bucket (the #319 regression this PR closes) shows up
	// here as a persistent short count, not a transient one, since the
	// migration has already finished and there is no further
	// convergence pending once the flag itself has settled cluster-wide.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			count, err := rangeCount(restURIOf(compose, nodeIdx), className, propName,
				tailSentinelBase, tailSentinelBase+tailWriteCount-1)
			if err != nil || count != tailWriteCount {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond,
		"all replicas should report every tail write once the migration is finished")

	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		count, err := rangeCount(restURIOf(compose, nodeIdx), className, propName,
			tailSentinelBase, tailSentinelBase+tailWriteCount-1)
		require.NoError(t, err, "post-migration tail-range query on node %d failed", nodeIdx)
		require.Equal(t, tailWriteCount, count,
			"node %d: expected all %d writes injected during the migration to be present in the "+
				"rangeable index; a lower count reproduces weaviate/0-weaviate-issues#319 (rangeable "+
				"instance) - a write in the post-swap pre-flip window silently missing the rangeable bucket",
			nodeIdx, tailWriteCount)
	}
}

// postSingleNumericObject creates one object with the given score via a
// direct (non-batch) POST, consistency_level=ALL so the write is
// replicated before this call returns. Returns false (without failing the
// test) on a non-2xx response, since an occasional transient failure
// during an in-flight migration (e.g. a node briefly unavailable) should
// shrink the expected count rather than fail the test outright - the
// assertion is "every ATTEMPTED write that succeeded is present", not
// "every tick produces a write".
func postSingleNumericObject(t *testing.T, restURI, className string, score int) bool {
	t.Helper()
	obj := map[string]interface{}{
		"class": className,
		"id":    uuid.New().String(),
		"properties": map[string]interface{}{
			"name":  fmt.Sprintf("tail-%d", score),
			"score": score,
		},
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return false
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/objects?consistency_level=ALL", restURI),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Logf("tail write for score %d failed (status %d): %s", score, resp.StatusCode, string(respBody))
		return false
	}
	return true
}
