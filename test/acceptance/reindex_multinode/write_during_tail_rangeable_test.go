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
// weaviate/0-weaviate-issues#319 (rangeable instance): deferring the
// cluster-wide schema flip to OnTaskCompleted (weaviate/weaviate#12189)
// reopens a per-shard write-loss window between that shard's local swap
// and the cluster flip, since double-write coverage is torn down at
// swap. Writes continuously through the migration (rather than a single
// well-timed shot) so it doesn't depend on catching one exact instant.
func TestMultiNode_EnableRangeable_WriteDuringTailLandsInRangeableIndex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RangeableWriteDuringTail"
	const propName = "score"
	// Large enough that the COMPLETED+IN_PROGRESS overlap window is
	// measured in seconds, giving the writer below room to land in it.
	const totalObjects = 50_000
	// Disjoint from the main import's [0, totalObjects) range, so a range
	// query over the band counts only the writes this test injected.
	const tailSentinelBase = 9_000_000

	defer createPreMigrationScoreCollection(t, compose, className, propName, totalObjects)()

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, propName,
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)

	// Fires every 100ms against node 1; a single node is enough since the
	// object routes by ID hash across all 3 shards. Runs until stopCh closes.
	//
	// Score is derived from a monotonic ATTEMPT counter rather than the
	// success counter: a request this test observes as failed can still
	// have committed on a quorum under partial-replication semantics, so
	// reusing its score for a retried object would let two distinct
	// objects share one sentinel score and desync the range-count
	// assertion below.
	var (
		attempted atomic.Int64
		written   atomic.Int64
		stopCh    = make(chan struct{})
		stopOnce  sync.Once
		writerWg  sync.WaitGroup
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
			n := attempted.Add(1) - 1
			score := tailSentinelBase + int(n)
			if postSingleNumericObject(t, restURIOf(compose, 1), className, score) {
				written.Add(1)
			}
		}
	}()
	// LIFO: fires before the cluster-teardown defers, so an early exit
	// doesn't leave the writer running against a tearing-down cluster.
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
	tailAttemptCount := int(attempted.Load())
	// Scores are attempt-numbered, so the band must cover every attempt,
	// not just the successful ones.
	tailScanUpperBound := tailSentinelBase + tailAttemptCount - 1
	t.Logf("injected %d successful tail writes (of %d attempts) with sentinel scores in [%d, %d]",
		tailWriteCount, tailAttemptCount, tailSentinelBase, tailScanUpperBound)
	require.Greater(t, tailWriteCount, 0,
		"the continuous writer should have produced at least one successful write during the migration")

	// A write silently missing from the rangeable bucket (#319) shows up
	// as a persistent short count here, not a transient one.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			count, err := rangeCount(restURIOf(compose, nodeIdx), className, propName,
				tailSentinelBase, tailScanUpperBound)
			if err != nil || count != tailWriteCount {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond,
		"all replicas should report every tail write once the migration is finished")

	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		count, err := rangeCount(restURIOf(compose, nodeIdx), className, propName,
			tailSentinelBase, tailScanUpperBound)
		require.NoError(t, err, "post-migration tail-range query on node %d failed", nodeIdx)
		require.Equal(t, tailWriteCount, count,
			"node %d: expected all %d writes injected during the migration to be present in the "+
				"rangeable index; a lower count reproduces weaviate/0-weaviate-issues#319 (rangeable "+
				"instance) - a write in the post-swap pre-flip window silently missing the rangeable bucket; "+
				"a higher count means two attempts collided on one sentinel score",
			nodeIdx, tailWriteCount)
	}
}

// postSingleNumericObject creates one object with the given score.
// Returns false (without failing the test) on a non-2xx response, since
// an occasional transient failure should shrink the expected count
// rather than fail the test outright.
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
