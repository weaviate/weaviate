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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_EnableRangeable_ConcurrentUpdatesNoLossNoPanic is the 3-node
// RF3 port of QA's donated f10 repro for weaviate/weaviate#11688 (fixed by
// this PR). Object UPDATES that race the enable-rangeable per-shard swap under
// replication caused two distinct failures on a build without the fix:
//
//  1. nil-deref panics in the reindex double-write DELETE callback
//     (adapters/repos/db/inverted_reindex_strategy_rangeable.go MakeDeleteCallback
//     → resolveScopedDoubleWriteBucket → Bucket.Strategy on a nil bucket that
//     is being swapped out mid-write), surfaced to clients as a REST
//     "panic serving" line and a swallowed empty-200 PATCH response, and
//  2. durable, silent rangeable write-loss: a PATCH acked 204, its new value
//     landed in the objects (source-of-truth) bucket, but the concurrent swap
//     discarded the double-write into the rangeable index, so a range query
//     over the updated band came up short — and stayed short across a restart
//     because the on-disk rangeable segments were missing the entries.
//
// The migrated property (dateInt) starts with IndexRangeFilters=false, so the
// rangeable index only exists after the runtime migration. Once the migration
// finishes and the rangeable index is ready, a range query over the moved band
// is served from that index alone, so a short count is a rangeable write-loss.
//
// The test PATCHes every mover into a sentinel band far above every initial
// value while the migration runs, then asserts, on every replica and again
// after a rolling restart: zero "panic serving" lines, and a rangeable count
// over the sentinel band that equals the mover count (no lost update). A
// pre-check confirms the movers' new values are durably present in the objects
// store, so any index shortfall is a silent index loss, not a failed write.
func TestMultiNode_EnableRangeable_ConcurrentUpdatesNoLossNoPanic(t *testing.T) {
	ctx := context.Background()

	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className = "EnableRangeableConcurrentUpdates"
		propName  = "dateInt"
		// Enough objects that the per-shard backfill+swap window is wide
		// enough for the PATCH storm to overlap it, small enough to stay well
		// under the 5-minute target on a 3-node RF3 cluster.
		totalObjects = 12_000
		// Every moverStride-th object is a mover: 1_500 concurrently-updated
		// objects, plenty of writes to hit the swap window on every shard.
		moverStride = 8
		// sentinelBase sits far above every initial dateInt (initial values are
		// the seq itself, < totalObjects), so a range query dateInt >=
		// sentinelBase counts exactly the movers.
		sentinelBase  = 8_000_000
		updateThreads = 8
	)

	trueVal, falseVal := true, false
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:              "seq",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
		{
			// The migrated property. IndexFilterable=true matches the reported
			// repro and the working multi-node enable-rangeable journey. Once
			// the migration finishes and the rangeable index is ready, range
			// queries are served from the rangeable index only (the
			// filterable bucket-walk fallback applies only while the rangeable
			// bucket is still being built), so a short count over the sentinel
			// band after FINISHED is an unambiguous rangeable write-loss.
			Name:              propName,
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
	})
	// Closure, not a direct defer: the rolling restart below re-maps node
	// ports, so the URI must be resolved at cleanup time, not now.
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportSeqDateInt(t, restURIOf(compose, 1), className, totalObjects)

	movers := make([]int, 0, totalObjects/moverStride)
	for s := 0; s < totalObjects; s += moverStride {
		movers = append(movers, s)
	}
	sentinelHi := sentinelBase + 10*totalObjects // wide upper bound; only movers land in the band

	// Submit enable-rangeable, then fire the PATCH storm so it overlaps the
	// whole migration window (backfill → per-shard swap → schema flip).
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURIOf(compose, 1), className, propName, "rangeFilters", `{}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)

	// stormCtx bounds the storm even if the test goroutine aborts (e.g. the
	// migration times out), so the update goroutines can never outlive the run.
	stormCtx, cancelStorm := context.WithTimeout(ctx, 240*time.Second)
	defer cancelStorm()

	var (
		patchOK   atomic.Int64
		patchErr  atomic.Int64
		stormWG   sync.WaitGroup
		logger    = logrus.New()
		restURINd = restURIOf(compose, 1)
	)
	for th := 0; th < updateThreads; th++ {
		chunk := make([]int, 0, len(movers)/updateThreads+1)
		for i := th; i < len(movers); i += updateThreads {
			chunk = append(chunk, movers[i])
		}
		stormWG.Add(1)
		enterrors.GoWrapper(func() {
			defer stormWG.Done()
			// Alternate the target value every pass so each PATCH is a real
			// value change (delete-old + add-new), maximising the number of
			// double-write DELETE callbacks that hit the swap window.
			for round := 0; stormCtx.Err() == nil; round++ {
				band := sentinelBase
				if round%2 == 1 {
					band = sentinelBase + totalObjects
				}
				for _, s := range chunk {
					if stormCtx.Err() != nil {
						break
					}
					if err := patchDateInt(restURINd, className, detUUID(className, s), band+s); err != nil {
						patchErr.Add(1)
						continue
					}
					patchOK.Add(1)
				}
			}
		}, logger)
	}

	reindexhelpers.AwaitReindexFinished(t, restURINd, taskID, reindexhelpers.WithTimeout(180*time.Second))
	cancelStorm()
	stormWG.Wait()
	t.Logf("update storm during migration: ok=%d transient_err=%d", patchOK.Load(), patchErr.Load())

	// Authoritative post-swap pass: PATCH every mover to a single known
	// in-band value and require 204. This guarantees the objects store holds
	// the new value for every mover, so a later index shortfall is provably a
	// silent index loss and not a dropped write. It is also a real change from
	// whatever the storm left, so it re-exercises the write path once more.
	for _, s := range movers {
		var lastErr error
		for attempt := 0; attempt < 5; attempt++ {
			if lastErr = patchDateInt(restURINd, className, detUUID(className, s), sentinelBase+s); lastErr == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		require.NoErrorf(t, lastErr, "authoritative PATCH of mover seq=%d must succeed", s)
	}

	// Schema flip must have committed on every node.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		cls := getClassFromNode(t, restURIOf(compose, nodeIdx), className)
		var flipped bool
		for _, prop := range cls.Properties {
			if prop.Name == propName && prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				flipped = true
				break
			}
		}
		assert.Truef(t, flipped, "node %d: IndexRangeFilters must be true after migration", nodeIdx)
	}

	// Every mover's new value must be durable in the objects store. If this
	// holds but the range count below is short, the loss is in the index.
	assertMoversDurableInObjectStore(t, restURINd, className, movers, sentinelBase)

	// No handler panicked during the storm (the nil-bucket DELETE-callback
	// deref surfaced as a "panic serving" REST line).
	assert.Equalf(t, 0, scanPanicLines(ctx, t, compose),
		"expected zero 'panic serving' lines across all nodes during concurrent-update enable-rangeable")

	// Core assertion, pre-restart: every replica serves all movers over the
	// sentinel band. A short count is a #11688 silent rangeable write-loss.
	assertRangeCountAllNodes(t, compose, className, propName, sentinelBase, sentinelHi, len(movers), "pre-restart")

	// Durability: the loss (if any) survives a restart because it is on disk.
	// A clean cluster must still serve every mover after a rolling restart.
	rollingRestartCluster(ctx, t, compose)
	assertRangeCountAllNodes(t, compose, className, propName, sentinelBase, sentinelHi, len(movers), "post-restart")

	// Re-scan: a panic could also fire during post-restart shard init/replay.
	assert.Equalf(t, 0, scanPanicLines(ctx, t, compose),
		"expected zero 'panic serving' lines across all nodes after rolling restart")
}

// detUUID returns a deterministic UUID for a seq so the storm and the
// verification pass address the exact objects that were imported.
func detUUID(className string, seq int) string {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf("%s-%d", className, seq))).String()
}

// batchImportSeqDateInt imports totalObjects with deterministic IDs, seq=i and
// an initial dateInt=i (< sentinelBase). consistency_level=ALL leaves the
// cluster fully replicated before the migration starts.
func batchImportSeqDateInt(t *testing.T, restURI, className string, total int) {
	t.Helper()
	const batchSize = 1_000
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		objects := make([]map[string]interface{}, 0, end-start)
		for i := start; i < end; i++ {
			objects = append(objects, map[string]interface{}{
				"class": className,
				"id":    detUUID(className, i),
				"properties": map[string]interface{}{
					"seq":     i,
					"dateInt": i,
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects})
		require.NoError(t, err)
		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equalf(t, http.StatusOK, resp.StatusCode, "batch %d-%d failed: %s", start, end, string(respBody))

		var batchResp []struct {
			Result struct {
				Errors *struct {
					Error []struct {
						Message string `json:"message"`
					} `json:"error"`
				} `json:"errors,omitempty"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(respBody, &batchResp))
		for i, br := range batchResp {
			if br.Result.Errors != nil && len(br.Result.Errors.Error) > 0 {
				t.Fatalf("batch %d-%d object %d errored: %s", start, end, i, br.Result.Errors.Error[0].Message)
			}
		}
	}
}

// patchDateInt PATCHes one object's dateInt via REST. Raw HTTP keeps it
// goroutine-safe (no testify off the test goroutine). A strict 204 doubles as a
// panic probe: the REST panic middleware recovers without writing a body, so a
// panicked handler surfaces as an empty 200 rather than the expected 204.
func patchDateInt(restURI, className, id string, value int) error {
	body, err := json.Marshal(map[string]interface{}{
		"class":      className,
		"id":         id,
		"properties": map[string]interface{}{"dateInt": value},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPatch,
		fmt.Sprintf("http://%s/v1/objects/%s/%s", restURI, className, id), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d (want 204): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// assertMoversDurableInObjectStore spot-checks that sampled movers hold a
// dateInt >= sentinelBase in the objects (source-of-truth) store, proving the
// storm's writes are durable there before the index count is asserted.
func assertMoversDurableInObjectStore(t *testing.T, restURI, className string, movers []int, sentinelBase int) {
	t.Helper()
	const sampleStride = 97 // coprime-ish stride so the sample spreads across the keyspace
	for i := 0; i < len(movers); i += sampleStride {
		s := movers[i]
		val, ok := fetchObjectDateInt(restURI, className, detUUID(className, s))
		require.Truef(t, ok, "mover seq=%d must be fetchable from the objects store", s)
		require.GreaterOrEqualf(t, val, sentinelBase,
			"mover seq=%d objects-store dateInt=%d must be in the sentinel band (write must be durable)", s, val)
	}
}

// fetchObjectDateInt GETs a single object and returns its dateInt.
func fetchObjectDateInt(restURI, className, id string) (int, bool) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/objects/%s/%s", restURI, className, id))
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	var obj struct {
		Properties struct {
			DateInt float64 `json:"dateInt"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		return 0, false
	}
	return int(obj.Properties.DateInt), true
}

// assertRangeCountAllNodes requires that every replica serves exactly want
// objects over [lo, hi] on propName within the convergence window. A durable
// loss never converges and fails the poll; a transient replication lag
// converges quickly.
func assertRangeCountAllNodes(t *testing.T, compose *docker.DockerCompose, className, propName string, lo, hi, want int, phase string) {
	t.Helper()
	var last [3]int
	ok := assert.Eventuallyf(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			count, err := rangeCount(restURIOf(compose, nodeIdx), className, propName, lo, hi)
			if err != nil {
				return false
			}
			last[nodeIdx-1] = count
			if count != want {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"%s: every replica must serve all %d updated objects over the sentinel band", phase, want)
	if !ok {
		t.Errorf("%s rangeable counts per node = %v, want %d on every node (silent rangeable write-loss)", phase, last, want)
	}
}

// scanPanicLines counts "panic serving" lines across every node's container
// logs. docker restart preserves logs, so a single end-of-test scan captures
// panics from both the storm and any post-restart replay.
func scanPanicLines(ctx context.Context, t *testing.T, compose *docker.DockerCompose) int {
	t.Helper()
	total := 0
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		if node == nil {
			continue
		}
		reader, err := node.Container().Logs(ctx)
		if err != nil {
			t.Logf("panic scan: failed to read logs for node %d: %v", i, err)
			continue
		}
		logs, _ := io.ReadAll(reader)
		reader.Close()
		for _, line := range strings.Split(string(logs), "\n") {
			if strings.Contains(line, "panic serving") {
				total++
				t.Logf("panic scan: node %d: %s", i, line)
			}
		}
	}
	return total
}
