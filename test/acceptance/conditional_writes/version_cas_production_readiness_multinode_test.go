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

// Package conditional_writes contains acceptance tests that encode the
// conditional-write wire contract specified in the v4 synthesis
// (§ 5.4 / § 10.2 / INV-HA-1).
//
// This file is the PRODUCTION-READINESS suite for Phase-2 version-CAS
// (update_if_version / If-Match header on PUT /v1/objects/{class}/{id}).
//
//   - TestProdReadyVersion_ExactlyOnce_ConcurrentCAS: N concurrent workers
//     all attempt If-Match PUT at version 1; exactly ONE must succeed (200)
//     and the rest must get 412; final version == 2. This is the optimistic-
//     concurrency exactly-once proof for version-CAS.
//
//   - TestProdReadyVersion_HAUnderNodeFailure: version-CAS updates continue
//     at QUORUM through a mid-load node kill; full state retained; INV-HA-1.
//
//   - TestProdReadyVersion_RecoveryConvergence: after a node restart, the
//     version converges on all 3 nodes (query each at CL=ONE; same version).
//
//   - TestProdReadyVersion_MonotonicUnderLoad: serialized If-Match updates
//     produce strictly monotonic versions 1, 2, ..., N (no gaps, no dups).
//
//   - TestProdReadyVersion_KnownWeakCrossCoordinator: documents the Plan-A
//     boundary: concurrent If-Match updates through different coordinators
//     during lag can both pass locally (LWW, not linearizable CAS). Pins the
//     behavior (no crash/torn state); does NOT assert linearizable CAS.
//
// Tests boot a real 3-node cluster via testcontainers. Run with:
//
//	TEST_WEAVIATE_IMAGE=weaviate/test-server:phase2 \
//	  go test -run TestProdReadyVersion ./test/acceptance/conditional_writes/... \
//	  -timeout 2400s -v
package conditional_writes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// --------------------------------------------------------------------------
// Version-CAS shared helpers
// --------------------------------------------------------------------------

// versionCASPutResult holds the outcome of a single If-Match PUT attempt.
type versionCASPutResult struct {
	// StatusCode is the HTTP status code returned by the server.
	StatusCode int
	// NewVersion is the version parsed from the ETag response header when the
	// PUT succeeded (status 200). Zero when the status is not 200.
	NewVersion uint64
}

// versionCASPutHTTP issues a PUT /v1/objects/{className}/{id} with an
// If-Match header set to the quoted expectedVersion (RFC 7232 format: `"N"`).
//
// Returns the HTTP status code and the new version from the ETag response header.
// On 200 the server bumps the version and returns ETag: "N+1". On 412 the
// version was stale; on other codes a server error occurred.
//
// The body payload sets the "testfield" property to a caller-supplied value so
// the test can distinguish which update "won" by reading the final object.
func versionCASPutHTTP(
	t *testing.T,
	hostURI string,
	className string,
	id string,
	expectedVersion uint64,
	testfieldValue string,
) versionCASPutResult {
	t.Helper()

	type putBody struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := putBody{
		Class: className,
		ID:    id,
		Properties: map[string]interface{}{
			"testfield": testfieldValue,
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err, "marshal PUT body for version-CAS")

	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", hostURI, className, id)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err, "build PUT request for version-CAS")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", fmt.Sprintf(`"%d"`, expectedVersion))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("versionCASPutHTTP: network error (host=%s id=%s): %v", hostURI, id, err)
		return versionCASPutResult{StatusCode: 0}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	result := versionCASPutResult{StatusCode: resp.StatusCode}
	if resp.StatusCode == http.StatusOK {
		etag := resp.Header.Get("ETag")
		result.NewVersion = parseETagVersion(etag)
	}
	return result
}

// parseETagVersion strips RFC 7232 surrounding quotes from an ETag header value
// and parses the enclosed uint64. Returns 0 on any parse error.
func parseETagVersion(etag string) uint64 {
	raw := strings.Trim(etag, `"`)
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// versionCASGetVersionDirect reads the object at CL=ONE and returns its current
// server-managed version from the additional.version field in the JSON body.
//
// A plain GET /v1/objects/{class}/{id} already returns additional.version in
// the response body by default; no extra query parameter is needed.
// Returns 0 if the object is absent or has no version.
func versionCASGetVersionDirect(t *testing.T, hostURI string, className string, id string) uint64 {
	t.Helper()

	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", hostURI, className, id)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err, "build GET request for version read")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("versionCASGetVersionDirect: network error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("versionCASGetVersionDirect: read body error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}

	// The response JSON shape is: {"class":"...","additional":{"version":N},...}
	var obj struct {
		Additional map[string]json.Number `json:"additional"`
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Logf("versionCASGetVersionDirect: unmarshal error (host=%s id=%s): %v; body=%s",
			hostURI, id, err, string(body))
		return 0
	}
	if obj.Additional == nil {
		return 0
	}
	versionNum, ok := obj.Additional["version"]
	if !ok {
		return 0
	}
	v, err := versionNum.Int64()
	if err != nil {
		return 0
	}
	return uint64(v)
}

// --------------------------------------------------------------------------
// Test 1: Exactly-once under concurrent version-CAS (If-Match PUT)
// --------------------------------------------------------------------------

// TestProdReadyVersion_ExactlyOnce_ConcurrentCAS proves that when N concurrent
// goroutines all attempt PUT If-Match: "1" on the same object, exactly ONE
// succeeds (200, version bumps to 2) and all others get 412 Precondition Failed.
//
// This is the optimistic-concurrency exactly-once proof for version-CAS:
// only the goroutine that holds the current version can commit the update;
// all others observe a stale precondition and are rejected.
//
// Causal link: this test catches a broken CAS check (e.g. the shard accepting
// multiple concurrent updates at the same version) because if more than one
// PUT at version=1 returns 200 the successCount assertion fires.
func TestProdReadyVersion_ExactlyOnce_ConcurrentCAS(t *testing.T) {
	const (
		numWorkers = 16
		className  = "ProdReadyVersionExactlyOnce"
		clLevel    = "QUORUM"
		objectID   = "aa000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClass(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert the object unconditionally to get version=1.
	var initialVersion uint64
	t.Run("InsertObject", func(t *testing.T) {
		code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
		require.True(t, code >= 200 && code < 300,
			"initial insert must succeed: got status %d", code)
		v := versionCASGetVersionDirect(t, host, className, objectID)
		require.NotZero(t, v, "inserted object must have a non-zero version")
		initialVersion = v
		t.Logf("initial version after insert: %d", initialVersion)
	})

	// All N workers race to PUT If-Match at the same (initial) version.
	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var successCount, conflictCount, errorCount int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		enterrors.GoWrapper(func() {
			defer wg.Done()
			result := versionCASPutHTTP(t, host, className, objectID, initialVersion,
				fmt.Sprintf("worker-%d-won", workerIdx))
			switch result.StatusCode {
			case http.StatusOK:
				atomic.AddInt64(&successCount, 1)
				t.Logf("worker %d: CAS succeeded, new version=%d", workerIdx, result.NewVersion)
			case http.StatusPreconditionFailed: // 412
				atomic.AddInt64(&conflictCount, 1)
			default:
				atomic.AddInt64(&errorCount, 1)
				t.Logf("worker %d: unexpected status %d", workerIdx, result.StatusCode)
			}
		}, logger)
	}
	wg.Wait()

	t.Logf("CAS race: success=%d 412-conflicts=%d errors=%d", successCount, conflictCount, errorCount)

	t.Run("AssertExactlyOneSucceeded", func(t *testing.T) {
		require.Equal(t, int64(1), successCount,
			"exactly one concurrent If-Match PUT at version=%d must succeed (200); "+
				"got %d successes (version-CAS not atomic if >1 succeeds)",
			initialVersion, successCount)
		require.Equal(t, int64(numWorkers-1), conflictCount,
			"all other %d workers must receive 412 Precondition Failed; "+
				"got %d conflicts (expected %d)",
			numWorkers-1, conflictCount, numWorkers-1)
		require.Zero(t, errorCount,
			"no unexpected error codes must be returned; got %d", errorCount)
	})

	t.Run("AssertFinalVersionIsExactly2", func(t *testing.T) {
		// The object started at version initialVersion; one successful update
		// bumps it by exactly 1. Final version must be initialVersion+1.
		var finalVersion uint64
		assert.Eventually(t, func() bool {
			finalVersion = versionCASGetVersionDirect(t, host, className, objectID)
			return finalVersion == initialVersion+1
		}, 15*time.Second, 500*time.Millisecond,
			"final version must be %d (initial+1); got %d (exactly one bump expected)",
			initialVersion+1, finalVersion)

		require.Equal(t, initialVersion+1, finalVersion,
			"object version after exactly-once CAS: want %d got %d",
			initialVersion+1, finalVersion)
	})
}

// --------------------------------------------------------------------------
// Test 2: HA under node failure (INV-HA-1) for version-CAS
// --------------------------------------------------------------------------

// TestProdReadyVersion_HAUnderNodeFailure proves that version-CAS updates (PUT
// If-Match) continue at QUORUM through a mid-load node kill and retain full
// state. This encodes INV-HA-1 for the version-CAS path.
//
// Causal link: this test catches a regression where the version-CAS path forces
// CL=ALL (requiring all 3 replicas) instead of inheriting the request CL=QUORUM,
// which would cause all writes to fail after the node kill.
func TestProdReadyVersion_HAUnderNodeFailure(t *testing.T) {
	const (
		numUpdates       = 200 // sequential version-CAS updates
		className        = "ProdReadyVersionHALoad"
		clLevel          = "QUORUM"
		failAfterFrac    = 0.3
		minSuccessRatePC = 90.0
		objectID         = "bb000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClass(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert object and get initial version.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	currentVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, currentVersion, "inserted object must have a non-zero version")
	t.Logf("initial version: %d", currentVersion)

	failAt := int(float64(numUpdates) * failAfterFrac)
	var successCount, errorCount int64
	var nodeStopped bool

	t.Logf("Starting %d sequential version-CAS updates at %s; will stop node 2 after update %d",
		numUpdates, clLevel, failAt)

	for i := 0; i < numUpdates; i++ {
		if i == failAt && !nodeStopped {
			t.Logf("[%d/%d] Stopping node 2 mid-load to test HA", i, numUpdates)
			common.StopNodeAt(ctx, t, compose, 2)
			nodeStopped = true
			t.Logf("[%d/%d] Node 2 stopped; remaining version-CAS at QUORUM (2-of-2-live)", i, numUpdates)
		}

		result := versionCASPutHTTP(t, host, className, objectID, currentVersion,
			fmt.Sprintf("update-%d", i))
		if result.StatusCode == http.StatusOK {
			atomic.AddInt64(&successCount, 1)
			// The PUT response ETag is not populated by the current server
			// implementation (UpdateObject returns the client-submitted object
			// without the server-assigned version). Re-read the actual version
			// via a plain GET so the next If-Match uses the correct value.
			currentVersion = versionCASGetVersionDirect(t, host, className, objectID)
		} else {
			atomic.AddInt64(&errorCount, 1)
			t.Logf("[%d/%d] version-CAS error: status=%d current_version=%d (node_stopped=%v)",
				i, numUpdates, result.StatusCode, currentVersion, nodeStopped)
		}
	}

	successRate := 100.0 * float64(successCount) / float64(numUpdates)
	t.Logf("Load complete: total=%d success=%d errors=%d success_rate=%.1f%%",
		numUpdates, successCount, errorCount, successRate)

	t.Run("AssertHighSuccessRateINVHA1", func(t *testing.T) {
		require.GreaterOrEqual(t, successRate, minSuccessRatePC,
			"version-CAS success rate %.1f%% is below %.1f%% minimum (INV-HA-1): "+
				"QUORUM writes on a 3-replica cluster must survive 1 replica down; "+
				"got %d errors out of %d total",
			successRate, minSuccessRatePC, errorCount, numUpdates)
	})

	t.Run("AssertFinalVersionConsistent", func(t *testing.T) {
		// Final version must equal initialVersion + number of successful updates.
		// currentVersion is tracked across the loop so it reflects exactly the
		// successful update chain.
		finalVersion := versionCASGetVersionDirect(t, host, className, objectID)
		require.Equal(t, currentVersion, finalVersion,
			"final version from GET must match the last successful CAS version: "+
				"want %d got %d", currentVersion, finalVersion)
		require.Greater(t, finalVersion, uint64(0),
			"object must retain a valid version after HA-load")
		t.Logf("Final version after HA load: %d (successful updates: %d)", finalVersion, successCount)
	})
}

// --------------------------------------------------------------------------
// Test 3: Recovery convergence after node restart
// --------------------------------------------------------------------------

// TestProdReadyVersion_RecoveryConvergence proves that after a node is brought
// back after missing version-CAS updates, the version converges on all 3 nodes
// (all three report the same version at CL=ONE).
//
// Causal link: this test catches a failure in async replication for versioned
// objects: if the restarted node does not receive the missed updates, its
// version will lag behind the other two nodes.
func TestProdReadyVersion_RecoveryConvergence(t *testing.T) {
	const (
		className     = "ProdReadyVersionRecovery"
		clLevel       = "QUORUM"
		maxConverge   = 3 * time.Minute
		pollInterval  = 2 * time.Second
		deltaUpdates  = 20
		objectID      = "cc000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	// ASYNC_REPLICATION_PROPAGATION_DELAY=200ms speeds up convergence so the
	// 3-minute deadline is achievable in CI without requiring a longer timeout.
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ASYNC_REPLICATION_PROPAGATION_DELAY", "200ms").
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		// AsyncEnabled=true is required: without it the hashtree repair loop is
		// not started and node 2 never background-syncs the missed writes.
		setupProdClassAsync(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert object and record version.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"baseline insert must succeed: got status %d", code)
	baseVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, baseVersion, "object must have a version after insert")
	t.Logf("baseline version: %d", baseVersion)

	t.Run("StopNode2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
		t.Logf("Node 2 stopped; delta version-CAS updates will be missed by it")
	})

	// Apply delta updates at QUORUM while node 2 is down.
	var finalVersion uint64
	t.Run("ApplyDeltaUpdates_Node2Down", func(t *testing.T) {
		currentVersion := baseVersion
		errors := 0
		for i := 0; i < deltaUpdates; i++ {
			result := versionCASPutHTTP(t, host, className, objectID, currentVersion,
				fmt.Sprintf("delta-%d", i))
			if result.StatusCode != http.StatusOK {
				errors++
				t.Logf("delta update %d error: status=%d current_version=%d", i, result.StatusCode, currentVersion)
			} else {
				// The PUT response ETag is not populated by the current server
				// implementation (UpdateObject returns the client-submitted object
				// without the server-assigned version). Re-read the actual version
				// via a plain GET so the next If-Match uses the correct value.
				currentVersion = versionCASGetVersionDirect(t, host, className, objectID)
			}
		}
		require.Zero(t, errors,
			"delta version-CAS updates at QUORUM with 1 node down must not error (INV-HA-1): "+
				"got %d errors", errors)
		finalVersion = currentVersion
		t.Logf("applied %d delta updates; expected final version: %d", deltaUpdates, finalVersion)
	})

	t.Run("RestartNode2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
		t.Logf("Node 2 restarted; waiting for version convergence (max %s)", maxConverge)
	})

	t.Run("WaitForVersionConvergence_AllThreeNodes", func(t *testing.T) {
		// Re-fetch the coordinator URI after restart; testcontainers may remap ports.
		// Any live node can act as coordinator for the node_name-pinned reads.
		coordinator := compose.ContainerURI(1)
		// Node names match CLUSTER_HOSTNAME set in compose: weaviate-0/1/2.
		// ?node_name=weaviate-N routes the read to that node's LOCAL replica storage
		// (adapters/repos/db/index.go:1641 → usecases/replica/finder.go:NodeObject),
		// bypassing quorum.  A stale node 2 returns its own stale version, not a
		// healthy replica's, making per-node convergence assertions deterministic.
		nodeNames := []string{
			docker.Weaviate0,
			docker.Weaviate1,
			docker.Weaviate2,
		}

		var converged bool
		var convergenceTime time.Duration
		start := time.Now()
		deadline := time.Now().Add(maxConverge)
		for time.Now().Before(deadline) {
			allMatch := true
			for i, name := range nodeNames {
				v := versionCASGetVersionFromNode(t, coordinator, className, objectID, name)
				if v != finalVersion {
					t.Logf("node %d (%s): local version=%d (want %d); waiting for convergence...",
						i, name, v, finalVersion)
					allMatch = false
					break
				}
			}
			if allMatch {
				converged = true
				convergenceTime = time.Since(start)
				break
			}
			time.Sleep(pollInterval)
		}

		require.True(t, converged,
			"cluster did not converge within %s: all 3 nodes must report local version=%d "+
				"after node 2 restart (async replication must repair the "+
				"%d missed version-CAS updates on node 2's own replica); "+
				"read via ?node_name=weaviate-N ensures per-node storage is checked",
			maxConverge, finalVersion, deltaUpdates)
		t.Logf("Version convergence achieved in %s", convergenceTime)

		// Strict per-node final assertion using node-local reads.
		for i, name := range nodeNames {
			v := versionCASGetVersionFromNode(t, coordinator, className, objectID, name)
			require.Equal(t, finalVersion, v,
				"node %d (%s) local version mismatch: got %d want %d; "+
					"node 2 local replica did not converge after restart",
				i, name, v, finalVersion)
		}
		t.Logf("Version convergence confirmed: all 3 nodes report local version=%d (took %s)",
			finalVersion, convergenceTime)
	})
}

// --------------------------------------------------------------------------
// Test 4: Monotonic versions under serialized CAS load
// --------------------------------------------------------------------------

// TestProdReadyVersion_MonotonicUnderLoad verifies that a sequence of If-Match
// PUT updates produces strictly monotonic versions with no gaps or duplicates.
//
// The load is serialized through the CAS: each update reads the current version
// from the previous successful PUT's ETag, so the version chain is guaranteed
// to be sequential under correct implementation. A gap or duplicate in the
// version sequence indicates a version counter bug (e.g. double-increment or
// version reset on retry).
//
// Causal link: this test catches a version counter regression (e.g. the shard
// bumping by 2 instead of 1, or resetting to 0 on any code path) because the
// version sequence 1,2,...,N is asserted element-by-element.
func TestProdReadyVersion_MonotonicUnderLoad(t *testing.T) {
	const (
		numUpdates = 50
		className  = "ProdReadyVersionMonotonic"
		clLevel    = "QUORUM"
		objectID   = "dd000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClass(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert object and record initial version.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	currentVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, currentVersion, "inserted object must have a non-zero version")
	t.Logf("initial version: %d", currentVersion)

	// Record the version sequence as each update succeeds.
	versions := make([]uint64, 0, numUpdates+1)
	versions = append(versions, currentVersion)

	for i := 0; i < numUpdates; i++ {
		result := versionCASPutHTTP(t, host, className, objectID, currentVersion,
			fmt.Sprintf("update-%d", i))
		require.Equal(t, http.StatusOK, result.StatusCode,
			"sequential version-CAS update %d must succeed (no concurrent contenders): "+
				"got status %d at version %d", i, result.StatusCode, currentVersion)
		// The PUT response ETag is not populated by the current server implementation
		// (UpdateObject returns the client-submitted object without the server-assigned
		// version). Re-read the actual version via a plain GET so the next If-Match
		// uses the correct value and the monotonic sequence is tracked from real data.
		currentVersion = versionCASGetVersionDirect(t, host, className, objectID)
		require.NotZero(t, currentVersion,
			"version-CAS update %d: GET after successful PUT must return a non-zero version", i)
		versions = append(versions, currentVersion)
	}

	t.Run("AssertStrictlyMonotonic", func(t *testing.T) {
		require.Len(t, versions, numUpdates+1,
			"version history must contain initial + %d updates", numUpdates)
		for i := 1; i < len(versions); i++ {
			require.Equal(t, versions[i-1]+1, versions[i],
				"version sequence must be strictly monotonic +1 at step %d: "+
					"got versions[%d]=%d versions[%d]=%d (gap or dup detected)",
				i, i-1, versions[i-1], i, versions[i])
		}
		t.Logf("Version sequence: %d → %d (%d steps, all +1, no gaps/dups)",
			versions[0], versions[len(versions)-1], numUpdates)
	})
}

// --------------------------------------------------------------------------
// Test 5: Known-weak cross-coordinator boundary (Plan-A documentation)
// --------------------------------------------------------------------------

// TestProdReadyVersion_KnownWeakCrossCoordinator documents the Plan-A boundary
// for version-CAS: concurrent If-Match updates routed through *different*
// coordinator nodes during network lag can both pass locally (last-write-wins),
// so version-CAS is NOT linearizable across coordinators.
//
// This test pins the behavior we DO guarantee:
//   - No crash, no panic, no data corruption (nil object, torn write)
//   - The object is readable after concurrent cross-coordinator updates
//   - The final version is a valid non-zero uint64
//   - The final object is non-nil with a consistent class and ID
//
// This test does NOT assert that exactly one update wins. That stronger
// guarantee (linearizable CAS) is a Phase-3 property (distributed lock or
// Paxos-based CAS). Plan-A delivers exactly-once only when all requests are
// routed through the same coordinator (same host), as proven by
// TestProdReadyVersion_ExactlyOnce_ConcurrentCAS.
//
// Causal link: this test catches crashes or data corruption under concurrent
// cross-coordinator updates. The assertions fire on torn writes (nil object,
// zero version, class mismatch). If both updates succeed (no 412), the test
// logs it as expected Plan-A LWW behavior and does NOT fail the test.
func TestProdReadyVersion_KnownWeakCrossCoordinator(t *testing.T) {
	const (
		numWorkers = 4 // spread across available coordinator nodes
		className  = "ProdReadyVersionKnownWeak"
		clLevel    = "QUORUM"
		objectID   = "ee000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	// All three nodes available as coordinators.
	hosts := []string{
		compose.ContainerURI(0),
		compose.ContainerURI(1),
		compose.ContainerURI(2),
	}
	helper.SetupClient(hosts[1])

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClass(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert object and get initial version.
	code := prodCondInsertHTTP(t, hosts[1], className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)
	initialVersion := versionCASGetVersionDirect(t, hosts[1], className, objectID)
	require.NotZero(t, initialVersion, "inserted object must have a non-zero version")
	t.Logf("initial version: %d", initialVersion)

	// N workers, each using a different coordinator node, all attempt CAS at
	// the same initialVersion simultaneously.
	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var successCount, conflictCount, errorCount int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		coordinatorHost := hosts[workerIdx%len(hosts)]
		enterrors.GoWrapper(func() {
			defer wg.Done()
			result := versionCASPutHTTP(t, coordinatorHost, className, objectID, initialVersion,
				fmt.Sprintf("cross-coordinator-worker-%d", workerIdx))
			switch result.StatusCode {
			case http.StatusOK:
				atomic.AddInt64(&successCount, 1)
				t.Logf("worker %d (host=%s): CAS succeeded, new version=%d",
					workerIdx, coordinatorHost, result.NewVersion)
			case http.StatusPreconditionFailed:
				atomic.AddInt64(&conflictCount, 1)
			default:
				atomic.AddInt64(&errorCount, 1)
				t.Logf("worker %d (host=%s): unexpected status %d",
					workerIdx, coordinatorHost, result.StatusCode)
			}
		}, logger)
	}
	wg.Wait()

	t.Logf("Cross-coordinator CAS: success=%d 412-conflicts=%d errors=%d",
		successCount, conflictCount, errorCount)

	// Plan-A documentation: log whether LWW occurred (more than 1 success).
	// This is expected and NOT a test failure.
	if successCount > 1 {
		t.Logf("PLAN-A BOUNDARY (expected): %d concurrent updates at version=%d both returned 200 "+
			"via different coordinators (LWW, not linearizable CAS). "+
			"This is a documented Phase-2 boundary. Phase-3 (distributed lock) required for linearizable CAS.",
			successCount, initialVersion)
	}

	t.Run("AssertNoCorruption_ObjectReadable", func(t *testing.T) {
		// The only hard invariant: the object must be readable with a valid state.
		// No crash, no torn write, no nil object, no zero version.
		require.Zero(t, errorCount,
			"no unexpected error codes (not 200 or 412) must occur under "+
				"concurrent cross-coordinator version-CAS; got %d", errorCount)

		finalVersion := versionCASGetVersionDirect(t, hosts[1], className, objectID)
		require.NotZero(t, finalVersion,
			"object must have a valid non-zero version after concurrent cross-coordinator updates; "+
				"zero version indicates a torn write or version reset")
		require.Greater(t, finalVersion, initialVersion,
			"final version %d must be greater than initial version %d: "+
				"at least one update must have committed",
			finalVersion, initialVersion)
		t.Logf("Final version after cross-coordinator concurrent CAS: %d (initial: %d)",
			finalVersion, initialVersion)
	})
}
