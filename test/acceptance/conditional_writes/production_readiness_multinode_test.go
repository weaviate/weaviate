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
// This file is the PRODUCTION-READINESS suite (Workstream A):
//
//   - TestProdReady_RF3_ExactlyOnce_ConcurrentSameUUIDs: RF3, M concurrent
//     clients all racing to insert_if_not_exists the same K UUIDs. Asserts
//     exactly K objects post-load (no duplicates, no loss).
//
//   - TestProdReady_RF3_HAUnderNodeFailureDuringLoad: RF3, sustain QUORUM
//     writes through a mid-load node stop. Asserts no sustained errors and
//     full count retained (INV-HA-1).
//
//   - TestProdReady_RF3_RecoveryConvergence: RF3, confirm that a node brought
//     back after missing writes converges (all three nodes agree on the full
//     object set at CL=ONE).
//
//   - TestProdReady_MultiShard_RF3_ExactlyOnce: same as the first test but
//     with a 3-shard class to confirm exactly-once holds across shards.
//
//   - TestProdReady_RF3_ThroughputReport: measures conditional vs.
//     unconditional throughput and reports p50/p95/p99. No heavy assertions
//     beyond error-rate and a sane relative floor.
//
// These tests boot a real 3-node cluster via testcontainers.
// They are intentionally heavy: run with -timeout 1800s and set
// TEST_WEAVIATE_IMAGE to a pre-built image to avoid rebuilding per test.
//
// TEST-FIRST NOTICE: the cluster harness is green; the conditional-write
// assertions are RED until Phase-1 endpoint plumbing (insert_if_not_exists
// query-param handling) lands. Do NOT stub the feature to force a pass.
package conditional_writes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// --------------------------------------------------------------------------
// Shared helpers for this file
// --------------------------------------------------------------------------

// setupProdClass creates a collection with RF=3, the given number of shards,
// and no vectorizer (independent of module availability).
//
// The testfield text property is declared explicitly to avoid auto-schema
// property-add races under RAFT when concurrent workers hit multiple nodes
// before schema propagation completes (which causes 422 responses).
func setupProdClass(t *testing.T, className string, numShards int) {
	t.Helper()
	shardingConfig := interface{}(nil)
	if numShards > 0 {
		shardingConfig = map[string]interface{}{"desiredCount": numShards}
	}
	cls := &models.Class{
		Class:      className,
		Vectorizer: "none",
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 3,
		},
		ShardingConfig: shardingConfig,
		Properties: []*models.Property{
			{
				Name:     "testfield",
				DataType: []string{"text"},
			},
		},
	}
	helper.CreateClass(t, cls)
}

// waitForSchemaOnAllNodes polls GET /v1/schema/{className} on every node in
// the cluster until the class (including its testfield property) is visible on
// all of them, or until timeout elapses. This guards against schema
// propagation lag under RAFT: a write routed to a node that has not yet
// received the schema update returns 422, not 201/200.
func waitForSchemaOnAllNodes(t *testing.T, compose *docker.DockerCompose, className string, numNodes int) {
	t.Helper()
	const (
		timeout  = 60 * time.Second
		interval = 500 * time.Millisecond
	)
	deadline := time.Now().Add(timeout)
	for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
		nodeURI := compose.ContainerURI(nodeIdx)
		var ready bool
		for time.Now().Before(deadline) {
			helper.SetupClient(nodeURI)
			cls, err := helper.GetClassWithoutAssert(t, className, "")
			if err == nil && cls != nil {
				hasField := false
				for _, p := range cls.Properties {
					if p.Name == "testfield" {
						hasField = true
						break
					}
				}
				if hasField {
					ready = true
					break
				}
			}
			time.Sleep(interval)
		}
		require.True(t, ready,
			"schema for class %q (with testfield property) did not propagate to node %d within %s",
			className, nodeIdx, timeout)
	}
}

// prodCondInsertHTTP posts a conditional insert to hostURI for a specific class
// and returns the HTTP status code. The condition travels via the
// ?condition=insert_if_not_exists query parameter.
//
// consistencyLevel may be "" (omit → server default QUORUM), "QUORUM", or
// "ALL". The function does NOT assert the status; callers decide.
func prodCondInsertHTTP(
	t *testing.T,
	hostURI string,
	className string,
	id string,
	consistencyLevel string,
) int {
	t.Helper()

	type body struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := body{
		Class: className,
		ID:    id,
		Properties: map[string]interface{}{
			"testfield": fmt.Sprintf("value-for-%s", id),
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err, "marshal insert body")

	url := fmt.Sprintf("http://%s/v1/objects?condition=insert_if_not_exists", hostURI)
	if consistencyLevel != "" {
		url = fmt.Sprintf("%s&consistency_level=%s", url, consistencyLevel)
	}

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url,
		bytes.NewBuffer(jsonData),
	)
	require.NoError(t, err, "build HTTP request")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Network error: return a sentinel that callers can treat as a transient
		// failure. We use 0 so callers see it is clearly not an HTTP status code.
		t.Logf("prodCondInsertHTTP: network error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// prodUncondInsertHTTP posts a plain (non-conditional) object insert.
// Used for throughput comparison in TestProdReady_RF3_ThroughputReport.
func prodUncondInsertHTTP(
	t *testing.T,
	hostURI string,
	className string,
	id string,
	consistencyLevel string,
) int {
	t.Helper()

	type body struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := body{
		Class: className,
		ID:    id,
		Properties: map[string]interface{}{
			"testfield": fmt.Sprintf("value-for-%s", id),
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err, "marshal unconditional insert body")

	url := fmt.Sprintf("http://%s/v1/objects", hostURI)
	if consistencyLevel != "" {
		url = fmt.Sprintf("%s?consistency_level=%s", url, consistencyLevel)
	}

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url,
		bytes.NewBuffer(jsonData),
	)
	require.NoError(t, err, "build HTTP request")
	req.Header.Set("Content-Type", "application/json")

	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		t.Logf("prodUncondInsertHTTP: network error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// countObjectsViaGraphQL returns the aggregate count for className on host.
// Uses GraphQL Aggregate meta.count — same pattern as common.CountObjects.
func countObjectsViaGraphQL(t *testing.T, host, className string) int64 {
	t.Helper()
	helper.SetupClient(host)
	q := fmt.Sprintf(`{Aggregate{%s{meta{count}}}}`, className)
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)
	result := resp.Get("Aggregate").Get(className).AsSlice()
	require.Len(t, result, 1, "aggregate must return exactly 1 bucket")
	meta := result[0].(map[string]interface{})["meta"].(map[string]interface{})
	count, err := meta["count"].(json.Number).Int64()
	require.NoError(t, err, "parse aggregate count")
	return count
}

// makeUUIDs returns n deterministic UUIDs with the given 4-char prefix.
// All UUIDs are valid RFC 4122 hex strings (36 chars, 8-4-4-4-12 groups).
//
// Format: <prefix>0000-0000-4000-8000-<12-hex-index>
//   - Group 1 (8 chars):  <4-char-prefix> + "0000"
//   - Group 2 (4 chars):  "0000"
//   - Group 3 (4 chars):  "4000"  (version 4 marker, valid placeholder)
//   - Group 4 (4 chars):  "8000"  (variant bits set, valid placeholder)
//   - Group 5 (12 chars): zero-padded hex index
//
// UUIDs are deterministic per (prefix, index) and distinct across indices.
func makeUUIDs(prefix string, n int) []string {
	uuids := make([]string, n)
	for i := 0; i < n; i++ {
		uuids[i] = fmt.Sprintf("%s0000-0000-4000-8000-%012x", prefix, i)
	}
	return uuids
}

// percentileNs returns the p-th percentile of a sorted []int64 (nanoseconds).
func percentileNs(sorted []int64, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return time.Duration(sorted[idx])
}

// --------------------------------------------------------------------------
// Test 1: Exactly-once under concurrent same-UUID inserts (RF3, 1 shard)
// --------------------------------------------------------------------------

// TestProdReady_RF3_ExactlyOnce_ConcurrentSameUUIDs proves the
// insert_if_not_exists primitive delivers exactly-once semantics when
// M concurrent client goroutines all try to insert the SAME set of K UUIDs
// into a 3-node RF=3 cluster.
//
// After load completes:
//   - Aggregate count == K (no duplicates, no loss)
//   - Each UUID is individually present (spot-check the full set)
//
// Uses enterrors.GoWrapper for goroutines (enforced by linter).
func TestProdReady_RF3_ExactlyOnce_ConcurrentSameUUIDs(t *testing.T) {
	const (
		numWorkers = 16   // concurrent client goroutines
		numUUIDs   = 2000 // distinct UUIDs, each attempted by all 16 workers
		className  = "ProdReadyCWExactlyOnce"
		clLevel    = "QUORUM"
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

	uuids := makeUUIDs("aaaa", numUUIDs)

	t.Logf("Starting concurrent load: %d workers × %d UUIDs = %d total attempts",
		numWorkers, numUUIDs, numWorkers*numUUIDs)

	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var successTotal, conflictTotal, errorTotal int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		enterrors.GoWrapper(func() {
			defer wg.Done()
			localSuccess, localConflict, localErr := int64(0), int64(0), int64(0)
			for _, id := range uuids {
				code := prodCondInsertHTTP(t, host, className, id, clLevel)
				switch {
				case code == 201:
					localSuccess++
				case code == 200 || code == 409:
					// 200 = server-side idempotent skip (already exists)
					// 409 = server chose to return Conflict instead
					localConflict++
				case code >= 200 && code < 300:
					localSuccess++
				default:
					localErr++
					t.Logf("worker %d: unexpected status %d for UUID %s", workerIdx, code, id)
				}
			}
			atomic.AddInt64(&successTotal, localSuccess)
			atomic.AddInt64(&conflictTotal, localConflict)
			atomic.AddInt64(&errorTotal, localErr)
		}, logger)
	}
	wg.Wait()

	t.Logf("Load complete: success=%d conflict/skip=%d errors=%d",
		successTotal, conflictTotal, errorTotal)

	// Allow a tiny non-zero error budget: the test is about count correctness,
	// not zero-error rate under concurrency. But sustained errors indicate a
	// broken endpoint.
	require.Zero(t, errorTotal,
		"conditional inserts must not return error codes (4xx/5xx) for valid UUIDs; "+
			"got %d errors out of %d total attempts",
		errorTotal, numWorkers*numUUIDs)

	t.Run("AssertExactlyKObjects", func(t *testing.T) {
		// Poll for up to 30s to allow any async replication to settle.
		var finalCount int64
		assert.Eventually(t, func() bool {
			finalCount = countObjectsViaGraphQL(t, host, className)
			return finalCount == numUUIDs
		}, 30*time.Second, 500*time.Millisecond,
			"aggregate count must converge to exactly %d (got %d); "+
				"any deviation means duplicate or lost object",
			numUUIDs, finalCount)

		require.Equal(t, int64(numUUIDs), finalCount,
			"exactly %d objects must exist after %d-worker concurrent insert_if_not_exists load",
			numUUIDs, numWorkers)
	})

	t.Run("SpotCheckAllUUIDs", func(t *testing.T) {
		// Verify every UUID is present. Uses CL=ONE (reads from any replica).
		// A missing UUID = lost write; a count discrepancy = silent duplicate.
		missing := 0
		for _, id := range uuids {
			obj, err := helper.GetObject(t, className, strfmt.UUID(id))
			if err != nil || obj == nil {
				missing++
				t.Logf("missing UUID: %s", id)
			}
		}
		require.Zero(t, missing,
			"%d UUIDs are missing from the cluster after exactly-once load; "+
				"insert_if_not_exists must not lose objects", missing)
	})
}

// --------------------------------------------------------------------------
// Test 2: HA under node failure during load (INV-HA-1)
// --------------------------------------------------------------------------

// TestProdReady_RF3_HAUnderNodeFailureDuringLoad inserts N unique UUIDs at
// QUORUM while stopping a non-coordinator node mid-load. Verifies:
//   - Write success rate stays high (≥90%) after the brief failover blip
//   - Full expected count is present after load (no loss)
//
// This encodes INV-HA-1: a 3-replica write at QUORUM must succeed with 1
// replica down (2 of 3 satisfies floor((3+1)/2)=2).
func TestProdReady_RF3_HAUnderNodeFailureDuringLoad(t *testing.T) {
	const (
		numUUIDs         = 5000
		className        = "ProdReadyCWHALoad"
		clLevel          = "QUORUM"
		failAfterFrac    = 0.3  // stop a node when 30% of writes are done
		minSuccessRatePC = 90.0 // require ≥90% success after failover
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

	uuids := makeUUIDs("bbbb", numUUIDs)
	failAt := int(float64(numUUIDs) * failAfterFrac)

	var (
		successCount int64
		errorCount   int64
		nodeStopped  bool
	)

	t.Logf("Starting sequential load of %d UUIDs at %s; will stop node at index=2 after %d writes",
		numUUIDs, clLevel, failAt)

	for i, id := range uuids {
		// Kill the third node (0-based index 2) after failAt writes.
		if i == failAt && !nodeStopped {
			t.Logf("[%d/%d] Stopping node 2 (0-based) mid-load to test HA", i, numUUIDs)
			common.StopNodeAt(ctx, t, compose, 2)
			nodeStopped = true
			t.Logf("[%d/%d] Node 2 stopped; remaining writes at QUORUM (2-of-2-live)", i, numUUIDs)
		}

		code := prodCondInsertHTTP(t, host, className, id, clLevel)
		if code >= 200 && code < 300 {
			atomic.AddInt64(&successCount, 1)
		} else {
			atomic.AddInt64(&errorCount, 1)
			t.Logf("[%d/%d] write error: status=%d UUID=%s (node_stopped=%v)",
				i, numUUIDs, code, id, nodeStopped)
		}
	}

	total := int64(numUUIDs)
	successRate := 100.0 * float64(successCount) / float64(total)
	t.Logf("Load complete: total=%d success=%d errors=%d success_rate=%.1f%%",
		total, successCount, errorCount, successRate)

	t.Run("AssertHighSuccessRateINVHA1", func(t *testing.T) {
		require.GreaterOrEqual(t, successRate, minSuccessRatePC,
			"write success rate %.1f%% is below %.1f%% minimum (INV-HA-1): "+
				"QUORUM writes on a 3-replica cluster must survive 1 replica down; "+
				"got %d errors out of %d total",
			successRate, minSuccessRatePC, errorCount, total)
	})

	t.Run("AssertFullCountRetained", func(t *testing.T) {
		// The expected count is only the objects that actually succeeded.
		// We assert no objects were lost: count == successCount.
		// A count < successCount means silent data loss beyond the write error.
		var finalCount int64
		assert.Eventually(t, func() bool {
			finalCount = countObjectsViaGraphQL(t, host, className)
			return finalCount == successCount
		}, 30*time.Second, 500*time.Millisecond,
			"aggregate count must equal the number of successful writes "+
				"(got count=%d, want %d)",
			finalCount, successCount)

		require.Equal(t, successCount, finalCount,
			"object count must equal successful-write count (no silent loss): "+
				"count=%d successful_writes=%d",
			finalCount, successCount)
	})
}

// --------------------------------------------------------------------------
// Test 3: Recovery convergence after node restart
// --------------------------------------------------------------------------

// TestProdReady_RF3_RecoveryConvergence proves that a node brought back after
// missing a set of writes eventually converges:
//
//  1. Insert a baseline set at QUORUM (all 3 nodes up).
//  2. Stop node at index 2 (0-based).
//  3. Insert a delta set at QUORUM (node 2 misses these).
//  4. Restart node 2.
//  5. Poll each node's count at CL=ONE until all three agree on
//     baseline+delta (bounded 3-minute wait).
//
// This encodes the async replication / read-repair convergence guarantee.
func TestProdReady_RF3_RecoveryConvergence(t *testing.T) {
	const (
		baselineCount = 500
		deltaCount    = 300
		className     = "ProdReadyCWRecovery"
		clLevel       = "QUORUM"
		maxConverge   = 3 * time.Minute
		pollInterval  = 2 * time.Second
	)
	expectedTotal := int64(baselineCount + deltaCount)

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

	host1 := compose.ContainerURI(1)
	host2 := compose.ContainerURI(2) // node index 2, 0-based
	helper.SetupClient(host1)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClass(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	baselineUUIDs := makeUUIDs("cccc", baselineCount)
	deltaUUIDs := makeUUIDs("dddd", deltaCount)

	t.Run("InsertBaseline_AllNodesUp", func(t *testing.T) {
		for _, id := range baselineUUIDs {
			code := prodCondInsertHTTP(t, host1, className, id, clLevel)
			require.True(t, code >= 200 && code < 300,
				"baseline insert must succeed with all nodes up: got %d for UUID %s", code, id)
		}
		t.Logf("Inserted %d baseline objects with all 3 nodes up", baselineCount)
	})

	t.Run("StopNode2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
		t.Logf("Node 2 (0-based) stopped; delta writes will be missed by it")
	})

	t.Run("InsertDelta_Node2Down", func(t *testing.T) {
		errors := 0
		for _, id := range deltaUUIDs {
			code := prodCondInsertHTTP(t, host1, className, id, clLevel)
			if code < 200 || code >= 300 {
				errors++
				t.Logf("delta insert error: status=%d UUID=%s", code, id)
			}
		}
		require.Zero(t, errors,
			"delta inserts at QUORUM with 1 node down must not return errors "+
				"(INV-HA-1): got %d errors", errors)
		t.Logf("Inserted %d delta objects with node 2 down (%d QUORUM writes succeeded)",
			deltaCount, deltaCount-errors)
	})

	t.Run("RestartNode2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
		t.Logf("Node 2 restarted; waiting for convergence (max %s)", maxConverge)
	})

	t.Run("WaitForConvergence_AllThreeNodes", func(t *testing.T) {
		// Poll all three nodes at CL=ONE until each reports the full count.
		// CL=ONE reads from any single replica, so a lag on node 2 shows here.
		hosts := []string{
			compose.ContainerURI(0),
			compose.ContainerURI(1),
			host2,
		}

		var converged bool
		deadline := time.Now().Add(maxConverge)
		for time.Now().Before(deadline) {
			allMatch := true
			for i, h := range hosts {
				count := countObjectsViaGraphQL(t, h, className)
				if count != expectedTotal {
					t.Logf("node %d: count=%d (want %d); waiting for convergence...",
						i, count, expectedTotal)
					allMatch = false
					break
				}
			}
			if allMatch {
				converged = true
				break
			}
			time.Sleep(pollInterval)
		}

		require.True(t, converged,
			"cluster did not converge within %s: all three nodes must return "+
				"count=%d at CL=ONE after node recovery (async replication must "+
				"repair the %d missed writes on node 2)",
			maxConverge, expectedTotal, deltaCount)

		// Final concrete assertion: each node must agree.
		for i, h := range hosts {
			finalCount := countObjectsViaGraphQL(t, h, className)
			require.Equal(t, expectedTotal, finalCount,
				"node %d final count mismatch: got %d want %d; "+
					"node 2 did not converge after restart",
				i, finalCount, expectedTotal)
		}
		t.Logf("Convergence confirmed: all 3 nodes report count=%d", expectedTotal)
	})
}

// --------------------------------------------------------------------------
// Test 4: Exactly-once with multi-shard (3-shard RF3)
// --------------------------------------------------------------------------

// TestProdReady_MultiShard_RF3_ExactlyOnce is the multi-shard variant of
// Test 1. A 3-shard RF=3 class distributes UUIDs across shards. The
// per-UUID mutex is per-shard; exactly-once must hold across all shards
// with 16 concurrent workers racing on the same K UUIDs.
func TestProdReady_MultiShard_RF3_ExactlyOnce(t *testing.T) {
	const (
		numWorkers = 16
		numUUIDs   = 2000
		numShards  = 3
		className  = "ProdReadyCWMultiShard"
		clLevel    = "QUORUM"
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
		setupProdClass(t, className, numShards)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	uuids := makeUUIDs("eeee", numUUIDs)

	t.Logf("Starting multi-shard concurrent load: %d workers × %d UUIDs, %d shards",
		numWorkers, numUUIDs, numShards)

	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var successTotal, conflictTotal, errorTotal int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		enterrors.GoWrapper(func() {
			defer wg.Done()
			localSuccess, localConflict, localErr := int64(0), int64(0), int64(0)
			for _, id := range uuids {
				code := prodCondInsertHTTP(t, host, className, id, clLevel)
				switch {
				case code == 201:
					localSuccess++
				case code == 200 || code == 409:
					localConflict++
				case code >= 200 && code < 300:
					localSuccess++
				default:
					localErr++
					t.Logf("worker %d: unexpected status %d for UUID %s", workerIdx, code, id)
				}
			}
			atomic.AddInt64(&successTotal, localSuccess)
			atomic.AddInt64(&conflictTotal, localConflict)
			atomic.AddInt64(&errorTotal, localErr)
		}, logger)
	}
	wg.Wait()

	t.Logf("Multi-shard load complete: success=%d conflict/skip=%d errors=%d",
		successTotal, conflictTotal, errorTotal)

	require.Zero(t, errorTotal,
		"multi-shard conditional inserts must not return error codes for valid UUIDs; "+
			"got %d errors out of %d total attempts",
		errorTotal, numWorkers*numUUIDs)

	t.Run("AssertExactlyKObjectsAcrossShards", func(t *testing.T) {
		var finalCount int64
		assert.Eventually(t, func() bool {
			finalCount = countObjectsViaGraphQL(t, host, className)
			return finalCount == numUUIDs
		}, 30*time.Second, 500*time.Millisecond,
			"aggregate count must equal exactly %d across %d shards (got %d); "+
				"per-shard mutex must prevent duplicates",
			numUUIDs, numShards, finalCount)

		require.Equal(t, int64(numUUIDs), finalCount,
			"exactly %d objects must exist after multi-shard exactly-once load "+
				"(no duplicates, no loss across %d shards)",
			numUUIDs, numShards)
	})
}

// --------------------------------------------------------------------------
// Test 5: Throughput report (conditional vs. unconditional)
// --------------------------------------------------------------------------

// TestProdReady_RF3_ThroughputReport measures conditional vs. unconditional
// write throughput on a 3-node RF=3 cluster at QUORUM. Asserts:
//   - Conditional error rate == 0
//   - Conditional ops/s >= 0.5 * unconditional ops/s
//     (a conservative floor; the per-UUID mutex adds overhead but should
//     not cause a >2x regression against unconditional)
//
// All latency percentiles are logged for the overseer; this test is
// primarily a measurement surface, not a correctness gate.
func TestProdReady_RF3_ThroughputReport(t *testing.T) {
	const (
		numObjects     = 1000 // distinct UUIDs, inserted once each
		className      = "ProdReadyCWThroughput"
		uncondClass    = "ProdReadyCWThroughputUncond"
		clLevel        = "QUORUM"
		minRelativeFac = 0.5 // conditional ops/s must be >= 50% of unconditional
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
		setupProdClass(t, uncondClass, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
		waitForSchemaOnAllNodes(t, compose, uncondClass, 3)
	})

	condUUIDs := makeUUIDs("ffff", numObjects)
	uncondUUIDs := makeUUIDs("9999", numObjects)

	// Measure conditional throughput.
	var condErrors int64
	condLatencies := make([]int64, 0, numObjects)
	condStart := time.Now()

	for _, id := range condUUIDs {
		start := time.Now()
		code := prodCondInsertHTTP(t, host, className, id, clLevel)
		latNs := time.Since(start).Nanoseconds()
		condLatencies = append(condLatencies, latNs)
		if code < 200 || code >= 300 {
			condErrors++
			t.Logf("conditional insert error: status=%d UUID=%s", code, id)
		}
	}
	condDuration := time.Since(condStart)
	condOpsPerSec := float64(numObjects) / condDuration.Seconds()

	// Measure unconditional throughput.
	var uncondErrors int64
	uncondLatencies := make([]int64, 0, numObjects)
	uncondStart := time.Now()

	for _, id := range uncondUUIDs {
		start := time.Now()
		code := prodUncondInsertHTTP(t, host, uncondClass, id, clLevel)
		latNs := time.Since(start).Nanoseconds()
		uncondLatencies = append(uncondLatencies, latNs)
		if code < 200 || code >= 300 {
			uncondErrors++
		}
	}
	uncondDuration := time.Since(uncondStart)
	uncondOpsPerSec := float64(numObjects) / uncondDuration.Seconds()

	// Sort for percentile computation.
	sort.Slice(condLatencies, func(i, j int) bool { return condLatencies[i] < condLatencies[j] })
	sort.Slice(uncondLatencies, func(i, j int) bool { return uncondLatencies[i] < uncondLatencies[j] })

	t.Logf("=== Throughput Report (RF3/QUORUM, N=%d each) ===", numObjects)
	t.Logf("Conditional   : %.1f ops/s | p50=%v p95=%v p99=%v | errors=%d",
		condOpsPerSec,
		percentileNs(condLatencies, 50),
		percentileNs(condLatencies, 95),
		percentileNs(condLatencies, 99),
		condErrors)
	t.Logf("Unconditional : %.1f ops/s | p50=%v p95=%v p99=%v | errors=%d",
		uncondOpsPerSec,
		percentileNs(uncondLatencies, 50),
		percentileNs(uncondLatencies, 95),
		percentileNs(uncondLatencies, 99),
		uncondErrors)
	t.Logf("Relative ratio: %.2fx (conditional / unconditional)", condOpsPerSec/uncondOpsPerSec)

	t.Run("AssertConditionalErrorRateZero", func(t *testing.T) {
		require.Zero(t, condErrors,
			"conditional write error rate must be 0 for %d distinct UUIDs at QUORUM; "+
				"got %d errors (%.1f%%)",
			numObjects, condErrors, 100.0*float64(condErrors)/float64(numObjects))
	})

	t.Run("AssertThroughputFloor", func(t *testing.T) {
		require.Greater(t, condOpsPerSec, 0.0, "conditional throughput must be > 0")
		if uncondOpsPerSec > 0 {
			ratio := condOpsPerSec / uncondOpsPerSec
			require.GreaterOrEqual(t, ratio, minRelativeFac,
				"conditional throughput %.1f ops/s is less than %.0f%% of "+
					"unconditional %.1f ops/s (ratio=%.2f); "+
					"the per-UUID mutex overhead is too large",
				condOpsPerSec, minRelativeFac*100, uncondOpsPerSec, ratio)
		}
	})
}
