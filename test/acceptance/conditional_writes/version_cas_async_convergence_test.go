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

// Package conditional_writes: async-replication version-convergence coverage.
//
// This file closes the "whack-a-mole" surface for async-repair version
// preservation: the three tests below cover the journeys adjacent to
// TestProdReadyVersion_RecoveryConvergence that could silently diverge
// even after the main fix:
//
//   - TestVersionCAS_AsyncConv_DeltaConvergence: node down during version-CAS
//     updates → all 3 nodes converge to the latest version after restart.
//     (Same journey as RecoveryConvergence, pinned here as a dedicated unit
//     because the RecoveryConvergence test exercises the outer prod-readiness
//     harness; this test is a focused convergence-only regression pin.)
//
//   - TestVersionCAS_AsyncConv_InitialInsertConvergence: object created
//     entirely while a node was down → after restart the node converges to
//     the correct version (not 0) via async repair.
//
//   - TestVersionCAS_AsyncConv_PostConvergenceIfMatch: after convergence, an
//     If-Match update using the converged version succeeds on a coordinator
//     that routes to the formerly-down node.
//
// Each test boots its own real 3-node cluster via testcontainers. Run with:
//
//	TEST_WEAVIATE_IMAGE=weaviate/test-server:phase2 \
//	  go test -run TestVersionCAS_AsyncConv ./test/acceptance/conditional_writes/... \
//	  -timeout 2400s -v
package conditional_writes

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	asyncConvPropagationDelay = "200ms"
	asyncConvMaxConverge      = 3 * time.Minute
	asyncConvPollInterval     = 2 * time.Second
)

// waitVersionConvergenceAllNodes polls each node at CL=ONE until all nodes
// return wantVersion, or until deadline elapses.  Returns true on convergence.
func waitVersionConvergenceAllNodes(
	t *testing.T,
	hosts []string,
	className string,
	objectID string,
	wantVersion uint64,
	maxWait time.Duration,
) bool {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		allMatch := true
		for i, h := range hosts {
			v := versionCASGetVersionDirect(t, h, className, objectID)
			if v != wantVersion {
				t.Logf("node %d (%s): version=%d want=%d; still converging...", i, h, v, wantVersion)
				allMatch = false
				break
			}
		}
		if allMatch {
			return true
		}
		time.Sleep(asyncConvPollInterval)
	}
	return false
}

// --------------------------------------------------------------------------
// Test A: delta version-CAS updates missed by a down node → converge
// --------------------------------------------------------------------------

// TestVersionCAS_AsyncConv_DeltaConvergence pins that a node that was down
// during a series of version-CAS updates at QUORUM converges to the latest
// version after restarting, via async hashtree repair.
//
// Causal link: this test catches async-repair dropping or re-minting the
// coordinator-assigned version, because the restarted node's local read at
// CL=ONE must return exactly finalVersion (not 0, not an intermediate value).
func TestVersionCAS_AsyncConv_DeltaConvergence(t *testing.T) {
	const (
		className    = "AsyncConvDelta"
		clLevel      = "QUORUM"
		deltaUpdates = 10
		objectID     = "ac000001-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ASYNC_REPLICATION_PROPAGATION_DELAY", asyncConvPropagationDelay).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClassAsync(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert baseline object.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"baseline insert must succeed: got status %d", code)
	baseVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, baseVersion, "inserted object must have a non-zero version")
	t.Logf("baseline version: %d", baseVersion)

	// Take node 2 down.
	t.Run("StopNode2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Apply delta version-CAS updates at QUORUM while node 2 is down.
	var finalVersion uint64
	t.Run("ApplyDeltaUpdates_Node2Down", func(t *testing.T) {
		cur := baseVersion
		for i := 0; i < deltaUpdates; i++ {
			result := versionCASPutHTTP(t, host, className, objectID, cur,
				fmt.Sprintf("async-delta-%d", i))
			require.Equal(t, http.StatusOK, result.StatusCode,
				"version-CAS delta update %d must succeed at QUORUM: got status %d", i, result.StatusCode)
			cur = versionCASGetVersionDirect(t, host, className, objectID)
			require.NotZero(t, cur, "re-read version after update %d must be non-zero", i)
		}
		finalVersion = cur
		t.Logf("final version after %d delta updates: %d", deltaUpdates, finalVersion)
	})

	// Restart node 2.
	t.Run("RestartNode2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	// All three nodes must converge to finalVersion via async repair.
	t.Run("WaitConvergence", func(t *testing.T) {
		hosts := []string{
			compose.ContainerURI(0),
			compose.ContainerURI(1),
			compose.ContainerURI(2),
		}
		converged := waitVersionConvergenceAllNodes(t, hosts, className, objectID,
			finalVersion, asyncConvMaxConverge)
		require.True(t, converged,
			"cluster did not converge to version=%d within %s after node 2 restart; "+
				"async repair must propagate the %d missed version-CAS updates",
			finalVersion, asyncConvMaxConverge, deltaUpdates)

		for i, h := range hosts {
			v := versionCASGetVersionDirect(t, h, className, objectID)
			require.Equal(t, finalVersion, v,
				"node %d: final version mismatch: got %d want %d", i, v, finalVersion)
		}
		t.Logf("all 3 nodes report version=%d", finalVersion)
	})
}

// --------------------------------------------------------------------------
// Test B: object created entirely while a node was down → converge
// --------------------------------------------------------------------------

// TestVersionCAS_AsyncConv_InitialInsertConvergence pins that an object that
// was inserted while a node was completely down converges to the correct
// non-zero version on the formerly-down node after it restarts.
//
// Causal link: this test catches async-repair not propagating the initial
// insert at all, or propagating it but losing the version (leaving the
// formerly-down node with version=0).  The assertion checks version equality
// across all 3 nodes at CL=ONE.
func TestVersionCAS_AsyncConv_InitialInsertConvergence(t *testing.T) {
	const (
		className = "AsyncConvInitInsert"
		clLevel   = "ONE" // write to 1 node only so node 2 definitely misses it
		objectID  = "ac000002-0000-4000-8000-000000000002"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ASYNC_REPLICATION_PROPAGATION_DELAY", asyncConvPropagationDelay).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClassAsync(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Take node 2 down before the object exists.
	t.Run("StopNode2_BeforeInsert", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Insert the object while node 2 is completely down.
	var insertedVersion uint64
	t.Run("InsertObject_Node2Down", func(t *testing.T) {
		code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
		require.True(t, code >= 200 && code < 300,
			"insert while node 2 down must succeed at CL=ONE: got status %d", code)
		insertedVersion = versionCASGetVersionDirect(t, host, className, objectID)
		require.NotZero(t, insertedVersion,
			"freshly inserted object must have a non-zero version")
		t.Logf("inserted version: %d", insertedVersion)
	})

	// Restart node 2.
	t.Run("RestartNode2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	// Node 2 missed the entire insert, so it starts at version 0.
	// Async repair must propagate the object (with its version) to node 2.
	t.Run("WaitConvergence_AllThreeNodes", func(t *testing.T) {
		hosts := []string{
			compose.ContainerURI(0),
			compose.ContainerURI(1),
			compose.ContainerURI(2),
		}
		converged := waitVersionConvergenceAllNodes(t, hosts, className, objectID,
			insertedVersion, asyncConvMaxConverge)
		require.True(t, converged,
			"node 2 did not receive the initial insert via async repair within %s; "+
				"all 3 nodes must report version=%d at CL=ONE",
			asyncConvMaxConverge, insertedVersion)

		for i, h := range hosts {
			v := versionCASGetVersionDirect(t, h, className, objectID)
			require.Equal(t, insertedVersion, v,
				"node %d: version mismatch after convergence: got %d want %d", i, v, insertedVersion)
		}
		t.Logf("all 3 nodes report version=%d after initial-insert convergence", insertedVersion)
	})
}

// --------------------------------------------------------------------------
// Test C: post-convergence If-Match succeeds on the formerly-down node
// --------------------------------------------------------------------------

// TestVersionCAS_AsyncConv_PostConvergenceIfMatch pins that after async repair
// has converged the formerly-down node to the correct version, a subsequent
// If-Match version-CAS update routed through that node succeeds.
//
// Causal link: this test catches a scenario where the version is repaired
// correctly on the data replica (shard) of node 2 but is invisible to
// coordinator logic running on node 2, causing false 412 rejections on a
// valid If-Match value.  A 200 response from node 2 is the positive proof.
func TestVersionCAS_AsyncConv_PostConvergenceIfMatch(t *testing.T) {
	const (
		className    = "AsyncConvIfMatch"
		clLevel      = "QUORUM"
		deltaUpdates = 5
		objectID     = "ac000003-0000-4000-8000-000000000003"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ASYNC_REPLICATION_PROPAGATION_DELAY", asyncConvPropagationDelay).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	host0 := compose.ContainerURI(0)
	host1 := compose.ContainerURI(1)
	helper.SetupClient(host1)

	t.Run("CreateSchema", func(t *testing.T) {
		setupProdClassAsync(t, className, 1)
		waitForSchemaOnAllNodes(t, compose, className, 3)
	})

	// Insert baseline object.
	code := prodCondInsertHTTP(t, host1, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"baseline insert must succeed: got status %d", code)
	baseVersion := versionCASGetVersionDirect(t, host1, className, objectID)
	require.NotZero(t, baseVersion)

	// Take node 2 down.
	t.Run("StopNode2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Apply delta updates at QUORUM while node 2 is down.
	var finalVersion uint64
	t.Run("ApplyDelta_Node2Down", func(t *testing.T) {
		cur := baseVersion
		for i := 0; i < deltaUpdates; i++ {
			result := versionCASPutHTTP(t, host1, className, objectID, cur,
				fmt.Sprintf("pre-conv-delta-%d", i))
			require.Equal(t, http.StatusOK, result.StatusCode,
				"delta update %d must succeed at QUORUM: got status %d", i, result.StatusCode)
			cur = versionCASGetVersionDirect(t, host1, className, objectID)
			require.NotZero(t, cur)
		}
		finalVersion = cur
		t.Logf("version after delta updates: %d", finalVersion)
	})

	// Restart node 2.
	t.Run("RestartNode2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	// Wait for node 2 to converge.
	t.Run("WaitConvergence", func(t *testing.T) {
		hosts := []string{
			compose.ContainerURI(0),
			compose.ContainerURI(1),
			compose.ContainerURI(2),
		}
		converged := waitVersionConvergenceAllNodes(t, hosts, className, objectID,
			finalVersion, asyncConvMaxConverge)
		require.True(t, converged,
			"cluster did not converge to version=%d within %s", finalVersion, asyncConvMaxConverge)
		t.Logf("convergence confirmed: all nodes at version=%d", finalVersion)
	})

	// Now issue a version-CAS update through node 0 (which may route the shard
	// operation to the formerly-down node 2 depending on shard assignment).
	// The update must succeed with the converged version.
	t.Run("IfMatchSucceeds_PostConvergence", func(t *testing.T) {
		// Re-read the current version from all nodes to confirm convergence.
		v0 := versionCASGetVersionDirect(t, host0, className, objectID)
		v1 := versionCASGetVersionDirect(t, host1, className, objectID)
		v2 := versionCASGetVersionDirect(t, compose.ContainerURI(2), className, objectID)
		require.Equal(t, finalVersion, v0, "node 0 version must equal finalVersion")
		require.Equal(t, finalVersion, v1, "node 1 version must equal finalVersion")
		require.Equal(t, finalVersion, v2, "node 2 version must equal finalVersion")

		// Issue a version-CAS PUT through node 0 (the coordinator for this call).
		// If node 2's shard holds the object and its version is correctly repaired,
		// the CAS succeeds.
		result := versionCASPutHTTP(t, host0, className, objectID, finalVersion,
			"post-convergence-write")
		require.Equal(t, http.StatusOK, result.StatusCode,
			"If-Match update at version=%d through node 0 must succeed after convergence: "+
				"got status %d (node 2 shard must have been repaired to the correct version)",
			finalVersion, result.StatusCode)

		newVersion := versionCASGetVersionDirect(t, host0, className, objectID)
		require.Equal(t, finalVersion+1, newVersion,
			"version after post-convergence update must be finalVersion+1=%d, got %d",
			finalVersion+1, newVersion)
		t.Logf("post-convergence If-Match succeeded: version %d → %d", finalVersion, newVersion)
	})
}
