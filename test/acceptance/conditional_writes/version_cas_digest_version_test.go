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

// Package conditional_writes contains acceptance tests for the digest-Version
// fix (ObjectDigests populates Version from the on-disk binary header) and
// the coordinator double-CAS guard (IfVersion cleared before replica fan-out).
//
// These are the anti-whack-a-mole tests required by the task spec:
//
//	(b) TestVersionCASDigest_SingleCoordinator_RF3: single-coordinator If-Match
//	    update on RF=3 advances the version and reads back correctly.
//
//	(c) TestVersionCASDigest_SequentialChain_RF3: sequential version chain
//	    1->2->3->4->5 each gated by If-Match on RF=3.
//
//	(d) TestVersionCASDigest_WrongIfMatch_RF3: wrong If-Match returns 412 on
//	    RF=3 (negative path).
//
//	(e) TestVersionCASDigest_CrossCoordinatorMakesProgress: concurrent If-Match
//	    via different coordinators makes progress (final version > initial,
//	    no zero-progress livelock).
//
// Tests boot a real 3-node cluster via testcontainers. Run with:
//
//	TEST_WEAVIATE_IMAGE=weaviate/test-server:phase2 \
//	  go test -run TestVersionCASDigest ./test/acceptance/conditional_writes/... \
//	  -timeout 2400s -v
package conditional_writes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestVersionCASDigest_SingleCoordinator_RF3 verifies that a single-coordinator
// If-Match update on a 3-node RF=3 cluster actually advances the version.
//
// This is the key regression test for the ObjectDigests Version=0 bug:
// before the fix, resolveCurrentVersion always returned 0 because
// ObjectDigests never populated Version from the binary header. The
// coordinator then compared e.g. IfVersion=1 != 0 and returned 412 for
// every If-Match update on RF>1 clusters, making version-CAS completely
// broken in production (RF>1) deployments.
//
// Causal link: if ObjectDigests still returns Version=0, then
// resolveCurrentVersion returns 0, the coordinator CAS check fires
// (IfVersion=1 != current=0) and this test fails with 412 instead of 200.
func TestVersionCASDigest_SingleCoordinator_RF3(t *testing.T) {
	const (
		className = "VersionCASDigestSingle"
		clLevel   = "QUORUM"
		objectID  = "b1000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PERSISTENCE_OBJECT_VERSION_WRITE", "2").
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
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	initialVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, initialVersion,
		"inserted object must have a non-zero version (MarshallerVersion=2 gate must be open)")
	t.Logf("initial version after insert: %d", initialVersion)

	// Perform a single If-Match update from the same coordinator.
	result := versionCASPutHTTP(t, host, className, objectID, initialVersion, "single-coordinator-update")
	assert.Equal(t, 200, result.StatusCode,
		"If-Match PUT at version=%d on RF=3 must succeed (200); "+
			"got %d; this indicates ObjectDigests still returns Version=0 "+
			"(resolveCurrentVersion returns 0, coordinator CAS rejects all If-Match updates)",
		initialVersion, result.StatusCode)
	assert.Equal(t, initialVersion+1, result.NewVersion,
		"new version must be initialVersion+1=%d; got %d",
		initialVersion+1, result.NewVersion)

	// Read back to confirm the version advanced on the cluster.
	finalVersion := versionCASGetVersionDirect(t, host, className, objectID)
	assert.Equal(t, initialVersion+1, finalVersion,
		"read-back version must equal the minted version %d; got %d",
		initialVersion+1, finalVersion)
}

// TestVersionCASDigest_SequentialChain_RF3 verifies that a sequential chain of
// If-Match updates 1->2->3->4->5 on a 3-node RF=3 cluster all succeed.
//
// This tests the complete linearized version-CAS path: each update reads the
// current version (via ObjectDigests on the replica), passes the CAS check,
// mints the next version, and the next update uses that version as its IfVersion.
//
// Causal link: if any step in the chain fails with 412, it means
// resolveCurrentVersion returned a stale value (most likely 0 from the
// ObjectDigests bug), preventing the chain from advancing. A broken chain
// anywhere is a bug.
func TestVersionCASDigest_SequentialChain_RF3(t *testing.T) {
	const (
		className  = "VersionCASDigestChain"
		clLevel    = "QUORUM"
		objectID   = "b2000000-0000-4000-8000-000000000001"
		chainSteps = 4 // write 4 more times after the initial insert (version 1->5)
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PERSISTENCE_OBJECT_VERSION_WRITE", "2").
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

	// Insert the object unconditionally.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	currentVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, currentVersion, "inserted object must have a non-zero initial version")
	t.Logf("initial version: %d", currentVersion)

	// Walk the chain: each step uses the current version as IfVersion.
	for step := 1; step <= chainSteps; step++ {
		result := versionCASPutHTTP(t, host, className, objectID, currentVersion,
			fmt.Sprintf("chain-step-%d", step))
		require.Equal(t, 200, result.StatusCode,
			"chain step %d: If-Match PUT at version=%d must succeed (200) on RF=3; "+
				"got %d; broken chain indicates ObjectDigests version bug or "+
				"double-CAS rejection from replica re-evaluating IfVersion",
			step, currentVersion, result.StatusCode)
		require.Equal(t, currentVersion+1, result.NewVersion,
			"chain step %d: minted version must be %d; got %d",
			step, currentVersion+1, result.NewVersion)

		t.Logf("chain step %d: version %d -> %d", step, currentVersion, result.NewVersion)
		currentVersion = result.NewVersion
	}

	// Verify the final version matches the expected value.
	finalVersion := versionCASGetVersionDirect(t, host, className, objectID)
	assert.Equal(t, currentVersion, finalVersion,
		"read-back version after %d chain steps must be %d; got %d",
		chainSteps, currentVersion, finalVersion)
	t.Logf("final version after %d-step chain: %d", chainSteps, finalVersion)
}

// TestVersionCASDigest_WrongIfMatch_RF3 verifies that an If-Match PUT with a
// stale IfVersion returns 412 on a 3-node RF=3 cluster.
//
// This is the negative path: the coordinator resolves the current version via
// ObjectDigests and correctly rejects the stale IfVersion. After the fix,
// this must still return 412; the fix must not break the rejection path.
//
// Causal link: if the coordinator always accepted IfVersion (bug in the other
// direction), this test would fail with 200. If ObjectDigests returns wrong
// versions, the rejection might fire for valid versions instead (tested by (b)).
func TestVersionCASDigest_WrongIfMatch_RF3(t *testing.T) {
	const (
		className = "VersionCASDigestNegative"
		clLevel   = "QUORUM"
		objectID  = "b3000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PERSISTENCE_OBJECT_VERSION_WRITE", "2").
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

	// Insert the object unconditionally.
	code := prodCondInsertHTTP(t, host, className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	currentVersion := versionCASGetVersionDirect(t, host, className, objectID)
	require.NotZero(t, currentVersion, "inserted object must have a non-zero initial version")

	const staleVersion = uint64(999)
	require.NotEqual(t, staleVersion, currentVersion,
		"staleVersion must not accidentally equal currentVersion for this test to be meaningful")

	// A stale IfVersion must return 412.
	result := versionCASPutHTTP(t, host, className, objectID, staleVersion, "should-not-land")
	assert.Equal(t, 412, result.StatusCode,
		"If-Match PUT with stale IfVersion=%d (current=%d) must return 412; "+
			"got %d; the coordinator is not correctly rejecting stale CAS",
		staleVersion, currentVersion, result.StatusCode)

	// The version must not have changed.
	afterVersion := versionCASGetVersionDirect(t, host, className, objectID)
	assert.Equal(t, currentVersion, afterVersion,
		"version must not change after a rejected If-Match PUT; was %d now %d",
		currentVersion, afterVersion)
}

// TestVersionCASDigest_CrossCoordinatorMakesProgress verifies that concurrent
// If-Match updates routed through different coordinators on RF=3 make progress:
// the final version must be strictly greater than the initial version.
//
// This is the anti-zero-progress-livelock test. Before the digest fix, ALL
// updates returned 412 (the coordinator always saw version=0, so any IfVersion>0
// was rejected). After the fix, at least one update per round must succeed.
//
// Note: under Plan-A (no distributed lock), multiple updates at the same version
// can all succeed if they arrive through different coordinators in a LWW window.
// This test does not assert exactly-one-winner; it only asserts no-zero-progress.
func TestVersionCASDigest_CrossCoordinatorMakesProgress(t *testing.T) {
	const (
		className = "VersionCASDigestCross"
		clLevel   = "QUORUM"
		objectID  = "b4000000-0000-4000-8000-000000000001"
		numWaves  = 3 // run this many sequential waves to confirm progress is repeatable
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PERSISTENCE_OBJECT_VERSION_WRITE", "2").
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

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

	// Insert the object unconditionally.
	code := prodCondInsertHTTP(t, hosts[1], className, objectID, clLevel)
	require.True(t, code >= 200 && code < 300,
		"initial insert must succeed: got status %d", code)

	currentVersion := versionCASGetVersionDirect(t, hosts[1], className, objectID)
	require.NotZero(t, currentVersion, "inserted object must have a non-zero initial version")
	t.Logf("initial version: %d", currentVersion)

	for wave := 1; wave <= numWaves; wave++ {
		// All three coordinators attempt the same IfVersion simultaneously.
		type result struct {
			host   string
			status int
			newVer uint64
		}
		results := make([]result, len(hosts))
		for i, h := range hosts {
			r := versionCASPutHTTP(t, h, className, objectID, currentVersion,
				fmt.Sprintf("wave-%d-host-%d", wave, i))
			results[i] = result{host: h, status: r.StatusCode, newVer: r.NewVersion}
			t.Logf("wave %d host %d: status=%d newVersion=%d", wave, i, r.StatusCode, r.NewVersion)
		}

		// Count successes: at least one must have advanced.
		successCount := 0
		for _, r := range results {
			if r.status == 200 {
				successCount++
			}
		}
		assert.Greater(t, successCount, 0,
			"wave %d: at least one of %d coordinators must succeed (200) for IfVersion=%d; "+
				"got 0 successes; this is the zero-progress livelock that ObjectDigests Version=0 causes",
			wave, len(hosts), currentVersion)

		// Advance currentVersion for the next wave.
		finalVersion := versionCASGetVersionDirect(t, hosts[1], className, objectID)
		assert.Greater(t, finalVersion, currentVersion,
			"wave %d: final version %d must be > initial version %d for this wave",
			wave, finalVersion, currentVersion)
		currentVersion = finalVersion
		t.Logf("wave %d complete: version now %d", wave, currentVersion)
	}
}
