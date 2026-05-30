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
// TEST-FIRST NOTICE: These tests are intentionally RED until Phase-1
// implementation lands (insert_if_not_exists + CL-inheritance plumbing).
// The harness (cluster boot, replica kill) is green; only the conditional
// write assertions are expected to fail with 404/422 until the endpoint
// is wired. Do NOT stub the feature to force a pass; the test is the
// contract lock.
//
// Relevant invariant: INV-HA-1 (ha-write-under-n-minus-1-replicas).
// A 3-replica class with RF=3 must complete a write at the default
// ConsistencyLevel (QUORUM) with 1 replica down. v3 synthesis broke
// this by forcing CL=ALL; v4 restores it by inheriting the request CL.
// This file is the regression test that would have caught the v3 break.
package conditional_writes

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// haTestClassName is the collection used by both HA tests. RF=3 so that
// floor(3/2)=1 replica can be down and QUORUM (2-of-3) still completes.
const haTestClassName = "ConditionalWriteHATest"

// TestHAPreservationDefaultCL encodes INV-HA-1:
//
//	With 1 of 3 replicas down, an insert_if_not_exists at the DEFAULT
//	ConsistencyLevel (QUORUM) MUST return 2xx. Under v3's CL=ALL default
//	this would have been a 4xx (not-enough-replicas). v4 restores the HA
//	invariant by inheriting the request CL (QUORUM).
//
// startHA3NodeClusterKillThird boots a 3-node cluster, creates the HA test
// schema on it, kills node 2, and waits for memberlist failure detection to
// complete.  It registers all cleanup via t.Cleanup so callers need not track
// the compose object.  Returns (ctx, compose) ready for write assertions.
func startHA3NodeClusterKillThird(t *testing.T) (context.Context, *docker.DockerCompose) {
	t.Helper()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	t.Cleanup(func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	})

	// Use node 1 as the API target for schema ops.
	helper.SetupClient(compose.ContainerURI(1))

	t.Run("CreateSchema", func(t *testing.T) {
		setupRF3Class(t, haTestClassName, 0)
	})

	// Stop node 2 (0-based); cluster is at N-1 = 2/3 replicas.
	// StopNodeAt polls /v1/nodes until the killed node is no longer reported
	// HEALTHY so memberlist failure-detection is complete before returning.
	t.Run("KillThirdNode", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	return ctx, compose
}

// TestHAPreservationDefaultCL encodes INV-HA-1:
//
//	With 1 of 3 replicas down, an insert_if_not_exists at the DEFAULT
//	ConsistencyLevel (QUORUM) MUST return 2xx. Under v3's CL=ALL default
//	this would have been a 4xx (not-enough-replicas). v4 restores the HA
//	invariant by inheriting the request CL (QUORUM).
//
// TEST-FIRST: until Phase-1 endpoint plumbing lands, this test is RED
// because /v1/objects does not yet support the "condition" field.
// The cluster harness (3-node up, 1 killed) is expected to work.
func TestHAPreservationDefaultCL(t *testing.T) {
	_, compose := startHA3NodeClusterKillThird(t)

	// --- AC 2: insert_if_not_exists at default CL with 1 replica down returns 2xx ---
	//
	// EXPECTED WHILE PHASE-1 IS NOT YET LANDED:
	//   The server does not yet recognise the "condition" field and will
	//   return 422 (unknown field) or similar. This assertion will FAIL.
	//   That is correct — the test is the contract lock for INV-HA-1.
	//
	// EXPECTED AFTER PHASE-1 LANDS:
	//   The server processes the conditional insert at QUORUM (default),
	//   contacts 2/3 replicas, and returns 201 (created) or 200 (skipped).
	t.Run("ConditionalInsert_DefaultCL_1ReplicaDown_Expects2xx", func(t *testing.T) {
		id := strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000001")
		statusCode := condInsertHTTP(t, compose.ContainerURI(1), haTestClassName, string(id), "")

		// 2xx: either 201 (object inserted) or 200 (object already existed, skipped).
		// Both are valid successful outcomes for insert_if_not_exists at QUORUM
		// when 1/3 replicas is down (INV-HA-1 preserved per v4 § 10.1).
		require.True(t, statusCode >= 200 && statusCode < 300,
			"insert_if_not_exists at default CL (QUORUM) with 1 replica down "+
				"must return 2xx (INV-HA-1). Got %d. "+
				"If this is 404/422 the Phase-1 endpoint is not yet wired "+
				"(test-first, expected RED until Phase-1 plumbing lands).",
			statusCode)
	})
}

// TestHAPreservationAllCL verifies CL=ALL semantics with 1 dead-detected replica
// for both the conditional and unconditional write paths.
//
// CL=ALL means "all currently-live replicas as known by memberlist."  When a
// node is detected dead, buildReplicas (cluster/router/router.go:159-175) prunes
// it: NodeHostname returns ok==false for memberlist-dead nodes.  StopNodeAt
// waits for failure-detection to complete before returning, so by write-time the
// dead node is gone from the replica set.  ToInt(n) receives n=2 (the two live
// nodes), CL=ALL resolves to 2, and both surviving nodes reply 2xx.
//
// The conditional sub-test (CL-inheritance principle, v4 § 5.4) and the
// unconditional parity sub-test (regression guard) share the same cluster
// harness and are deliberately kept together so any divergence is immediately
// visible.
func TestHAPreservationAllCL(t *testing.T) {
	_, compose := startHA3NodeClusterKillThird(t)

	insertFns := []struct {
		name   string
		id     string
		doInsert func(t *testing.T, host, className, id, cl string) int
		wantMsg  string
	}{
		{
			name: "ConditionalInsert_ExplicitALL_1ReplicaDown_Succeeds_LivePruned",
			id:   "bbbbbbbb-0000-0000-0000-000000000002",
			doInsert: func(t *testing.T, host, className, id, cl string) int {
				return condInsertHTTP(t, host, className, id, cl)
			},
			wantMsg: "insert_if_not_exists at explicit CL=ALL with 1 dead-detected replica " +
				"must return 2xx (ALL-of-2-live succeeds; dead node pruned by " +
				"buildReplicas at cluster/router/router.go:159-175). Got %d.",
		},
		{
			// Unconditional parity: plain POST /v1/objects at CL=ALL with 1 dead
			// replica also returns 2xx.  If unconditional returns 4xx/5xx this is a
			// regression in the base write path - unrelated to conditional writes.
			name: "UnconditionalInsert_ExplicitALL_1ReplicaDown_Succeeds_LivePruned",
			id:   "cccccccc-0000-0000-0000-000000000003",
			doInsert: func(t *testing.T, host, className, id, cl string) int {
				return uncondInsertHTTP(t, host, className, id, cl)
			},
			wantMsg: "unconditional POST /v1/objects at CL=ALL with 1 dead-detected " +
				"replica must return 2xx (ALL-of-2-live; dead node pruned). " +
				"Got %d. A >=400 is a regression in the unconditional write path.",
		},
	}

	for _, tc := range insertFns {
		t.Run(tc.name, func(t *testing.T) {
			statusCode := tc.doInsert(t, compose.ContainerURI(1), haTestClassName, tc.id, "ALL")
			require.True(t, statusCode >= 200 && statusCode < 300, tc.wantMsg, statusCode)
		})
	}
}
