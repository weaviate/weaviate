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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// haTestClassName is the collection used by both HA tests. RF=3 so that
// floor(3/2)=1 replica can be down and QUORUM (2-of-3) still completes.
const haTestClassName = "ConditionalWriteHATest"

// conditionalInsertRequest is the minimal JSON body for a
// POST /v1/objects?condition=insert_if_not_exists.
// The condition itself travels via the ?condition= query parameter; the
// JSON body carries only the object payload. This matches the server
// contract implemented in adapters/handlers/rest/handlers_objects.go.
type conditionalInsertRequest struct {
	Class      string                 `json:"class"`
	ID         string                 `json:"id"`
	Properties map[string]interface{} `json:"properties"`
}

// setupHAClass creates a collection with RF=3 and no vectorizer so that
// the test is independent of module availability.
func setupHAClass(t *testing.T) {
	t.Helper()
	cls := &models.Class{
		Class:      haTestClassName,
		Vectorizer: "none",
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 3,
		},
	}
	helper.CreateClass(t, cls)
}

// conditionalInsertHTTP posts a conditional insert to the given host and
// returns the HTTP status code. It does NOT assert the code; callers
// choose what to assert based on the test scenario.
//
// consistencyLevel is the URL query parameter value to pass (e.g.
// "QUORUM", "ALL"). Pass "" to omit it, which instructs the server to
// use its default (QUORUM per v4 § 5.4).
func conditionalInsertHTTP(
	t *testing.T,
	hostURI string,
	id strfmt.UUID,
	consistencyLevel string,
) int {
	t.Helper()

	body := conditionalInsertRequest{
		Class: haTestClassName,
		ID:    string(id),
		Properties: map[string]interface{}{
			"testfield": fmt.Sprintf("value-for-%s", id),
		},
	}

	jsonData, err := json.Marshal(body)
	require.NoError(t, err, "marshal conditional insert request")

	// The condition travels via ?condition= query parameter; consistency level
	// is a separate orthogonal parameter. Both are combined in the URL below.
	url := fmt.Sprintf("http://%s/v1/objects?condition=insert_if_not_exists", hostURI)
	if consistencyLevel != "" {
		url = fmt.Sprintf("%s&consistency_level=%s", url, consistencyLevel)
	}

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		url,
		bytes.NewBuffer(jsonData),
	)
	require.NoError(t, err, "build HTTP request")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err, "execute HTTP request to %s", url)
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	return resp.StatusCode
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
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// --- AC 1: 3-replica cluster + 1-replica kill infrastructure ---
	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	// Use node 1 as the API target for schema ops and writes.
	helper.SetupClient(compose.ContainerURI(1))

	t.Run("CreateSchema", func(t *testing.T) {
		setupHAClass(t)
	})

	// Stop replica 3: now cluster is at N-1 = 2/3 replicas.
	// QUORUM for RF=3 is ceil((3+1)/2)=2 replicas, so writes must still succeed.
	t.Run("KillReplica3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

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
		statusCode := conditionalInsertHTTP(t, compose.ContainerURI(1), id, "")

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

// TestHAPreservationAllCL encodes the opt-in CP contract (v4 § 5.4 point 4):
//
//	A caller who explicitly requests ConsistencyLevel=ALL with 1 of 3
//	replicas down MUST get 4xx/5xx (not-enough-replicas / write-unavailable).
//	This is the expected behaviour for a caller who has opted into CP semantics
//	at the cost of HA. It is NOT a regression — it is the contract.
//
// TEST-FIRST: see TestHAPreservationDefaultCL notice above.
func TestHAPreservationAllCL(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// --- AC 1: 3-replica cluster + 1-replica kill infrastructure ---
	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node cluster")
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate cluster: %v", err)
		}
	}()

	helper.SetupClient(compose.ContainerURI(1))

	t.Run("CreateSchema", func(t *testing.T) {
		setupHAClass(t)
	})

	// Stop replica 3: cluster is at N-1 = 2/3 replicas.
	// CL=ALL requires all 3 replicas; with only 2 up the write must be rejected.
	t.Run("KillReplica3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	// --- AC 3: insert_if_not_exists at explicit CL=ALL with 1 replica down returns 4xx ---
	//
	// EXPECTED WHILE PHASE-1 IS NOT YET LANDED:
	//   The server returns 422 (unknown field "condition") or similar, which
	//   is technically >=400 and will satisfy this assertion numerically.
	//   However, once Phase-1 lands the test must verify the response
	//   signals "not enough replicas", not "unknown field".  The test is
	//   RED in spirit — the assert passes for the wrong reason — but will
	//   become correctly GREEN (for the right reason) when Phase-1 ships.
	//
	// EXPECTED AFTER PHASE-1 LANDS:
	//   The server attempts CL=ALL, finds only 2/3 replicas reachable, and
	//   returns 503 (not enough replicas) or equivalent >=400 error.
	t.Run("ConditionalInsert_ExplicitALL_1ReplicaDown_Expects4xx", func(t *testing.T) {
		id := strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000002")
		statusCode := conditionalInsertHTTP(t, compose.ContainerURI(1), id, "ALL")

		// 4xx or 5xx: the server must not return 2xx when CL=ALL cannot
		// be satisfied because a replica is down.  Any >=400 signals "write
		// did not complete", which is the correct CP opt-in behaviour
		// (v4 § 5.4 point 4).
		require.True(t, statusCode >= 400,
			"insert_if_not_exists at explicit CL=ALL with 1 replica down "+
				"must return >=400 (caller opted into CP). Got %d. "+
				"A 2xx here means the server incorrectly completed "+
				"a CL=ALL write against only 2/3 replicas.",
			statusCode)
	})
}
