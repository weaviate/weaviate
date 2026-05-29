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

	// Stop the third node (0-based index 2): cluster is at N-1 = 2/3 replicas.
	// QUORUM for RF=3 is ceil((3+1)/2)=2 replicas, so writes must still succeed.
	// StopAt is 0-based; valid indices for a 3-node cluster are 0, 1, 2.
	t.Run("KillThirdNode", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
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

// TestHAPreservationAllCL verifies the actual CL=ALL semantics in weaviate:
//
//	CL=ALL means "all currently-live replicas as known by memberlist", NOT
//	"all RF replicas in the schema."  When a node is detected dead,
//	buildReplicas (cluster/router/router.go:159-175) skips it because
//	NodeHostname returns ok==false for memberlist-dead nodes.  StopNodeAt
//	polls /v1/nodes until the dead node is gone before returning, so by
//	write-time the dead node is already pruned from the broadcast set.
//	ToInt(n) then sees n=2 (the two live nodes), CL=ALL resolves to integer
//	2, both surviving nodes reply, and the write returns 2xx.
//
//	Therefore: conditional insert at CL=ALL with 1 node down correctly
//	returns 2xx (ALL-of-2-live), identical to QUORUM behaviour and identical
//	to an unconditional write — because conditional inherits CL exactly
//	(v4 § 5.4 CL-inheritance principle).
//
//	See TestHAPreservationAllCL_UnconditionalParity for the baseline that
//	proves conditional and unconditional paths stay in lockstep.
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

	// Stop the third node (0-based index 2): cluster is at N-1 = 2/3 replicas.
	// StopNodeAt polls /v1/nodes until the killed node is no longer reported
	// HEALTHY, ensuring memberlist failure-detection has completed before the
	// subsequent write runs.  This makes the pruning deterministic, not a race.
	// StopAt is 0-based; valid indices for a 3-node cluster are 0, 1, 2.
	t.Run("KillThirdNode", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// --- AC 3: insert_if_not_exists at explicit CL=ALL with 1 replica down returns 2xx ---
	//
	// CL=ALL = all LIVE (memberlist) replicas, not all RF replicas.
	// After StopNodeAt completes, the dead node is pruned from the write-replica
	// slice by buildReplicas (cluster/router/router.go:159-175): NodeHostname
	// returns ok==false for memberlist-dead nodes and they are skipped.
	// ToInt(n) then receives n=2 (the two surviving live nodes), so CL=ALL
	// resolves to integer 2.  Both surviving nodes reply successfully and the
	// write returns 2xx.
	//
	// This is identical to unconditional CL=ALL behaviour (see
	// TestHAPreservationAllCL_UnconditionalParity) and identical to QUORUM
	// behaviour in this 2-live-of-3 scenario.  Conditional inherits the
	// request CL exactly — the core v4 CL-inheritance principle (§ 5.4).
	t.Run("ConditionalInsert_ExplicitALL_1ReplicaDown_Succeeds_LivePruned", func(t *testing.T) {
		id := strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000002")
		statusCode := conditionalInsertHTTP(t, compose.ContainerURI(1), id, "ALL")

		// 2xx: either 201 (object inserted) or 200 (object already existed, skipped).
		// CL=ALL with a dead-detected node resolves to ALL-of-2-live, which
		// both surviving nodes satisfy.  A >=400 here would mean either (a)
		// the dead-node pruning did not happen (memberlist bug) or (b) the
		// conditional path is incorrectly forcing RF=3 for CL resolution
		// instead of inheriting the router's live-replica count.
		require.True(t, statusCode >= 200 && statusCode < 300,
			"insert_if_not_exists at explicit CL=ALL with 1 dead-detected replica "+
				"must return 2xx (ALL-of-2-live succeeds; dead node pruned by "+
				"buildReplicas at cluster/router/router.go:159-175). Got %d.",
			statusCode)
	})
}

// unconditionalInsertHTTP posts a plain (non-conditional) object insert to the
// given host at the specified consistency level and returns the HTTP status
// code.  Unlike conditionalInsertHTTP, the URL does NOT include the
// ?condition= parameter, so the server processes this as a normal write.
func unconditionalInsertHTTP(
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
	require.NoError(t, err, "marshal unconditional insert request")

	url := fmt.Sprintf("http://%s/v1/objects", hostURI)
	if consistencyLevel != "" {
		url = fmt.Sprintf("%s?consistency_level=%s", url, consistencyLevel)
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

// TestHAPreservationAllCL_UnconditionalParity establishes the unconditional
// baseline: a plain POST /v1/objects at CL=ALL with 1 dead-detected replica
// also returns 2xx.
//
// This is the regression guard that ensures the conditional write path
// (TestHAPreservationAllCL) stays in lockstep with the unconditional path.
// If unconditional returns 2xx and conditional returns 4xx/5xx, the
// conditional path is incorrectly overriding the router's CL resolution —
// exactly the class of bug the v4 CL-inheritance principle (§ 5.4) prevents.
//
// Evidence that unconditional CL=ALL-with-1-down returns 2xx:
//   - buildReplicas (cluster/router/router.go:159-175) prunes dead nodes
//   - NodeHostname (usecases/cluster/state.go:419-427) returns ok==false for
//     nodes absent from memberlist.Members() (which excludes DeadOrLeft nodes)
//   - StopNodeAt waits for failure-detection before returning, making this
//     deterministic (not a race)
func TestHAPreservationAllCL_UnconditionalParity(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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

	// Kill the third node and wait for memberlist to detect it as dead.
	// StopNodeAt polls /v1/nodes until the node is gone; by the time it
	// returns the dead node is absent from memberlist.Members() and will be
	// pruned by buildReplicas before CL integer resolution.
	t.Run("KillThirdNode", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Unconditional POST /v1/objects?consistency_level=ALL — no ?condition=.
	// Expected: 2xx.  The dead node is pruned; CL=ALL resolves to 2-of-2-live;
	// both surviving nodes reply and the write succeeds.
	// A >=400 here would be a product regression in the unconditional write
	// path — unrelated to conditional writes.
	t.Run("UnconditionalInsert_ExplicitALL_1ReplicaDown_Succeeds_LivePruned", func(t *testing.T) {
		id := strfmt.UUID("cccccccc-0000-0000-0000-000000000003")
		statusCode := unconditionalInsertHTTP(t, compose.ContainerURI(1), id, "ALL")

		require.True(t, statusCode >= 200 && statusCode < 300,
			"unconditional POST /v1/objects at CL=ALL with 1 dead-detected "+
				"replica must return 2xx (ALL-of-2-live; dead node pruned). "+
				"Got %d. A >=400 is a regression in the unconditional write path.",
			statusCode)
	})
}
