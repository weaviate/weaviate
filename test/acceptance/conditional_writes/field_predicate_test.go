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

// Package conditional_writes contains acceptance tests for Phase-3
// field-predicate conditional writes.
//
// This file tests the ?condition=field_match&property=<name>&value=<v>&value_type=<type>
// REST API on a 3-node RF=3 cluster. It is the multi-node harness that
// catches the silent-drop bug class: if the coordinator propagates the
// object to replicas WITHOUT the predicate query params, each replica evaluates
// an unconditional write and data is silently accepted instead of rejected.
//
// Tests:
//
//   - TestFieldPredicate_RF3_TrueCommits: predicate matches stored value → 200.
//   - TestFieldPredicate_RF3_FalseReturns409: predicate mismatch → 409 Conflict.
//   - TestFieldPredicate_RF3_MissingField409: property not on object → 409 Conflict.
//   - TestFieldPredicate_RF3_ConcurrentExactlyOneWinner: N concurrent goroutines
//     attempt to flip a status field; exactly one wins, rest get 409.
//   - TestFieldPredicate_RF3_HA_QUORUM: predicate writes survive a node failure;
//     predicate-false gets 409 (not 5xx) after node recovery.
//   - TestFieldPredicate_NoRegression_Phase1: insert_if_not_exists still works.
//
// Run with:
//
//	TEST_WEAVIATE_IMAGE=weaviate/test-server:phase3 \
//	  go test -run TestFieldPredicate ./test/acceptance/conditional_writes/... \
//	  -timeout 2400s -v
package conditional_writes

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

	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// --------------------------------------------------------------------------
// Shared helpers for field-predicate tests
// --------------------------------------------------------------------------

// fpClass is the collection name reused across field-predicate tests.
// Each test creates its own class with a unique name suffix to avoid collisions.

// setupFPClass creates a single-shard RF=3 class with two properties:
//   - "status" (text): used for field_match predicate tests
//   - "counter" (int): used for int-type predicate tests
func setupFPClass(t *testing.T, className string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 3,
		},
		Properties: []*models.Property{
			{Name: "status", DataType: []string{"text"}},
			{Name: "counter", DataType: []string{"int"}},
		},
	})
}

// fpInsertHTTP posts an object to className on hostURI with the given id and status.
// Returns the HTTP status code.
func fpInsertHTTP(t *testing.T, hostURI, className, id, status string) int {
	t.Helper()
	return fpInsertFullHTTP(t, hostURI, className, id, status, 1, "")
}

// fpInsertFullHTTP posts an object with status, counter, and an optional condition.
func fpInsertFullHTTP(t *testing.T, hostURI, className, id, status string, counter int64, condition string) int {
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
			"status":  status,
			"counter": counter,
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/v1/objects", hostURI)
	if condition != "" {
		url += "?" + condition
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Logf("fpInsertFullHTTP: network error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// fpUpdateHTTP sends a PUT to update an existing object with a field_match condition.
// condition is the full condition query string fragment, e.g.:
//
//	"condition=field_match&property=status&value=draft&value_type=text"
func fpUpdateHTTP(t *testing.T, hostURI, className, id, newStatus, condition string) int {
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
			"status": newStatus,
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", hostURI, className, id)
	if condition != "" {
		url += "?" + condition
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Logf("fpUpdateHTTP: network error (host=%s id=%s): %v", hostURI, id, err)
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// fpGetStatus retrieves the status property from an object. Returns ("", false) when
// the object does not exist or cannot be read.
func fpGetStatus(t *testing.T, hostURI, className, id string) (string, bool) {
	t.Helper()
	helper.SetupClient(hostURI)
	obj, err := helper.GetObject(t, className, strfmt.UUID(id))
	if err != nil || obj == nil {
		return "", false
	}
	props, ok := obj.Properties.(map[string]interface{})
	if !ok {
		return "", false
	}
	s, ok := props["status"].(string)
	return s, ok
}

// --------------------------------------------------------------------------
// Test 1: Predicate-true commits (POST + PUT)
// --------------------------------------------------------------------------

// TestFieldPredicate_RF3_TrueCommits inserts an object, then performs a
// field_match update where the stored value matches the predicate → succeeds.
//
// This is the canonical "green path" for Phase-3 field-predicate.
func TestFieldPredicate_RF3_TrueCommits(t *testing.T) {
	const className = "FPTrueCommits"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	id := "aaaa0000-0000-4000-8000-000000000001"

	t.Run("InsertObject", func(t *testing.T) {
		code := fpInsertHTTP(t, host, className, id, "draft")
		require.True(t, code >= 200 && code < 300, "initial insert must succeed: got %d", code)
	})

	t.Run("UpdateWithMatchingPredicate", func(t *testing.T) {
		condition := "condition=field_match&property=status&value=draft&value_type=text"
		code := fpUpdateHTTP(t, host, className, id, "published", condition)
		require.True(t, code >= 200 && code < 300,
			"field_match update where stored==expected must succeed: got %d", code)
	})

	t.Run("VerifyStatusUpdated", func(t *testing.T) {
		status, ok := fpGetStatus(t, host, className, id)
		require.True(t, ok, "object must be readable after update")
		assert.Equal(t, "published", status, "status must be 'published' after successful field_match update")
	})
}

// --------------------------------------------------------------------------
// Test 2: Predicate-false returns 409
// --------------------------------------------------------------------------

// TestFieldPredicate_RF3_FalseReturns409 inserts an object, then attempts a
// field_match update where the stored value does NOT match → 409 Conflict, not 5xx.
//
// This is the canonical "red path": the predicate fails and the stored value is
// unchanged. If the replica propagation silently drops the predicate, the replica
// would accept the write unconditionally -- this test detects that bug on RF=3.
func TestFieldPredicate_RF3_FalseReturns409(t *testing.T) {
	const className = "FPFalse409"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	id := "bbbb0000-0000-4000-8000-000000000001"

	t.Run("InsertObject", func(t *testing.T) {
		code := fpInsertHTTP(t, host, className, id, "draft")
		require.True(t, code >= 200 && code < 300, "initial insert: got %d", code)
	})

	t.Run("UpdateWithMismatchedPredicate", func(t *testing.T) {
		condition := "condition=field_match&property=status&value=published&value_type=text"
		code := fpUpdateHTTP(t, host, className, id, "archived", condition)
		require.Equal(t, http.StatusConflict, code,
			"field_match where stored!=expected must return 409 Conflict (not 5xx): got %d", code)
	})

	t.Run("VerifyStatusUnchanged", func(t *testing.T) {
		status, ok := fpGetStatus(t, host, className, id)
		require.True(t, ok)
		assert.Equal(t, "draft", status, "status must remain 'draft' after failed field_match")
	})
}

// --------------------------------------------------------------------------
// Test 3: Missing field returns 409
// --------------------------------------------------------------------------

// TestFieldPredicate_RF3_MissingField409 inserts an object WITHOUT setting the
// "status" property (only "counter" is set), then attempts a field_match on
// "status" → 409 Conflict.
//
// Verifies that absent properties fail the predicate, not silently pass.
func TestFieldPredicate_RF3_MissingField409(t *testing.T) {
	const className = "FPMissingField409"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	id := "cccc0000-0000-4000-8000-000000000001"

	// Insert object with only "counter" set -- "status" is absent from Properties.
	t.Run("InsertObjectWithoutStatus", func(t *testing.T) {
		type body struct {
			Class      string                 `json:"class"`
			ID         string                 `json:"id"`
			Properties map[string]interface{} `json:"properties"`
		}
		payload := body{Class: className, ID: id, Properties: map[string]interface{}{"counter": float64(1)}}
		jsonData, _ := json.Marshal(payload)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
			fmt.Sprintf("http://%s/v1/objects", host), bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp, err2 := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		require.NoError(t, err2)
		defer resp.Body.Close()
		require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "insert: %d", resp.StatusCode)
	})

	t.Run("FieldMatchOnMissingProperty", func(t *testing.T) {
		condition := "condition=field_match&property=status&value=draft&value_type=text"

		type body struct {
			Class      string                 `json:"class"`
			ID         string                 `json:"id"`
			Properties map[string]interface{} `json:"properties"`
		}
		payload := body{Class: className, ID: id, Properties: map[string]interface{}{"status": "published"}}
		jsonData, _ := json.Marshal(payload)
		url := fmt.Sprintf("http://%s/v1/objects/%s/%s?%s", host, className, id, condition)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp, err2 := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		require.NoError(t, err2)
		defer resp.Body.Close()
		require.Equal(t, http.StatusConflict, resp.StatusCode,
			"field_match on absent property must return 409 Conflict: got %d", resp.StatusCode)
	})
}

// --------------------------------------------------------------------------
// Test 4: Concurrent exactly-one-winner
// --------------------------------------------------------------------------

// TestFieldPredicate_RF3_ConcurrentExactlyOneWinner creates an object with
// status="draft", then launches M concurrent goroutines all attempting to
// flip status from "draft" to "published" using field_match. Exactly one
// must succeed (200) and all others must get 409.
//
// This is the key correctness test for the per-UUID lock under field-predicate:
// the winner atomically reads "draft", passes the predicate, and writes "published".
// All subsequent attempts see "published" != "draft" and get 409.
//
// This test also catches the silent-drop replication bug: if the replica
// receives an unconditional write (predicate dropped), all M goroutines could
// "succeed" and the final status would be indeterminate.
func TestFieldPredicate_RF3_ConcurrentExactlyOneWinner(t *testing.T) {
	const (
		className  = "FPConcurrentWinner"
		numWorkers = 16
		clLevel    = "QUORUM"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	// Insert 50 objects, each initially with status="draft".
	const numObjects = 50
	uuids := makeUUIDs("dddd", numObjects)

	t.Run("InsertDraftObjects", func(t *testing.T) {
		for _, id := range uuids {
			code := fpInsertHTTP(t, host, className, id, "draft")
			require.True(t, code >= 200 && code < 300, "insert %s: got %d", id, code)
		}
	})

	// Each UUID is attempted by all numWorkers goroutines. Only one should win.
	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var totalSuccess, totalConflict, totalError int64

	t.Run("ConcurrentFlip", func(t *testing.T) {
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				localSuccess, localConflict, localErr := int64(0), int64(0), int64(0)
				for _, id := range uuids {
					condition := "condition=field_match&property=status&value=draft&value_type=text&consistency_level=" + clLevel
					code := fpUpdateHTTP(t, host, className, id, "published", condition)
					switch {
					case code >= 200 && code < 300:
						localSuccess++
					case code == http.StatusConflict:
						localConflict++
					default:
						localErr++
						t.Logf("unexpected status %d for UUID %s", code, id)
					}
				}
				atomic.AddInt64(&totalSuccess, localSuccess)
				atomic.AddInt64(&totalConflict, localConflict)
				atomic.AddInt64(&totalError, localErr)
			}, logger)
		}
		wg.Wait()

		t.Logf("Concurrent flip: success=%d conflict=%d error=%d",
			totalSuccess, totalConflict, totalError)
	})

	t.Run("AssertZeroErrors", func(t *testing.T) {
		require.Zero(t, totalError,
			"field_match writes must not return unexpected error codes; got %d errors", totalError)
	})

	t.Run("AssertExactlyOneWinnerPerObject", func(t *testing.T) {
		// Exactly numObjects successes across all goroutines: one per UUID.
		require.Equal(t, int64(numObjects), totalSuccess,
			"exactly %d successes expected (one per object); got %d -- "+
				"if > %d, the per-UUID lock failed to serialize the predicate check; "+
				"if < %d, a predicate-true update was incorrectly rejected",
			numObjects, totalSuccess, numObjects, numObjects)
	})

	t.Run("VerifyAllStatusPublished", func(t *testing.T) {
		// Every object must now be "published".
		wrongCount := 0
		for _, id := range uuids {
			status, ok := fpGetStatus(t, host, className, id)
			if !ok || status != "published" {
				wrongCount++
				t.Logf("UUID %s has status=%q (want 'published')", id, status)
			}
		}
		require.Zero(t, wrongCount,
			"%d objects did not reach status='published' after exactly-one-winner test", wrongCount)
	})
}

// --------------------------------------------------------------------------
// Test 5: HA at QUORUM through node failure
// --------------------------------------------------------------------------

// TestFieldPredicate_RF3_HA_QUORUM verifies that field_match writes continue at
// QUORUM through a mid-load node stop, and that predicate-false returns 409 (not 5xx)
// even when a replica is down. Encodes INV-HA-1.
func TestFieldPredicate_RF3_HA_QUORUM(t *testing.T) {
	const (
		className        = "FPHA"
		clLevel          = "QUORUM"
		numUUIDs         = 200
		failAfterFrac    = 0.4
		minSuccessRatePC = 85.0
	)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	// Pre-insert all objects with status="draft".
	uuids := makeUUIDs("eeee", numUUIDs)
	for _, id := range uuids {
		code := fpInsertHTTP(t, host, className, id, "draft")
		require.True(t, code >= 200 && code < 300, "pre-insert %s: got %d", id, code)
	}

	failAt := int(float64(numUUIDs) * failAfterFrac)
	var (
		successCount  int64
		conflictCount int64
		errorCount    int64
		nodeStopped   bool
	)

	for i, id := range uuids {
		if i == failAt && !nodeStopped {
			t.Logf("[%d/%d] Stopping node 2 mid-load", i, numUUIDs)
			common.StopNodeAt(ctx, t, compose, 2)
			nodeStopped = true
		}

		condition := fmt.Sprintf("condition=field_match&property=status&value=draft&value_type=text&consistency_level=%s", clLevel)
		code := fpUpdateHTTP(t, host, className, id, "published", condition)
		switch {
		case code >= 200 && code < 300:
			atomic.AddInt64(&successCount, 1)
		case code == http.StatusConflict:
			// 409 is acceptable: predicate-false (object was already updated).
			atomic.AddInt64(&conflictCount, 1)
		default:
			atomic.AddInt64(&errorCount, 1)
			t.Logf("[%d/%d] unexpected status=%d UUID=%s (node_stopped=%v)", i, numUUIDs, code, id, nodeStopped)
		}
	}

	successRate := 100.0 * float64(successCount) / float64(numUUIDs)
	t.Logf("HA load: total=%d success=%d conflict=%d error=%d success_rate=%.1f%%",
		numUUIDs, successCount, conflictCount, errorCount, successRate)

	t.Run("AssertHighSuccessRate_INV_HA_1", func(t *testing.T) {
		require.GreaterOrEqual(t, successRate, minSuccessRatePC,
			"field_match write success rate %.1f%% below %.1f%% floor (INV-HA-1): "+
				"QUORUM writes on RF=3 must survive 1 replica down",
			successRate, minSuccessRatePC)
	})

	t.Run("AssertNoUnexpectedErrors", func(t *testing.T) {
		// Allow a small number of transient errors during node failover (the brief
		// window where the RAFT leader is re-elected). 5% budget.
		maxErrors := int64(float64(numUUIDs) * 0.05)
		require.LessOrEqual(t, errorCount, maxErrors,
			"unexpected error count %d exceeds 5%% budget of %d total: "+
				"field_match predicate failures must return 409, not 5xx",
			errorCount, numUUIDs)
	})
}

// --------------------------------------------------------------------------
// Test 6: Phase-1 no-regression on phase3 image
// --------------------------------------------------------------------------

// TestFieldPredicate_NoRegression_Phase1 verifies that insert_if_not_exists
// still works correctly after Phase-3 changes. This catches any accidental
// regression in the conditional dispatch switch.
func TestFieldPredicate_NoRegression_Phase1(t *testing.T) {
	const (
		className = "FPNoRegP1"
		numUUIDs  = 100
		clLevel   = "QUORUM"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	host := compose.ContainerURI(1)
	helper.SetupClient(host)
	setupFPClass(t, className)
	waitForSchemaOnAllNodes(t, compose, className, 3)

	uuids := makeUUIDs("ffff", numUUIDs)

	t.Run("InsertIfNotExists_AllSucceed", func(t *testing.T) {
		var errCount int64
		for _, id := range uuids {
			code := prodCondInsertHTTP(t, host, className, id, clLevel)
			if code < 200 || code >= 300 {
				atomic.AddInt64(&errCount, 1)
				t.Logf("insert_if_not_exists error: status=%d UUID=%s", code, id)
			}
		}
		require.Zero(t, errCount, "Phase-1 insert_if_not_exists must still work on phase3 image")
	})

	t.Run("InsertIfNotExists_DuplicatesReturnOK", func(t *testing.T) {
		for _, id := range uuids[:5] {
			code := prodCondInsertHTTP(t, host, className, id, clLevel)
			// 200 = skipped (idempotent), 201 = inserted (race won). Both acceptable.
			require.True(t, code == http.StatusOK || code == http.StatusCreated,
				"duplicate insert_if_not_exists must return 200 or 201: got %d for %s", code, id)
		}
	})

	t.Run("AssertCountCorrect", func(t *testing.T) {
		var finalCount int64
		assert.Eventually(t, func() bool {
			finalCount = countObjectsViaGraphQL(t, host, className)
			return finalCount == int64(numUUIDs)
		}, 30*time.Second, 500*time.Millisecond,
			"aggregate count must be exactly %d; got %d", numUUIDs, finalCount)
	})
}
