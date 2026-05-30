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

// Package conditional_writes B1 gate-closed acceptance test.
//
// This file tests the GATE-CLOSED state (PERSISTENCE_OBJECT_VERSION_WRITE != 2,
// the default installation). In this state:
//
//   - Unconditional writes must succeed (no regression on the base path).
//   - Conditional version-CAS writes (If-Match / update_if_version) must return
//     a clear error indicating that version-CAS is not active, NOT silently pass
//     as unconditional writes (a silent pass is a lost-update vector).
//   - Version stored on objects must be 0 (not persisted in v1 binary).
//
// Run with:
//
//	TEST_WEAVIATE_IMAGE=weaviate/test-server:phase2 \
//	  go test -run TestGateClosed ./test/acceptance/conditional_writes/... \
//	  -timeout 600s -v
package conditional_writes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestGateClosed_ConditionalWriteReturnsNotActiveError boots a 3-node cluster
// WITHOUT PERSISTENCE_OBJECT_VERSION_WRITE=2 (gate closed, the default).
//
// Assertions:
//  1. Unconditional writes succeed (no regression on the default path).
//  2. A version-CAS PUT (If-Match header) returns a non-200 status with a body
//     that contains "version-CAS not active" - never silently succeeds as
//     unconditional.
//  3. Object version remains 0 after multiple writes (not persisted in v1 binary).
//
// Causal link: before the B1 fix, PutObject had no gate-closed guard. A
// client-side conditional write would proceed through the coordinator's lock,
// compare versions, and "succeed" because both sides were 0 (no version
// persisted). The client received a 200 thinking the conditional was evaluated;
// in reality the write was unconditional. This test catches that regression by
// asserting the server returns an error for If-Match on a gate-closed cluster.
func TestGateClosed_ConditionalWriteReturnsNotActiveError(t *testing.T) {
	const (
		className = "GateClosedCASTest"
		objectID  = "cc000000-0000-4000-8000-000000000001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start cluster WITHOUT PERSISTENCE_OBJECT_VERSION_WRITE=2 (gate closed).
	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(ctx)
	require.NoError(t, err, "start 3-node RF3 cluster (gate closed)")
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

	t.Run("UnconditionalWriteSucceeds", func(t *testing.T) {
		// Unconditional PUT must succeed on a gate-closed cluster.
		code := unconditionalPutHTTP(t, host, className, objectID, "initial-value")
		assert.True(t, code >= 200 && code < 300,
			"unconditional PUT must succeed on gate-closed cluster; got status %d", code)
	})

	t.Run("GateClosed_VersionIsZero", func(t *testing.T) {
		// Object version must be 0 (not persisted in v1 binary).
		version := versionCASGetVersionDirect(t, host, className, objectID)
		assert.Equal(t, uint64(0), version,
			"gate-closed: object version must be 0 (not persisted in v1 binary)")
	})

	t.Run("GateClosed_IfMatchReturnsNotActiveError", func(t *testing.T) {
		// Conditional PUT (If-Match: "0") must return a non-200 response with a
		// clear "version-CAS not active" message.
		result := versionCASPutHTTP(t, host, className, objectID, 0, "conditional-attempt")

		assert.NotEqual(t, http.StatusOK, result.StatusCode,
			"gate-closed conditional PUT must NOT return 200 (would be a silent unconditional write); "+
				"got status %d", result.StatusCode)

		// 422 (Unprocessable Entity) or 400 (Bad Request) are both acceptable;
		// the key is the error body contains the diagnostic message.
		// We do NOT assert a specific 4xx code because the REST layer maps errors
		// differently, but we do verify the response body.
		statusBody := gateCASErrorBody(t, host, className, objectID, 0)
		assert.True(t,
			strings.Contains(strings.ToLower(statusBody), "version-cas not active") ||
				strings.Contains(strings.ToLower(statusBody), "versioning disabled") ||
				strings.Contains(strings.ToLower(statusBody), "object versioning"),
			"gate-closed error body must contain a 'version-CAS not active' diagnostic; got: %q",
			statusBody)
	})

	t.Run("GateClosed_PhaseOneConditionalsStillWork", func(t *testing.T) {
		// Phase-1 OnlyIfNotExists must still work on a gate-closed cluster.
		// This verifies the gate guard does not block non-version conditionals.
		code := prodCondInsertHTTP(t, host, className, objectID, "QUORUM")
		// The object already exists, so insert_if_not_exists must return 409 or 412.
		assert.True(t, code == http.StatusConflict || code == http.StatusPreconditionFailed,
			"gate-closed: OnlyIfNotExists on existing object must return 409 or 412; got %d", code)
	})
}

// unconditionalPutHTTP issues an unconditional PUT (no If-Match header).
func unconditionalPutHTTP(t *testing.T, hostURI, className, id, fieldValue string) int {
	t.Helper()

	type putBody struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := putBody{
		Class: className, ID: id,
		Properties: map[string]interface{}{"testfield": fieldValue},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", hostURI, className, id)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// gateCASErrorBody issues a conditional PUT and returns the raw response body
// as a string (for asserting the error message).
func gateCASErrorBody(t *testing.T, hostURI, className, id string, ifVersion uint64, fieldValues ...string) string {
	t.Helper()

	fieldValue := "gate-closed-probe"
	if len(fieldValues) > 0 {
		fieldValue = fieldValues[0]
	}

	type putBody struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := putBody{
		Class: className, ID: id,
		Properties: map[string]interface{}{"testfield": fieldValue},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", hostURI, className, id)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", fmt.Sprintf(`"%d"`, ifVersion))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}
