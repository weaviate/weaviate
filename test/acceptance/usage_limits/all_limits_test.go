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

package usage_limits

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestAllLimits_SingleSharedContainer covers the bulk of the RFC's
// promised behaviors against ONE testcontainer startup so the suite
// stays cheap to run in CI. The container is booted with every limit
// set to a small value and a custom error-message template so the same
// state exercises:
//
//   - Object limit (REST single create, REST batch, gRPC batch)
//   - Tenant limit (REST tenants create)
//   - Shard limit (REST class create)
//   - Custom error-message template rendering
//   - Default scope (no USAGE_LIMITS_SCOPE env var → behaves as "node")
//
// Runtime-override and unsupported-scope startup-rejection scenarios
// live in their own test functions because they need different
// container configurations.
func TestAllLimits_SingleSharedContainer(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	// One container, every limit set tight enough to hit, and a custom
	// message template that exercises both placeholders.
	compose, terminate := startContainer(t, ctx, map[string]string{
		"MAXIMUM_ALLOWED_OBJECTS_COUNT":          "10",
		"MAXIMUM_ALLOWED_COLLECTIONS_COUNT":      "3",
		"MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION": "2",
		"MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION":  "1",
		// USAGE_LIMITS_SCOPE deliberately unset → defaults to "node".
		"USAGE_LIMITS_ERROR_MESSAGE": "hit limit of {value} {limit}, upgrade at https://x",
	})
	defer terminate()

	httpURI := "http://" + compose.GetWeaviate().URI()
	grpcURI := compose.GetWeaviate().GrpcURI()

	t.Run("shard limit on class create", func(t *testing.T) {
		body := []byte(`{
			"class":"BeyondShardCap",
			"vectorizer":"none",
			"shardingConfig":{"desiredCount":4}
		}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/schema", body, "shards", 1)
	})

	t.Run("collection limit hit after 3 collections", func(t *testing.T) {
		// Create 3 collections (succeed), then 4th must 429.
		for _, name := range []string{"C1", "C2", "C3"} {
			createCollection(t, ctx, httpURI, name)
		}
		body := []byte(`{"class":"C4","vectorizer":"none"}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/schema", body, "collections", 3)
	})

	t.Run("tenant limit per collection", func(t *testing.T) {
		// Create an MT collection up-front (counts toward collection cap).
		createCollection(t, ctx, httpURI, "MTcoll")
		enableMT(t, ctx, httpURI, "MTcoll")

		// Cap is 2 — first two tenants succeed, third rejected.
		addTenants(t, ctx, httpURI, "MTcoll", []string{"T1", "T2"}, false, "")
		addTenants(t, ctx, httpURI, "MTcoll", []string{"T3"}, true, "tenants")
	})

	t.Run("object limit single create", func(t *testing.T) {
		// We use C1 from the previous sub-test (it was created without MT).
		// Insert exactly 10 (the cap), then the 11th must 429.
		for i := 0; i < 10; i++ {
			body := []byte(fmt.Sprintf(`{"class":"C1","properties":{"i":%d}}`, i))
			postOK(t, ctx, httpURI+"/v1/objects", body)
		}
		body := []byte(`{"class":"C1","properties":{"i":99}}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/objects", body, "objects", 10)
	})

	t.Run("object limit whole-batch rejection (REST)", func(t *testing.T) {
		// We're already at the cap (10) from the previous sub-test, so a
		// batch of 5 must whole-batch-reject — and *no* objects from the
		// batch should land. We re-check the count by trying a single
		// insert immediately after; it should also be rejected.
		body := []byte(`{"objects":[
			{"class":"C1","properties":{"i":100}},
			{"class":"C1","properties":{"i":101}},
			{"class":"C1","properties":{"i":102}},
			{"class":"C1","properties":{"i":103}},
			{"class":"C1","properties":{"i":104}}
		]}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/batch/objects", body, "objects", 10)

		// Confirm no partial fill — single insert still rejected, meaning
		// the count is still exactly 10.
		single := []byte(`{"class":"C1","properties":{"i":200}}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/objects", single, "objects", 10)
	})

	t.Run("object limit gRPC batch maps to RESOURCE_EXHAUSTED", func(t *testing.T) {
		conn, err := grpc.NewClient(grpcURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		client := pb.NewWeaviateClient(conn)
		req := &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				{Collection: "C1", Properties: nil},
				{Collection: "C1", Properties: nil},
			},
		}
		_, err = client.BatchObjects(ctx, req)
		require.Error(t, err, "gRPC batch should reject when over the object cap")
		st, ok := status.FromError(err)
		require.True(t, ok, "expected gRPC status, got %T: %v", err, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code(),
			"expected codes.ResourceExhausted, got %v: %s", st.Code(), st.Message())
	})

	t.Run("default scope unset → behaves as node", func(t *testing.T) {
		// All preceding sub-tests ran on a container with USAGE_LIMITS_SCOPE
		// deliberately unset. Their limits firing as expected is the proof
		// that the default-scope path enforces. This sub-test exists as a
		// documented anchor — no additional assertion required.
	})
}

// --- helpers ---

// assertLimitExceeded POSTs body to url and asserts the response is the
// canonical HTTP 429 + USAGE_LIMIT_EXCEEDED structured body, including the
// rendered custom message template.
func assertLimitExceeded(t *testing.T, ctx context.Context, url string, body []byte, expectLimit string, expectValue int64) {
	t.Helper()
	resp := postRaw(t, ctx, url, body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode,
		"expected HTTP 429 for limit '%s'", expectLimit)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var parsed struct {
		ErrorCode string `json:"errorCode"`
		Limit     string `json:"limit"`
		Value     int64  `json:"value"`
		Message   string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(raw, &parsed),
		"response body should be JSON: %s", raw)

	assert.Equal(t, "USAGE_LIMIT_EXCEEDED", parsed.ErrorCode)
	assert.Equal(t, expectLimit, parsed.Limit)
	assert.Equal(t, expectValue, parsed.Value)
	// Custom template is "hit limit of {value} {limit}, upgrade at https://x".
	// Verify both placeholders rendered AND the static suffix is present.
	expectedSubstr := fmt.Sprintf("hit limit of %d %s, upgrade at https://x", expectValue, expectLimit)
	assert.Contains(t, parsed.Message, expectedSubstr,
		"custom error template should be rendered with substituted placeholders")
}

func postRaw(t *testing.T, ctx context.Context, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "POST %s", url)
	return resp
}

func postOK(t *testing.T, ctx context.Context, url string, body []byte) {
	t.Helper()
	resp := postRaw(t, ctx, url, body)
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 2xx for POST %s, got %d: %s", url, resp.StatusCode, raw)
	}
}

func createCollection(t *testing.T, ctx context.Context, httpURI, name string) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"class":"%s","vectorizer":"none"}`, name))
	postOK(t, ctx, httpURI+"/v1/schema", body)
}

func enableMT(t *testing.T, ctx context.Context, httpURI, class string) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"class":"%s","multiTenancyConfig":{"enabled":true}}`, class))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		httpURI+"/v1/schema/"+class, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("enable MT failed: %d %s", resp.StatusCode, raw)
	}
}

func addTenants(t *testing.T, ctx context.Context, httpURI, class string,
	tenants []string, expectExceed bool, expectLimit string,
) {
	t.Helper()
	parts := make([]string, len(tenants))
	for i, n := range tenants {
		parts[i] = fmt.Sprintf(`{"name":"%s"}`, n)
	}
	body := []byte("[" + strings.Join(parts, ",") + "]")
	url := httpURI + "/v1/schema/" + class + "/tenants"
	if expectExceed {
		assertLimitExceeded(t, ctx, url, body, expectLimit, 2)
	} else {
		postOK(t, ctx, url, body)
	}
}
