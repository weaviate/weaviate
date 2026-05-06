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
	"time"

	"github.com/google/uuid"
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
//
// The runtime-override scenario lives in its own test function because
// it needs a different container configuration (no env-var limit set,
// then a YAML override written mid-flight).
//
// `PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS=1` is set so the
// async object-count path picks up freshly-inserted objects within a
// few seconds; the implementation uses CountAsync (no memtable) and
// the RFC documents the bounded overshoot. waitForFlush in the
// object-limit sub-tests gives the cycle manager time to flush.
func TestAllLimits_SingleSharedContainer(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	// One container, every limit set tight enough to hit, and a custom
	// message template that exercises both placeholders. Collection cap
	// is 4 to leave room for the MT collection used in the tenant
	// sub-test (1 MT + 3 single-tenant = 4 = at cap; a 5th create
	// rejects).
	compose, terminate := startContainer(t, ctx, mergeEnv(aggressiveFlushEnv(), map[string]string{
		"MAXIMUM_ALLOWED_OBJECTS_COUNT":          "10",
		"MAXIMUM_ALLOWED_COLLECTIONS_COUNT":      "4",
		"MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION": "2",
		"MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION":  "1",
		"USAGE_LIMITS_ERROR_MESSAGE":             "hit limit of {value} {limit}, upgrade at https://x",
	}))
	defer terminate()

	httpURI := "http://" + compose.GetWeaviate().URI()
	grpcURI := compose.GetWeaviate().GrpcURI()

	// Set up the MT collection up front so the tenant sub-test can run
	// without consuming a "collection" slot mid-suite. It counts toward
	// MAXIMUM_ALLOWED_COLLECTIONS_COUNT (4).
	createMTCollection(t, ctx, httpURI, "MTcoll")

	t.Run("shard limit on class create", func(t *testing.T) {
		body := []byte(`{
			"class":"BeyondShardCap",
			"vectorizer":"none",
			"shardingConfig":{"desiredCount":4}
		}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/schema", body, "shards", 1)
	})

	t.Run("collection limit hit after cap", func(t *testing.T) {
		// MTcoll already counts as 1; create 3 more (4 total = cap),
		// then the 5th must 429.
		for _, name := range []string{"C1", "C2", "C3"} {
			createCollection(t, ctx, httpURI, name)
		}
		body := []byte(`{"class":"C4","vectorizer":"none"}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/schema", body, "collections", 4)
	})

	t.Run("tenant limit per collection", func(t *testing.T) {
		// Cap is 2 — first two tenants succeed, third rejected.
		addTenants(t, ctx, httpURI, "MTcoll", []string{"T1", "T2"}, false, "")
		addTenants(t, ctx, httpURI, "MTcoll", []string{"T3"}, true, "tenants")
	})

	t.Run("object limit single create — loops until limit fires", func(t *testing.T) {
		// We don't pin the exact count at which the limit fires: the
		// implementation uses an async count that lags the memtable
		// (RFC "Accepted imperfections" — sync count would be too
		// expensive at scale). Instead, loop-insert and assert the
		// limit fires *eventually* with the canonical 429 body. The
		// container is configured with aggressive flush settings so
		// "eventually" is bounded to a few seconds beyond the cap.
		gotLimit := loopInsertUntilLimit(t, ctx, httpURI, "C1", "objects", 10)
		require.True(t, gotLimit,
			"expected the object cap to fire eventually within the suite timeout")
	})

	t.Run("object limit whole-batch rejection (REST)", func(t *testing.T) {
		// Already over the cap from the previous sub-test, so any batch
		// must reject. Whole-batch rejection means every object in the
		// reply is gone — verified by sending one extra single insert
		// after and asserting it ALSO 429s (count didn't grow).
		body := []byte(`{"objects":[
			{"class":"C1","properties":{"i":100}},
			{"class":"C1","properties":{"i":101}},
			{"class":"C1","properties":{"i":102}},
			{"class":"C1","properties":{"i":103}},
			{"class":"C1","properties":{"i":104}}
		]}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/batch/objects", body, "objects", 10)

		single := []byte(`{"class":"C1","properties":{"i":200}}`)
		assertLimitExceeded(t, ctx, httpURI+"/v1/objects", single, "objects", 10)
	})

	t.Run("object limit gRPC batch maps to RESOURCE_EXHAUSTED", func(t *testing.T) {
		conn, err := grpc.NewClient(grpcURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		// gRPC BatchObjects parses each pb.BatchObject upfront; objects
		// missing required fields (e.g. Uuid) get filtered as "parse
		// errors" *before* the limit gate, leaving the batch empty so
		// the handler short-circuits with a successful empty reply
		// (handlers/grpc/v1/batch/handler.go:97-109). Send well-formed
		// objects so the limit gate is the actual gate exercised.
		client := pb.NewWeaviateClient(conn)
		req := &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				{Collection: "C1", Uuid: uuid.NewString()},
				{Collection: "C1", Uuid: uuid.NewString()},
			},
		}
		_, err = client.BatchObjects(ctx, req)
		require.Error(t, err, "gRPC batch should reject when over the object cap")
		st, ok := status.FromError(err)
		require.True(t, ok, "expected gRPC status, got %T: %v", err, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code(),
			"expected codes.ResourceExhausted, got %v: %s", st.Code(), st.Message())
	})
}

// aggressiveFlushEnv returns env vars that push every memtable flush
// trigger to its minimum so the async object-count path catches up to
// recent writes within seconds rather than the 15-200s default windows.
// Required by the object-limit acceptance scenarios because the
// production hot-path uses CountAsync (no memtable, by design — see
// RFC's "Accepted imperfections"); without these the per-test wait
// for "the count to reflect new objects" would dominate runtime.
func aggressiveFlushEnv() map[string]string {
	return map[string]string{
		"PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS":   "1",
		"PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS":    "1",
		"PERSISTENCE_MEMTABLES_MIN_ACTIVE_DURATION_SECONDS": "1",
		"PERSISTENCE_MEMTABLES_MAX_ACTIVE_DURATION_SECONDS": "2",
	}
}

// mergeEnv overlays b on top of a, returning a fresh map. Used to
// combine the aggressive-flush base with the per-test limit env vars
// without mutating the shared base.
func mergeEnv(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// loopInsertUntilLimit hammers the single-object create endpoint until
// the configured object-limit fires (HTTP 429 with USAGE_LIMIT_EXCEEDED)
// or a generous deadline elapses. Returns true if the limit fired
// before the deadline.
//
// The async object-count path lags writes (RFC: "bounded overshoot,
// self-corrects on flush"), so the limit may not fire at exactly
// `cap+1` — but with aggressive flush settings configured on the
// container, it fires within a few seconds beyond the cap. The test
// asserts only "fires eventually" and checks the structured 429 body
// when it does.
func loopInsertUntilLimit(t *testing.T, ctx context.Context, httpURI, class, expectLimit string, expectValue int64) bool {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	probeURL := httpURI + "/v1/objects"
	for i := 0; time.Now().Before(deadline); i++ {
		body := []byte(fmt.Sprintf(`{"class":"%s","properties":{"i":%d}}`, class, i))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, probeURL, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			// Count hasn't propagated yet; keep going.
			continue
		case http.StatusTooManyRequests:
			// The limit fired — confirm the structured body matches.
			var parsed struct {
				ErrorCode string `json:"errorCode"`
				Limit     string `json:"limit"`
				Value     int64  `json:"value"`
				Message   string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(raw, &parsed),
				"429 response should be JSON: %s", raw)
			assert.Equal(t, "USAGE_LIMIT_EXCEEDED", parsed.ErrorCode)
			assert.Equal(t, expectLimit, parsed.Limit)
			assert.Equal(t, expectValue, parsed.Value)
			assert.NotEmpty(t, parsed.Message)
			return true
		default:
			t.Fatalf("unexpected status %d on insert %d: %s", resp.StatusCode, i, raw)
		}
	}
	return false
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

// createMTCollection creates a multi-tenancy-enabled class. MT must be set
// at create time — flipping it on later via PUT requires the class to be
// empty and is brittle in tests; constructing with multiTenancyConfig
// inline keeps the fixture deterministic.
func createMTCollection(t *testing.T, ctx context.Context, httpURI, name string) {
	t.Helper()
	body := []byte(fmt.Sprintf(
		`{"class":"%s","vectorizer":"none","multiTenancyConfig":{"enabled":true}}`,
		name))
	postOK(t, ctx, httpURI+"/v1/schema", body)
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
