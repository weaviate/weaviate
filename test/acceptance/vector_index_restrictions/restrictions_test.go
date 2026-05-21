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

package vector_index_restrictions

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestrictions_SingleSharedContainer covers the bulk of the RFC's
// promised behaviours against ONE testcontainer startup. The container
// is booted with the "common shape" from the brief:
//
//	ALLOWED_VECTOR_INDEX_TYPES=hfresh,hnsw
//	ALLOWED_COMPRESSION_TYPES=rq-8
//	DEFAULT_VECTOR_INDEX=hfresh
//	DEFAULT_QUANTIZATION=rq-8
//	RESTRICTIONS_ERROR_MESSAGE=<custom template>
//
// Sub-tests exercise: disallowed type, disallowed compression on hnsw,
// allowed hnsw+rq-8, hfresh exempt from compression check, and that
// the operator-overridable template renders into the wire `message`.
//
// The fail-to-boot scenario (hfresh-only + compression set) and the
// runtime-override scenario each spin up their own container because
// they need a different startup config.
func TestRestrictions_SingleSharedContainer(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	compose, terminate := startContainer(t, ctx, map[string]string{
		"ALLOWED_VECTOR_INDEX_TYPES": "hfresh,hnsw",
		"ALLOWED_COMPRESSION_TYPES":  "rq-8",
		"DEFAULT_VECTOR_INDEX":       "hfresh",
		"DEFAULT_QUANTIZATION":       "rq-8",
		"RESTRICTIONS_ERROR_MESSAGE": "config rejected: {value} for {restriction}, allowed: {allowed} -- upgrade at https://x",
	})
	defer terminate()
	httpURI := "http://" + compose.GetWeaviate().URI()

	t.Run("flat class is rejected with vector_index_type violation", func(t *testing.T) {
		body := []byte(`{
			"class":"FlatRejected",
			"vectorizer":"none",
			"vectorIndexType":"flat"
		}`)
		v := assertRestrictionViolation(t, ctx, httpURI+"/v1/schema", body, "vector_index_type", "flat")
		assert.Contains(t, v.Allowed, "hfresh")
		assert.Contains(t, v.Allowed, "hnsw")
		// Operator template rendered into wire message.
		assert.Contains(t, v.Message, "config rejected: flat for vector_index_type")
		assert.Contains(t, v.Message, "upgrade at https://x")
	})

	t.Run("hnsw class with pq compression is rejected", func(t *testing.T) {
		body := []byte(`{
			"class":"PqRejected",
			"vectorizer":"none",
			"vectorIndexType":"hnsw",
			"vectorIndexConfig":{"pq":{"enabled":true}}
		}`)
		v := assertRestrictionViolation(t, ctx, httpURI+"/v1/schema", body, "compression", "pq")
		assert.Equal(t, []string{"rq-8"}, v.Allowed)
	})

	t.Run("hnsw class with rq-8 compression succeeds", func(t *testing.T) {
		body := []byte(`{
			"class":"HnswRq8",
			"vectorizer":"none",
			"vectorIndexType":"hnsw",
			"vectorIndexConfig":{"rq":{"enabled":true,"bits":8}}
		}`)
		assertCreateOK(t, ctx, httpURI+"/v1/schema", body)
	})

	t.Run("hnsw class without compression block — default rq-8 applied, succeeds", func(t *testing.T) {
		body := []byte(`{
			"class":"HnswDefaultCompression",
			"vectorizer":"none",
			"vectorIndexType":"hnsw"
		}`)
		assertCreateOK(t, ctx, httpURI+"/v1/schema", body)
	})

	t.Run("hfresh class — compression check skipped, succeeds", func(t *testing.T) {
		// Hfresh classes are exempt from ALLOWED_COMPRESSION_TYPES per the
		// RFC: hfresh has no compression knobs operators can policy on.
		body := []byte(`{
			"class":"HfreshExempt",
			"vectorizer":"none",
			"vectorIndexType":"hfresh"
		}`)
		assertCreateOK(t, ctx, httpURI+"/v1/schema", body)
	})

	t.Run("named-vector with disallowed type is rejected", func(t *testing.T) {
		// Same allow-list applies to vectorConfig entries: a class that
		// passes the legacy gate must still have all of its named vectors
		// in the allow-list.
		body := []byte(`{
			"class":"NamedVectorFlat",
			"vectorConfig":{
				"v1":{
					"vectorizer":{"none":{}},
					"vectorIndexType":"flat"
				}
			}
		}`)
		v := assertRestrictionViolation(t, ctx, httpURI+"/v1/schema", body, "vector_index_type", "flat")
		assert.Contains(t, v.Allowed, "hfresh")
	})

	t.Run("named-vector with disallowed compression is rejected", func(t *testing.T) {
		body := []byte(`{
			"class":"NamedVectorPq",
			"vectorConfig":{
				"v1":{
					"vectorizer":{"none":{}},
					"vectorIndexType":"hnsw",
					"vectorIndexConfig":{"pq":{"enabled":true}}
				}
			}
		}`)
		assertRestrictionViolation(t, ctx, httpURI+"/v1/schema", body, "compression", "pq")
	})

	t.Run("PUT adding a new named-vector with disallowed type — 422", func(t *testing.T) {
		// This is the only scenario where the allow-list is the SOLE
		// gate: the RAFT-side immutable check only iterates
		// initial.VectorConfig, so brand-new entries on the updated
		// side reach the use-case allow-list unfiltered.
		create := []byte(`{
			"class":"PutAddNamedVectorTest",
			"vectorConfig":{
				"v1":{
					"vectorizer":{"none":{}},
					"vectorIndexType":"hnsw",
					"vectorIndexConfig":{"rq":{"enabled":true,"bits":8}}
				}
			}
		}`)
		assertCreateOK(t, ctx, httpURI+"/v1/schema", create)

		update := []byte(`{
			"class":"PutAddNamedVectorTest",
			"vectorConfig":{
				"v1":{
					"vectorizer":{"none":{}},
					"vectorIndexType":"hnsw",
					"vectorIndexConfig":{"rq":{"enabled":true,"bits":8}}
				},
				"v2":{
					"vectorizer":{"none":{}},
					"vectorIndexType":"flat"
				}
			}
		}`)
		v := assertRestrictionViolationOnPut(t, ctx,
			httpURI+"/v1/schema/PutAddNamedVectorTest", update, "vector_index_type", "flat")
		assert.Contains(t, v.Allowed, "hfresh")
		assert.Contains(t, v.Allowed, "hnsw")
	})
}

// --- response shape + helpers ---

// restrictionViolationBody mirrors the hand-rolled response in
// adapters/handlers/rest/restriction_violation_responder.go. Keep
// the JSON tags in sync.
type restrictionViolationBody struct {
	ErrorCode   string   `json:"errorCode"`
	Restriction string   `json:"restriction"`
	Value       string   `json:"value"`
	Allowed     []string `json:"allowed"`
	Message     string   `json:"message"`
}

func assertRestrictionViolation(t *testing.T, ctx context.Context, url string, body []byte, expectRestriction, expectValue string) restrictionViolationBody {
	t.Helper()
	return assertRestrictionViolationOnMethod(t, ctx, http.MethodPost, url, body, expectRestriction, expectValue)
}

func assertRestrictionViolationOnPut(t *testing.T, ctx context.Context, url string, body []byte, expectRestriction, expectValue string) restrictionViolationBody {
	t.Helper()
	return assertRestrictionViolationOnMethod(t, ctx, http.MethodPut, url, body, expectRestriction, expectValue)
}

func assertRestrictionViolationOnMethod(t *testing.T, ctx context.Context, method, url string, body []byte, expectRestriction, expectValue string) restrictionViolationBody {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "%s %s", method, url)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode,
		"expected HTTP 422 for restriction %q (value %q)", expectRestriction, expectValue)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var parsed restrictionViolationBody
	require.NoError(t, json.Unmarshal(raw, &parsed),
		"response body should be JSON: %s", raw)
	assert.Equal(t, "CONFIG_NOT_ALLOWED", parsed.ErrorCode)
	assert.Equal(t, expectRestriction, parsed.Restriction)
	assert.Equal(t, expectValue, parsed.Value)
	assert.NotEmpty(t, parsed.Allowed, "Allowed list should be populated for SDK consumers")
	assert.NotEmpty(t, parsed.Message, "rendered message must be present")
	return parsed
}

func assertCreateOK(t *testing.T, ctx context.Context, url string, body []byte) {
	t.Helper()
	resp := postRaw(t, ctx, url, body)
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 2xx for POST %s, got %d: %s", url, resp.StatusCode, raw)
	}
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
