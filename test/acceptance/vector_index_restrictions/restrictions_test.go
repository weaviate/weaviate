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

	"github.com/weaviate/weaviate/entities/models"
)

// TestRestrictions_SharedCluster bundles the RFC scenarios that share
// the mixed `hfresh,hnsw` allow-list into a single 3-node testcontainer:
// the cluster-shape lets one sub-test cover Dirk's per-node 422 concern
// without paying for a second container start. Boot-time and runtime-
// override scenarios spin up their own containers because they need a
// different startup config.
func TestRestrictions_SharedCluster(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	compose, terminate := startCluster(t, ctx, map[string]string{
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
		// hfresh has no compression knobs; exempt from the allow-list.
		body := []byte(`{
			"class":"HfreshExempt",
			"vectorizer":"none",
			"vectorIndexType":"hfresh"
		}`)
		assertCreateOK(t, ctx, httpURI+"/v1/schema", body)
	})

	t.Run("named-vector with disallowed type is rejected", func(t *testing.T) {
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
		// Only scenario where the allow-list is the sole gate: the
		// RAFT-side immutable check iterates initial.VectorConfig, so
		// brand-new entries on the updated side reach the use-case
		// allow-list unfiltered.
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

	// Pre-RAFT rejection per Dirk's Slack note: each node surfaces the
	// typed 422 locally. A 500 here means the typed error didn't survive
	// a RAFT round-trip — see clusterapi/shared/indices_payloads.go
	// (#11342) for the usage-limit precedent.
	t.Run("all nodes return typed 422 for a disallowed type", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			i := i
			t.Run("node_"+string(rune('0'+i)), func(t *testing.T) {
				uri := "http://" + compose.GetWeaviateNode(i).URI() + "/v1/schema"
				body := []byte(`{
					"class":"FlatRejectedNode` + string(rune('0'+i)) + `",
					"vectorizer":"none",
					"vectorIndexType":"flat"
				}`)
				assertRestrictionViolation(t, ctx, uri, body, "vector_index_type", "flat")
			})
		}
	})
}

func assertRestrictionViolation(t *testing.T, ctx context.Context, url string, body []byte, expectRestriction, expectValue string) *models.RestrictionViolationResponse {
	t.Helper()
	return assertRestrictionViolationOnMethod(t, ctx, http.MethodPost, url, body, expectRestriction, expectValue)
}

func assertRestrictionViolationOnPut(t *testing.T, ctx context.Context, url string, body []byte, expectRestriction, expectValue string) *models.RestrictionViolationResponse {
	t.Helper()
	return assertRestrictionViolationOnMethod(t, ctx, http.MethodPut, url, body, expectRestriction, expectValue)
}

func assertRestrictionViolationOnMethod(t *testing.T, ctx context.Context, method, url string, body []byte, expectRestriction, expectValue string) *models.RestrictionViolationResponse {
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
	parsed := &models.RestrictionViolationResponse{}
	require.NoError(t, json.Unmarshal(raw, parsed),
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
