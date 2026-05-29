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

package reindex_singlenode

// testReindexAPIValidation exercises the HTTP-contract surface of
// PUT /v1/schema/{className}/indexes/{propertyName}. It covers cases that
// previously had no direct acceptance coverage:
//
//   - Empty body / malformed JSON         -> 400
//   - All flags absent (no actionable)     -> 400
//   - Unknown collection                   -> 404
//   - Unknown property                     -> 404
//   - tenants= on a single-tenant class    -> 400
//   - tenants=<nonexistent> on MT class    -> 400
//   - same-tokenization (word->word)       -> 400
//   - already-rangeable / already-filterable / already-searchable -> 400
//   - non-numeric for rangeable            -> 400
//   - reference / blob for filterable      -> 400
//
// All cases finish in milliseconds — they fail at validation before any task
// is submitted, so the test does not extend the shared-container runtime.

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func testReindexAPIValidation(t *testing.T, restURI string) {
	trueVal := true
	falseVal := false

	// Single-tenant class with a representative mix of properties.
	const stClass = "APIValidationSTClass"
	helper.CreateClass(t, &models.Class{
		Class: stClass,
		Properties: []*models.Property{
			{Name: "text_word", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &trueVal, IndexFilterable: &trueVal},
			{Name: "text_unindexed", DataType: []string{"text"}, IndexSearchable: &falseVal, IndexFilterable: &falseVal},
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal},
			{Name: "blob_prop", DataType: []string{"blob"}, IndexFilterable: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, stClass)

	// Multi-tenant class for tenant-param tests.
	const mtClass = "APIValidationMTClass"
	helper.CreateClass(t, &models.Class{
		Class: mtClass,
		Properties: []*models.Property{
			{Name: "text_word", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &trueVal, IndexFilterable: &trueVal},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Vectorizer:         "none",
	})
	// The "tenants subset accepted on MT collection" sub-test submits a
	// repair-filterable task on mtClass.text_word and does NOT await
	// terminal state. After PR https://github.com/weaviate/weaviate/pull/11320 / https://github.com/weaviate/0-weaviate-issues/issues/218 / https://github.com/weaviate/0-weaviate-issues/issues/219,
	// DeleteClass is rejected by the schema FSM's MutationGuard while
	// any reindex task is in flight on the collection (the guard is
	// what protects against the bucket↔schema-inversion family of
	// Sev 1 bugs). The deferred cleanup must therefore cancel the
	// in-flight task BEFORE asking the schema FSM to delete the class.
	defer func() {
		// Best-effort cancel of any reindex still STARTED on
		// mtClass.text_word. The PUT body shape mirrors what the
		// test's positive-path sub-test submitted; the cancel verb
		// here is what makes DTM transition the task out of STARTED
		// so the MutationGuard will allow the DeleteClass below.
		cancelURL := fmt.Sprintf("http://%s/v1/schema/%s/indexes/text_word", restURI, mtClass)
		cancelReq, _ := http.NewRequest(http.MethodPut, cancelURL,
			bytes.NewReader([]byte(`{"filterable":{"cancel":true}}`)))
		cancelReq.Header.Set("Content-Type", "application/json")
		if cancelResp, err := http.DefaultClient.Do(cancelReq); err == nil {
			cancelResp.Body.Close()
		}
		// A best-effort cancel may not terminalize a task already in SWAPPING,
		// so poll the delete until the MutationGuard clears. The guard returns
		// 400 while a reindex task is still in flight; retry only on that (and
		// transient transport errors) and surface any other status immediately
		// rather than masking a real failure for the whole timeout. Manual
		// loop, not require.Eventually, because the latter runs its closure in
		// a separate goroutine where t.Fatalf/require are unsafe.
		deadline := time.Now().Add(60 * time.Second)
		var lastInfo string
		for {
			delReq, _ := http.NewRequest(http.MethodDelete,
				fmt.Sprintf("http://%s/v1/schema/%s", restURI, mtClass), nil)
			delResp, err := http.DefaultClient.Do(delReq)
			if err != nil {
				lastInfo = fmt.Sprintf("transport error: %v", err)
			} else {
				body, _ := io.ReadAll(delResp.Body)
				delResp.Body.Close()
				if delResp.StatusCode == http.StatusOK {
					break
				}
				require.Equalf(t, http.StatusBadRequest, delResp.StatusCode,
					"DeleteClass(%s) returned unexpected status %d (expected 200, or 400 from the in-flight MutationGuard): %s",
					mtClass, delResp.StatusCode, string(body))
				lastInfo = fmt.Sprintf("400 MutationGuard: %s", string(body))
			}
			if time.Now().After(deadline) {
				t.Fatalf("DeleteClass(%s) did not succeed within 60s — reindex task never left in-flight; last response: %s",
					mtClass, lastInfo)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	helper.CreateTenants(t, mtClass, []*models.Tenant{
		{Name: "tenant-a"}, {Name: "tenant-b"},
	})

	type apiCase struct {
		name        string
		collection  string
		property    string
		body        string
		tenantsQS   string // optional ?tenants=... query string fragment
		wantStatus  int
		wantBodyHas string // optional substring in the response body
	}

	cases := []apiCase{
		{
			// go-swagger's body-required validation fires before the handler
			// runs, producing 422 with "body in body is required". Handler-
			// level "request body required" 400 is only reachable when the
			// body parses to nil (e.g. literal `null`), not for missing-body.
			name:       "empty body",
			collection: stClass, property: "text_word",
			body: "", wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "malformed JSON",
			collection: stClass, property: "text_word",
			body: `{"searchable":`, wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no actionable flags",
			collection: stClass, property: "text_word",
			body:        `{"searchable":{},"filterable":{},"rangeable":{}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "no actionable change",
		},
		{
			name:       "explicitly false flags only",
			collection: stClass, property: "text_word",
			body:        `{"searchable":{"enabled":false,"rebuild":false}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "no actionable change",
		},
		{
			name:       "unknown collection",
			collection: "DoesNotExist", property: "text_word",
			body:       `{"searchable":{"algorithm":"blockmax"}}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown property",
			collection: stClass, property: "nope",
			body:       `{"searchable":{"algorithm":"blockmax"}}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "tenants on single-tenant collection",
			collection: stClass, property: "text_word",
			body:        `{"searchable":{"algorithm":"blockmax"}}`,
			tenantsQS:   "?tenants=t1",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "multi-tenant",
		},
		{
			// Format-only body so the per-tenant existence check is reached
			// (semantic bodies short-circuit on the semantic-vs-tenants gate).
			name:       "tenants=<nonexistent> on MT class with format-only migration",
			collection: mtClass, property: "text_word",
			body:        `{"filterable":{"rebuild":true}}`,
			tenantsQS:   "?tenants=nonexistent_tenant_xyz",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not exist",
		},
		{
			name:       "tenants on semantic change-algorithm rejected",
			collection: mtClass, property: "text_word",
			body:        `{"searchable":{"algorithm":"blockmax"}}`,
			tenantsQS:   "?tenants=t1",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "semantic migrations",
		},
		{
			name:       "change-tokenization same value (word -> word) rejected",
			collection: stClass, property: "text_word",
			body:        `{"searchable":{"tokenization":"word"}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "already uses tokenization",
		},
		{
			name:       "change-tokenization invalid tokenization",
			collection: stClass, property: "text_word",
			body:        `{"searchable":{"tokenization":"not_a_real_tokenization"}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "invalid tokenization",
		},
		{
			name:       "rangeable on non-numeric (text)",
			collection: stClass, property: "text_word",
			body:        `{"rangeable":{"enabled":true}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "not a numeric type",
		},
		{
			name:       "filterable.enabled on blob",
			collection: stClass, property: "blob_prop",
			body:        `{"filterable":{"enabled":true}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not support",
		},
		{
			name:       "filterable.enabled on already-filterable prop",
			collection: stClass, property: "score",
			body:        `{"filterable":{"enabled":true}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "already has a filterable index",
		},
		{
			name:       "searchable.enabled on non-text",
			collection: stClass, property: "score",
			body:        `{"searchable":{"enabled":true,"tokenization":"word"}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "not a text type",
		},
		{
			name:       "searchable.enabled without tokenization",
			collection: stClass, property: "text_unindexed",
			body:        `{"searchable":{"enabled":true}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "requires a tokenization",
		},
		{
			name:       "searchable.enabled with invalid tokenization",
			collection: stClass, property: "text_unindexed",
			body:        `{"searchable":{"enabled":true,"tokenization":"not_real"}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "invalid tokenization",
		},
		{
			name:       "searchable.algorithm:blockmax on property with no searchable index",
			collection: stClass, property: "text_unindexed",
			body:        `{"searchable":{"algorithm":"blockmax"}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "has no searchable index",
		},
		{
			name:       "filterable.rebuild on property with no filterable index",
			collection: stClass, property: "text_unindexed",
			body:        `{"filterable":{"rebuild":true}}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not have a filterable index",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s%s",
				restURI, tc.collection, tc.property, tc.tenantsQS)
			req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(tc.body)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			require.Equal(t, tc.wantStatus, resp.StatusCode,
				"want %d, got %d; body=%s", tc.wantStatus, resp.StatusCode, string(body))
			if tc.wantBodyHas != "" {
				assert.Contains(t, string(body), tc.wantBodyHas,
					"body did not contain expected substring; got: %s", string(body))
			}
		})
	}

	// MT-specific positive case: tenants= subset succeeds (just verifies the
	// HTTP contract — the task itself isn't awaited because it would slow the
	// shared container; the validation path is what we care about).
	t.Run("tenants subset accepted on MT collection", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/text_word?tenants=tenant-a",
			restURI, mtClass)
		req, _ := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"filterable":{"rebuild":true}}`)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusAccepted, resp.StatusCode,
			"MT subset reindex should be accepted; got %d, body=%s",
			resp.StatusCode, string(body))
	})

	// Semantic migration (change-tokenization) cannot target a subset of tenants.
	t.Run("tenants= rejected for change-tokenization (semantic)", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/text_word?tenants=tenant-a",
			restURI, mtClass)
		req, _ := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"searchable":{"tokenization":"lowercase"}}`)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode,
			"change-tokenization+tenants must be 400; body=%s", string(body))
		assert.Contains(t, string(body), "semantic")
	})
}
