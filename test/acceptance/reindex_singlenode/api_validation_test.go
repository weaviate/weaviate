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

// testReindexAPIValidation exercises the HTTP-contract surface of the GA
// reindex resource family:
//
//	PUT    /v1/schema/{class}/properties/{prop}/index/{indexType}
//	POST   .../index/{indexType}/rebuild
//	POST   .../index/{indexType}/cancel
//
// It covers the status-code taxonomy that fails at validation before any task
// is submitted (so it does not extend the shared-container runtime):
//
//   - Empty body / malformed JSON                 -> 422 / 400
//   - Invalid {indexType} path value              -> 422
//   - Unknown collection / property               -> 404
//   - tenants= on a single-tenant class           -> 400
//   - tenants=<nonexistent> on MT class           -> 400
//   - tenants= on a semantic migration            -> 400
//   - tenants= on a NO_OP-resolving PUT           -> 400 (not silently 200)
//   - same-tokenization / already-filterable      -> 200 NO_OP (declarative upsert)
//   - non-numeric for rangeFilters                -> 400
//   - config field on rangeFilters                -> 400
//   - both tokenization and algorithm in one PUT  -> 400
//   - reference / blob for filterable             -> 400

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
	// The "tenants subset..." sub-test leaves a repair-filterable task
	// in-flight. DeleteClass is rejected by the MutationGuard while it runs,
	// so cleanup must cancel it (via POST .../cancel) before deleting the class.
	defer func() {
		cancelURL := fmt.Sprintf("http://%s/v1/schema/%s/properties/text_word/index/filterable/cancel", restURI, mtClass)
		cancelReq, _ := http.NewRequest(http.MethodPost, cancelURL, nil)
		if cancelResp, err := http.DefaultClient.Do(cancelReq); err == nil {
			cancelResp.Body.Close()
		}
		// A best-effort cancel may not terminalize a task already in SWAPPING,
		// so poll the delete until the MutationGuard clears. The guard returns
		// 400 while a reindex task is still in flight; retry only on that (and
		// transient transport errors) and surface any other status immediately.
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
		indexType   string
		verb        string // "" = PUT, "rebuild"/"cancel" = POST sub-resource
		body        string // PUT body only
		tenantsQS   string // optional ?tenants=... query string fragment
		wantStatus  int
		wantBodyHas string // optional substring in the response body
	}

	cases := []apiCase{
		{
			// go-swagger's body-required validation fires before the handler
			// runs, producing 422 for a missing body.
			name:       "empty body",
			collection: stClass, property: "text_word", indexType: "searchable",
			body: "", wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "malformed JSON",
			collection: stClass, property: "text_word", indexType: "searchable",
			body: `{"tokenization":`, wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid index type",
			collection: stClass, property: "text_word", indexType: "bogus",
			body: `{}`, wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "unknown collection",
			collection: "DoesNotExist", property: "text_word", indexType: "searchable",
			body: `{"algorithm":"blockmax"}`, wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown property",
			collection: stClass, property: "nope", indexType: "searchable",
			body: `{"algorithm":"blockmax"}`, wantStatus: http.StatusNotFound,
		},
		{
			// A rangeFilters create is a real (format-only) migration, so it
			// reaches the tenant-scope gate in the submit path.
			name:       "tenants on single-tenant collection",
			collection: stClass, property: "score", indexType: "rangeFilters",
			body:        `{}`,
			tenantsQS:   "?tenants=t1",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "multi-tenant",
		},
		{
			name:       "tenants=<nonexistent> on MT class with format-only migration",
			collection: mtClass, property: "text_word", indexType: "filterable", verb: "rebuild",
			tenantsQS:   "?tenants=nonexistent_tenant_xyz",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not exist",
		},
		{
			// Declarative upsert: same tokenization is now a NO_OP, not a 400.
			name:       "change-tokenization same value (word -> word) is NO_OP",
			collection: stClass, property: "text_word", indexType: "searchable",
			body:        `{"tokenization":"word"}`,
			wantStatus:  http.StatusOK,
			wantBodyHas: "NO_OP",
		},
		{
			// NO_OP + tenants: an already-filterable prop resolves to NO_OP,
			// but a tenants param on a single-tenant collection is still a 400.
			// Regression guard — the NO_OP fast-path used to return 200 and
			// silently swallow the tenants param ahead of this gate.
			name:       "tenants on NO_OP (already-filterable) single-tenant collection",
			collection: stClass, property: "score", indexType: "filterable",
			body:        `{}`,
			tenantsQS:   "?tenants=t1",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "multi-tenant",
		},
		{
			// NO_OP + tenants on a semantic op: same-tokenization searchable
			// resolves to NO_OP, but tenants on a semantic migration is a 400.
			// tenant-a exists, so this pins the semantic gate specifically (not
			// tenant validity) firing on the NO_OP path.
			name:       "tenants on NO_OP (same-tokenization) semantic op",
			collection: mtClass, property: "text_word", indexType: "searchable",
			body:        `{"tokenization":"word"}`,
			tenantsQS:   "?tenants=tenant-a",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "semantic",
		},
		{
			name:       "change-tokenization invalid tokenization",
			collection: stClass, property: "text_word", indexType: "searchable",
			body:        `{"tokenization":"not_a_real_tokenization"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "invalid tokenization",
		},
		{
			name:       "both tokenization and algorithm in one request",
			collection: stClass, property: "text_word", indexType: "searchable",
			body:        `{"tokenization":"lowercase","algorithm":"blockmax"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "at most one",
		},
		{
			name:       "rangeFilters on non-numeric (text)",
			collection: stClass, property: "text_word", indexType: "rangeFilters",
			body:        `{}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "not a numeric type",
		},
		{
			name:       "rangeFilters rejects config fields",
			collection: stClass, property: "score", indexType: "rangeFilters",
			body:        `{"tokenization":"word"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "no configuration fields",
		},
		{
			name:       "filterable create on blob",
			collection: stClass, property: "blob_prop", indexType: "filterable",
			body:        `{}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not support",
		},
		{
			// Already-filterable is now an idempotent NO_OP, not a 400.
			name:       "filterable on already-filterable prop is NO_OP",
			collection: stClass, property: "score", indexType: "filterable",
			body:        `{}`,
			wantStatus:  http.StatusOK,
			wantBodyHas: "NO_OP",
		},
		{
			name:       "searchable create on non-text",
			collection: stClass, property: "score", indexType: "searchable",
			body:        `{"tokenization":"word"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "not a text type",
		},
		{
			name:       "searchable create without tokenization",
			collection: stClass, property: "text_unindexed", indexType: "searchable",
			body:        `{}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "requires a tokenization",
		},
		{
			name:       "searchable create with invalid tokenization",
			collection: stClass, property: "text_unindexed", indexType: "searchable",
			body:        `{"tokenization":"not_real"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "invalid tokenization",
		},
		{
			name:       "algorithm on property with no searchable index",
			collection: stClass, property: "text_unindexed", indexType: "searchable",
			body:        `{"algorithm":"blockmax"}`,
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "has no searchable index",
		},
		{
			name:       "rebuild on property with no filterable index",
			collection: stClass, property: "text_unindexed", indexType: "filterable", verb: "rebuild",
			wantStatus:  http.StatusBadRequest,
			wantBodyHas: "does not have a filterable index",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := fmt.Sprintf("http://%s/v1/schema/%s/properties/%s/index/%s",
				restURI, tc.collection, tc.property, tc.indexType)
			method := http.MethodPut
			var reader io.Reader
			if tc.verb != "" {
				base += "/" + tc.verb
				method = http.MethodPost
			} else {
				reader = bytes.NewReader([]byte(tc.body))
			}
			req, err := http.NewRequest(method, base+tc.tenantsQS, reader)
			require.NoError(t, err)
			if tc.verb == "" {
				req.Header.Set("Content-Type", "application/json")
			}

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

	// MT-specific positive case: tenants= subset succeeds on a format-only
	// rebuild (just verifies the HTTP contract — the task itself isn't awaited
	// because it would slow the shared container).
	t.Run("tenants subset accepted on MT collection", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/v1/schema/%s/properties/text_word/index/filterable/rebuild?tenants=tenant-a",
			restURI, mtClass)
		req, _ := http.NewRequest(http.MethodPost, url, nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusAccepted, resp.StatusCode,
			"MT subset rebuild should be accepted; got %d, body=%s",
			resp.StatusCode, string(body))
	})

	// Semantic migration (change-tokenization) cannot target a subset of tenants.
	t.Run("tenants= rejected for change-tokenization (semantic)", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/v1/schema/%s/properties/text_word/index/searchable?tenants=tenant-a",
			restURI, mtClass)
		req, _ := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"tokenization":"lowercase"}`)))
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
