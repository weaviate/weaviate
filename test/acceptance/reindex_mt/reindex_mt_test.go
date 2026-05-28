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

// Package reindex_mt tests runtime reindex on multi-tenant collections.
// Each test creates its own MT collection with isolated tenants.
package reindex_mt

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestMultiTenant_ReindexSuite(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	// Dump container logs on failure.
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	t.Run("RepairAllTenants", func(t *testing.T) {
		testRepairAllTenants(t, restURI)
	})

	t.Run("RepairSpecificTenants", func(t *testing.T) {
		testRepairSpecificTenants(t, restURI)
	})

	t.Run("ChangeTokenization", func(t *testing.T) {
		testChangeTokenizationMT(t, restURI)
	})

	t.Run("EnableRangeable", func(t *testing.T) {
		testEnableRangeableMT(t, restURI)
	})

	t.Run("Validation", func(t *testing.T) {
		testValidation(t, restURI)
	})

	// Restart for deferred finalization.
	t.Run("PostRestart", func(t *testing.T) {
		t.Log("restarting container for deferred finalize")
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())
		restURI = compose.GetWeaviate().URI()

		// Verify change-tokenization survived restart.
		testChangeTokenizationMTPostRestart(t, restURI)
		// Verify enable-rangeable survived restart.
		testEnableRangeableMTPostRestart(t, restURI)
	})
}

// =============================================================================
// Test 1: Repair all tenants
// =============================================================================

func testRepairAllTenants(t *testing.T, restURI string) {
	className := "MTRepairAll"
	tenantNames := []string{"tenantA", "tenantB", "tenantC", "tenantD", "tenantE"}

	createMTClass(t, className, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, className, tenantNames)

	// Insert 5 objects per tenant.
	for _, tn := range tenantNames {
		for i := 0; i < 5; i++ {
			obj := &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"text": fmt.Sprintf("doc_%d for %s", i, tn)},
				Tenant:     tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Verify data exists.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "doc", tn)
		require.Len(t, ids, 5, "tenant %s should have 5 objects", tn)
	}

	// Submit repair-searchable (no tenants param → all tenants).
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text", `{"searchable":{"algorithm":"blockmax"}}`)
	t.Logf("repair all tenants task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Verify data still intact.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "doc", tn)
		assert.Len(t, ids, 5, "tenant %s should still have 5 objects after repair", tn)
	}
}

// =============================================================================
// Test 2: Repair specific tenants
// =============================================================================

func testRepairSpecificTenants(t *testing.T, restURI string) {
	// Format-only migration on specific tenants. The prior body
	// `{"searchable":{"algorithm":"blockmax"}}` submitted a
	// ChangeAlgorithm migration, which is now (post
	// weaviate/0-weaviate-issues#254) rejected at the handler — the
	// class-level UsingBlockMaxWAND flag cannot be flipped on a tenant
	// subset (the un-reindexed tenants' queries would route through
	// the wrong BM25 path post-flip). Replaced with enable-rangeable
	// on an int property: format-only, per-tenant compatible, exercises
	// the same tenant-filter dispatch.
	className := "MTRepairSpecific"
	tenantNames := []string{"t1", "t2", "t3", "t4", "t5"}

	createMTClass(t, className, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}},
	})
	addTenants(t, className, tenantNames)

	for _, tn := range tenantNames {
		for i := 0; i < 3; i++ {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"text":  fmt.Sprintf("item_%d from %s", i, tn),
					"score": float64(i + 1),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Repair only t1 and t2 via enable-rangeable on the int property.
	targetTenants := []string{"t1", "t2"}
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`, reindexhelpers.WithTenants(targetTenants))
	t.Logf("repair specific tenants task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// All tenants should still have data.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "item", tn)
		assert.Len(t, ids, 3, "tenant %s should have 3 objects", tn)
	}
}

// =============================================================================
// Test 3: Change tokenization (MT, all tenants, grouped barrier)
// =============================================================================

// Store baselines for post-restart checks.
var changeTokenMTBaselines struct {
	className   string
	tenantNames []string
	// Post-migration expected: field tokenization on "filepath".
	postFullPathIDs map[string][]string // tenant -> IDs from full path BM25 query
}

func testChangeTokenizationMT(t *testing.T, restURI string) {
	className := "MTRetokenize"
	tenantNames := []string{"ct1", "ct2", "ct3"}
	changeTokenMTBaselines.className = className
	changeTokenMTBaselines.tenantNames = tenantNames
	changeTokenMTBaselines.postFullPathIDs = make(map[string][]string)

	createMTClass(t, className, []*models.Property{
		{Name: "filepath", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "description", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, className, tenantNames)

	filepaths := []string{
		"/code/github.com/weaviate/weaviate/main.go",
		"/code/github.com/other/project/main.go",
		"/code/docs/tutorial/getting_started.md",
	}
	for _, tn := range tenantNames {
		for i, fp := range filepaths {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"filepath":    fp,
					"description": fmt.Sprintf("file %d for %s", i, tn),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Pre-migration: "weaviate" matches via word tokenization.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		require.NotEmpty(t, ids, "pre-migration: tenant %s should find 'weaviate'", tn)
	}

	// Change tokenization to field (must target all tenants).
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "filepath",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("change tokenization MT task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Wait for schema update.
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, className)
		for _, prop := range cls.Properties {
			if prop.Name == "filepath" {
				return prop.Tokenization == "field"
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "tokenization should change to field")

	// Post-migration: "weaviate" should NOT match (field tokenization).
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		assert.Empty(t, ids, "post-migration: tenant %s should NOT find 'weaviate' with field tokenization", tn)
	}

	// Post-migration: full path should match exactly 1.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath",
			"/code/github.com/weaviate/weaviate/main.go", tn)
		assert.Len(t, ids, 1, "post-migration: tenant %s should find exactly 1 for full path", tn)
		changeTokenMTBaselines.postFullPathIDs[tn] = ids
	}
}

func testChangeTokenizationMTPostRestart(t *testing.T, restURI string) {
	className := changeTokenMTBaselines.className
	if className == "" {
		t.Skip("change tokenization baselines not set")
	}

	helper.SetupClient(restURI)

	// Schema should still show field tokenization.
	cls := helper.GetClass(t, className)
	for _, prop := range cls.Properties {
		if prop.Name == "filepath" {
			assert.Equal(t, "field", prop.Tokenization, "post-restart: should be field")
		}
	}

	// Queries should still work.
	for _, tn := range changeTokenMTBaselines.tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		assert.Empty(t, ids, "post-restart: tenant %s should NOT find 'weaviate'", tn)

		ids = bm25QueryTenant(t, className, "filepath",
			"/code/github.com/weaviate/weaviate/main.go", tn)
		assert.Len(t, ids, 1, "post-restart: tenant %s should find 1 for full path", tn)
	}
}

// =============================================================================
// Test 4: Enable rangeable (MT)
// =============================================================================

var enableRangeableMTBaselines struct {
	className   string
	tenantNames []string
}

func testEnableRangeableMT(t *testing.T, restURI string) {
	className := "MTRangeable"
	tenantNames := []string{"rt1", "rt2", "rt3"}
	enableRangeableMTBaselines.className = className
	enableRangeableMTBaselines.tenantNames = tenantNames

	createMTClass(t, className, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{Name: "score", DataType: []string{"int"}},
	})
	addTenants(t, className, tenantNames)

	for _, tn := range tenantNames {
		for i := 0; i < 10; i++ {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"name":  fmt.Sprintf("item_%d", i),
					"score": float64(i + 1),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Pre-migration: range queries work via filterable (slower but functional).
	for _, tn := range tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		require.Len(t, ids, 5, "tenant %s should have 5 items with score>5", tn)
	}

	// Enable rangeable.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("enable rangeable MT task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Schema should show indexRangeFilters=true.
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, className)
		for _, prop := range cls.Properties {
			if prop.Name == "score" {
				return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
			}
		}
		return false
	}, 30*time.Second, 1*time.Second)

	// Range queries should still work.
	for _, tn := range tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		assert.Len(t, ids, 5, "post-rangeable: tenant %s should have 5 items with score>5", tn)
	}
}

func testEnableRangeableMTPostRestart(t *testing.T, restURI string) {
	className := enableRangeableMTBaselines.className
	if className == "" {
		t.Skip("enable rangeable baselines not set")
	}

	helper.SetupClient(restURI)

	cls := helper.GetClass(t, className)
	for _, prop := range cls.Properties {
		if prop.Name == "score" {
			require.NotNil(t, prop.IndexRangeFilters)
			assert.True(t, *prop.IndexRangeFilters, "post-restart: score should be rangeable")
		}
	}

	for _, tn := range enableRangeableMTBaselines.tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		assert.Len(t, ids, 5, "post-restart: tenant %s should have 5 items with score>5", tn)
	}
}

// =============================================================================
// Test 5: Validation
// =============================================================================

func testValidation(t *testing.T, restURI string) {
	// Non-MT class with tenants param → 400.
	nonMTClass := "MTValidateNonMT"
	createNonMTClass(t, nonMTClass, []*models.Property{
		{Name: "text", DataType: []string{"text"}},
	})
	for i := 0; i < 3; i++ {
		obj := &models.Object{
			Class:      nonMTClass,
			Properties: map[string]interface{}{"text": fmt.Sprintf("doc_%d", i)},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	t.Run("NonMT_with_tenants", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpdateExpect4xx(t, restURI, nonMTClass, "text",
			`{"searchable":{"algorithm":"blockmax"}}`, reindexhelpers.WithTenants([]string{"t1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"non-MT class with tenants should reject as 400: %s", got.Body)
	})

	// MT class for remaining validations.
	mtClass := "MTValidate"
	createMTClass(t, mtClass, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, mtClass, []string{"active1", "active2"})
	for _, tn := range []string{"active1", "active2"} {
		obj := &models.Object{
			Class:      mtClass,
			Properties: map[string]interface{}{"text": "hello world"},
			Tenant:     tn,
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	t.Run("ChangeTokenization_with_tenants", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpdateExpect4xx(t, restURI, mtClass, "text",
			`{"searchable":{"tokenization":"field"}}`, reindexhelpers.WithTenants([]string{"active1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"MT class with tenants on change-tokenization should reject as 400: %s", got.Body)
	})

	t.Run("ChangeAlgorithm_with_tenants", func(t *testing.T) {
		// Post weaviate/0-weaviate-issues#254, change-algorithm is
		// semantic — same tenant-subset rejection as change-tokenization.
		got := reindexhelpers.SubmitIndexUpdateExpect4xx(t, restURI, mtClass, "text",
			`{"searchable":{"algorithm":"blockmax"}}`, reindexhelpers.WithTenants([]string{"active1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"MT class with tenants on change-algorithm should reject as 400: %s", got.Body)
	})

	t.Run("Nonexistent_tenant", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpdateExpect4xx(t, restURI, mtClass, "text",
			`{"searchable":{"algorithm":"blockmax"}}`, reindexhelpers.WithTenants([]string{"does_not_exist"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"non-existent tenant should reject as 400: %s", got.Body)
	})
}

// =============================================================================
// Helpers
// =============================================================================

func createMTClass(t *testing.T, className string, properties []*models.Property) {
	t.Helper()
	class := &models.Class{
		Class:      className,
		Properties: properties,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
}

func createNonMTClass(t *testing.T, className string, properties []*models.Property) {
	t.Helper()
	class := &models.Class{
		Class:      className,
		Properties: properties,
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
}

func addTenants(t *testing.T, className string, tenantNames []string) {
	t.Helper()
	tenants := make([]*models.Tenant, len(tenantNames))
	for i, tn := range tenantNames {
		tenants[i] = &models.Tenant{
			Name:           tn,
			ActivityStatus: models.TenantActivityStatusHOT,
		}
	}
	helper.CreateTenants(t, className, tenants)
}

func bm25QueryTenant(t *testing.T, className, property, query, tenant string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}, tenant: %q) {
				_additional { id }
			}
		}
	}`, className, query, property, tenant)
	return runGraphQLQuery(t, className, gqlQuery)
}

func rangeQueryTenant(t *testing.T, className, tenant, where string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s, tenant: %q) {
				_additional { id }
			}
		}
	}`, className, where, tenant)
	return runGraphQLQuery(t, className, gqlQuery)
}

func runGraphQLQuery(t *testing.T, className, gqlQuery string) []string {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
	require.NoError(t, err)
	if len(resp.Errors) > 0 {
		t.Fatalf("graphql errors: %v", resp.Errors[0].Message)
	}
	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})
	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	sort.Strings(ids)
	return ids
}
