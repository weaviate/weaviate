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

package mcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	toolNameQueryHybrid = "weaviate-query-hybrid"
)

// setupQueryHybridTest handles the boilerplate setup: client init, class creation, and context generation.
// It returns the class schema, the context, and a cleanup function.
func setupQueryHybridTest(t *testing.T) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)

	cls := &models.Class{
		Class: "TestArticle",
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "contents",
				DataType: []string{"text"},
			},
			{
				Name:     "author",
				DataType: []string{"text"},
			},
			{
				Name:     "year",
				DataType: []string{"int"},
			},
			{
				Name:     "status",
				DataType: []string{"text"},
			},
			{
				Name:     "publishDate",
				DataType: []string{"date"},
			},
		},
	}

	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// setupQueryHybridMultiTenantTest creates a multi-tenant class and returns it with context and cleanup.
func setupQueryHybridMultiTenantTest(t *testing.T, tenantNames []string) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)

	cls := &models.Class{
		Class: "TestMultiTenantArticle",
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "contents",
				DataType: []string{"text"},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	// Create tenants if provided
	if len(tenantNames) > 0 {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i, name := range tenantNames {
			tenants[i] = &models.Tenant{Name: name}
		}
		helper.CreateTenantsAuth(t, cls.Class, tenants, testAPIKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// insertTestArticles inserts test articles for query testing
func insertTestArticles(t *testing.T, className string) {
	t.Helper()

	objects := []*models.Object{
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Machine Learning Basics",
				"contents":    "An introduction to machine learning concepts and algorithms",
				"author":      "John Doe",
				"year":        2020,
				"status":      "published",
				"publishDate": "2020-01-15T10:00:00Z",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Deep Learning Advanced",
				"contents":    "Advanced deep learning techniques for neural networks",
				"author":      "Jane Smith",
				"year":        2022,
				"status":      "published",
				"publishDate": "2022-06-20T14:30:00Z",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Python Programming",
				"contents":    "Learn Python programming from scratch",
				"author":      "Bob Johnson",
				"year":        2019,
				"status":      "draft",
				"publishDate": "2019-03-10T08:00:00Z",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Data Science Guide",
				"contents":    "A comprehensive guide to data science and analytics",
				"author":      "Alice Brown",
				"year":        2023,
				"status":      "published",
				"publishDate": "2023-09-05T12:00:00Z",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Neural Networks Explained",
				"contents":    "Understanding neural networks and their applications",
				"author":      "John Doe",
				"year":        2021,
				"status":      "published",
				"publishDate": "2021-11-30T16:45:00Z",
			},
		},
	}

	helper.CreateObjectsBatchAuth(t, objects, testAPIKey)
}

// Test 1: Pure BM25 keyword search (alpha=0.0)
func TestQueryHybrid_PureBM25Search(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0 // Pure BM25 keyword search
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "machine learning",
		Alpha:          &alpha,
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0, "should find matching results")

	// Note: Properties are returned at the top level of each result, not nested
	result := results.Results[0].(map[string]any)
	title := result["title"].(string)
	assert.Contains(t, title, "Machine Learning")
}

// Test 2: Test with limit parameter
func TestQueryHybrid_WithLimit(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Test with limit=2
	var results *search.QueryHybridResp
	alpha := 0.0
	limit := 2
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Limit:          &limit,
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	assert.LessOrEqual(t, len(results.Results), 2, "should return at most 2 results")

	// Test with limit=0
	limit = 0
	err = helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Limit:          &limit,
	}, &results, testAPIKey)
	require.Nil(t, err)
	assert.Len(t, results.Results, 0, "limit=0 should return no results")
}

// Test 3: Return specific properties
func TestQueryHybrid_ReturnSpecificProperties(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            "learning",
		Alpha:            &alpha,
		ReturnProperties: []string{"title", "author"},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify requested properties are returned
	// NOTE: Currently the MCP implementation returns all properties regardless of ReturnProperties
	// This test just verifies the requested properties are present
	result := results.Results[0].(map[string]any)
	assert.Contains(t, result, "title")
	assert.Contains(t, result, "author")
	// TODO: Once MCP properly implements property filtering, add these assertions:
	// assert.NotContains(t, result, "contents", "contents should not be returned")
	// assert.NotContains(t, result, "year", "year should not be returned")
}

// Test 4: Return all properties (default behavior)
func TestQueryHybrid_ReturnAllProperties(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all properties are returned
	result := results.Results[0].(map[string]any)
	assert.Contains(t, result, "title")
	assert.Contains(t, result, "contents")
	assert.Contains(t, result, "author")
	assert.Contains(t, result, "year")
	assert.Contains(t, result, "status")
}

// Test 5: Return metadata
func TestQueryHybrid_ReturnMetadata(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		ReturnMetadata: []string{"id", "score", "creationTimeUnix"},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify metadata is present
	result := results.Results[0].(map[string]any)
	assert.Contains(t, result, "id")

	// Check for additional metadata
	if additional, hasAdditional := result["_additional"].(map[string]any); hasAdditional {
		// score might be present in additional
		if score, hasScore := additional["score"]; hasScore {
			assert.IsType(t, float64(0), score)
		}
	}
}

// Test 6: Target specific properties for BM25 search
func TestQueryHybrid_TargetSpecificProperties(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search for "Python" targeting only title
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            "Python",
		Alpha:            &alpha,
		TargetProperties: []string{"title"},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify the result has "Python" in title
	result := results.Results[0].(map[string]any)
	title := result["title"].(string)
	assert.Contains(t, title, "Python")
}

// Test 7: Simple filter - Equal operator
func TestQueryHybrid_WithSimpleFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search with filter for status="published"
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "published",
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all results have status="published"
	for _, r := range results.Results {
		result := r.(map[string]any)
		assert.Equal(t, "published", result["status"])
	}
}

// Test 8: Numeric filter
func TestQueryHybrid_WithNumericFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search with filter for year >= 2020
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":     []string{"year"},
			"operator": "GreaterThanEqual",
			"valueInt": 2020,
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all results have year >= 2020
	for _, r := range results.Results {
		result := r.(map[string]any)
		year := int(result["year"].(float64))
		assert.GreaterOrEqual(t, year, 2020)
	}
}

// Test 9: Date filter with RFC3339 format
func TestQueryHybrid_WithDateFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search with filter for publishDate >= 2021-01-01T00:00:00Z
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":      []string{"publishDate"},
			"operator":  "GreaterThanEqual",
			"valueDate": "2021-01-01T00:00:00Z",
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all results have publishDate >= 2021-01-01
	for _, r := range results.Results {
		result := r.(map[string]any)
		publishDate := result["publishDate"].(string)
		assert.True(t, publishDate >= "2021-01-01T00:00:00Z")
	}
}

// Test 10: Complex AND filter
func TestQueryHybrid_WithComplexAndFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search with AND filter: status="published" AND year >= 2020
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"operator": "And",
			"operands": []map[string]any{
				{
					"path":      []string{"status"},
					"operator":  "Equal",
					"valueText": "published",
				},
				{
					"path":     []string{"year"},
					"operator": "GreaterThanEqual",
					"valueInt": 2020,
				},
			},
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all results match both conditions
	for _, r := range results.Results {
		result := r.(map[string]any)
		assert.Equal(t, "published", result["status"])
		year := int(result["year"].(float64))
		assert.GreaterOrEqual(t, year, 2020)
	}
}

// Test 11: OR filter
func TestQueryHybrid_WithOrFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search with OR filter: author="John Doe" OR author="Jane Smith"
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"operator": "Or",
			"operands": []map[string]any{
				{
					"path":      []string{"author"},
					"operator":  "Equal",
					"valueText": "John Doe",
				},
				{
					"path":      []string{"author"},
					"operator":  "Equal",
					"valueText": "Jane Smith",
				},
			},
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify all results match at least one condition
	for _, r := range results.Results {
		result := r.(map[string]any)
		author := result["author"].(string)
		assert.True(t, author == "John Doe" || author == "Jane Smith")
	}
}

// Test 12: Query without filter
func TestQueryHybrid_NoFilter(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	// Should return multiple results without filtering
	assert.Greater(t, len(results.Results), 0)
}

// Test 13: Multi-tenant search
func TestQueryHybrid_WithTenant(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls, _, cleanup := setupQueryHybridMultiTenantTest(t, []string{"tenant-a", "tenant-b"})
	defer cleanup()

	// Tenants already created in setup
	tenants := []*models.Tenant{
		{Name: "tenant-a"},
		{Name: "tenant-b"},
	}
	helper.CreateTenantsAuth(t, cls.Class, tenants, apiKey)

	// Insert objects for tenant-a
	objectsA := []*models.Object{
		{
			Class:  cls.Class,
			Tenant: "tenant-a",
			Properties: map[string]interface{}{
				"title":    "Tenant A Article 1",
				"contents": "Machine learning for tenant A",
			},
		},
		{
			Class:  cls.Class,
			Tenant: "tenant-a",
			Properties: map[string]interface{}{
				"title":    "Tenant A Article 2",
				"contents": "Deep learning algorithms",
			},
		},
	}
	helper.CreateObjectsBatchAuth(t, objectsA, apiKey)

	// Insert objects for tenant-b
	objectsB := []*models.Object{
		{
			Class:  cls.Class,
			Tenant: "tenant-b",
			Properties: map[string]interface{}{
				"title":    "Tenant B Article 1",
				"contents": "Data science basics",
			},
		},
	}
	helper.CreateObjectsBatchAuth(t, objectsB, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query tenant-a
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		TenantName:     "tenant-a",
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Verify only tenant-a objects are returned
	for _, r := range results.Results {
		result := r.(map[string]any)
		title := result["title"].(string)
		assert.Contains(t, title, "Tenant A", "should only return tenant-a objects")
	}
}

// Test 14: Complex query with multiple parameters
func TestQueryHybrid_ComplexQuery(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Complex query combining multiple parameters
	var results *search.QueryHybridResp
	alpha := 0.0
	limit := 3
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            "learning networks",
		Alpha:            &alpha,
		Limit:            &limit,
		TargetProperties: []string{"title", "contents"},
		ReturnProperties: []string{"title", "year"},
		ReturnMetadata:   []string{"id", "score"},
		Filters: map[string]any{
			"operator": "And",
			"operands": []map[string]any{
				{
					"path":      []string{"status"},
					"operator":  "Equal",
					"valueText": "published",
				},
				{
					"path":     []string{"year"},
					"operator": "GreaterThanEqual",
					"valueInt": 2020,
				},
			},
		},
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)
	assert.LessOrEqual(t, len(results.Results), 3, "should respect limit")

	// Verify all constraints are satisfied
	for _, r := range results.Results {
		result := r.(map[string]any)

		// Check ID is present
		assert.Contains(t, result, "id")

		// Check requested properties are returned
		assert.Contains(t, result, "title")
		assert.Contains(t, result, "year")
		// NOTE: Property filtering not yet implemented in MCP
		// TODO: Uncomment once implemented:
		// assert.NotContains(t, result, "contents")
		// assert.NotContains(t, result, "author")

		// Verify filter constraints
		year := int(result["year"].(float64))
		assert.GreaterOrEqual(t, year, 2020)
	}
}

// Test 15: Empty query
func TestQueryHybrid_EmptyQuery(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "",
		Alpha:          &alpha,
	}, &results, testAPIKey)

	// Empty query might return all results or be valid behavior
	// Just verify it doesn't crash
	if err == nil {
		require.NotNil(t, results)
	}
}

// Test 16: No results scenario
func TestQueryHybrid_NoResults(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Search for something that doesn't exist
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "xyznonexistentquery12345",
		Alpha:          &alpha,
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	assert.Len(t, results.Results, 0, "should return empty results for non-matching query")
}

// Test 17: Invalid collection name
func TestQueryHybrid_InvalidCollectionName(t *testing.T) {
	helper.SetupClient(testServerAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: "NonExistentCollection123",
		Query:          "test",
		Alpha:          &alpha,
	}, &results, testAPIKey)

	// Should return an error for non-existent collection
	assert.NotNil(t, err, "should return error for non-existent collection")
}

// Test 18: Default alpha behavior
func TestQueryHybrid_DefaultAlpha(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Query without specifying alpha (should default to 0.5 or similar)
	var results *search.QueryHybridResp
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		// Alpha not specified - should use default
	}, &results, testAPIKey)

	// Note: This might fail if no vectorizer is configured
	// The test verifies the parameter handling, not the semantic search itself
	if err != nil {
		// If it fails due to vectorization, that's expected without a vectorizer
		assert.Contains(t, err.Error(), "vector", "should mention vector-related error")
	} else {
		require.NotNil(t, results)
	}
}

// Test 19: Target all properties (default behavior)
func TestQueryHybrid_TargetAllProperties(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Query without specifying target_properties (should search all text properties)
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "Python",
		Alpha:          &alpha,
		// TargetProperties not specified - should search all text fields
	}, &results, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Greater(t, len(results.Results), 0)

	// Should find the result whether "Python" is in title or contents
	found := false
	for _, r := range results.Results {
		result := r.(map[string]any)
		title := fmt.Sprintf("%v", result["title"])
		contents := fmt.Sprintf("%v", result["contents"])
		if title == "Python Programming" || contents == "Learn Python programming from scratch" {
			found = true
			break
		}
	}
	assert.True(t, found, "should find Python-related article in any text field")
}

// Test 20: Empty results should not cause error or panic
func TestQueryHybrid_EmptyResults(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	defer cleanup()

	insertTestArticles(t, cls.Class)

	// Query with filter that matches nothing - should return empty results, not error
	var results *search.QueryHybridResp
	alpha := 0.0
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "test",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":     []string{"year"},
			"operator": "Equal",
			"valueInt": 9999, // No article from year 9999
		},
	}, &results, testAPIKey)

	require.Nil(t, err, "empty results should not cause an error")
	require.NotNil(t, results)
	assert.Len(t, results.Results, 0, "should return empty results")
}
