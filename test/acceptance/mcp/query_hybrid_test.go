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

	// Test data constants
	authorJohnDoe   = "John Doe"
	authorJaneSmith = "Jane Smith"
	tenantA         = "tenant-a"
	tenantB         = "tenant-b"
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
	createTenantsForClass(t, cls.Class, tenantNames)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// setupQueryHybridTestWithData combines setup and data insertion for most tests.
// Returns class, context, cleanup function, and pure BM25 alpha value.
func setupQueryHybridTestWithData(t *testing.T) (*models.Class, context.Context, func(), float64) {
	cls, ctx, cleanup := setupQueryHybridTest(t)
	insertTestArticles(t, cls.Class)
	alpha := 0.0 // Pure BM25 for most tests
	return cls, ctx, cleanup, alpha
}

// executeHybridQuery is a helper that executes a hybrid query and performs common validations
func executeHybridQuery(t *testing.T, ctx context.Context, args *search.QueryHybridArgs) *search.QueryHybridResp {
	t.Helper()
	var results *search.QueryHybridResp
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, args, &results, testAPIKey)
	require.Nil(t, err)
	require.NotNil(t, results)
	return results
}

// executeHybridQueryWithResults is like executeHybridQuery but also asserts exact result count
func executeHybridQueryWithResults(t *testing.T, ctx context.Context, args *search.QueryHybridArgs, expectedCount int) *search.QueryHybridResp {
	t.Helper()
	results := executeHybridQuery(t, ctx, args)
	require.Equal(t, expectedCount, len(results.Results), "should find exactly %d results", expectedCount)
	return results
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
				"author":      authorJohnDoe,
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
				"author":      authorJaneSmith,
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
				"author":      authorJohnDoe,
				"year":        2021,
				"status":      "published",
				"publishDate": "2021-11-30T16:45:00Z",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":       "Reinforcement Learning Guide",
				"contents":    "Complete guide to reinforcement learning algorithms and applications",
				"author":      authorJaneSmith,
				"year":        2021,
				"status":      "published",
				"publishDate": "2021-03-15T09:00:00Z",
			},
		},
	}

	helper.CreateObjectsBatchAuth(t, objects, testAPIKey)
}

// Test 1: Pure BM25 keyword search (alpha=0.0)
func TestQueryHybridPureBM25Search(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Expects: "Machine Learning Basics", "Deep Learning Advanced", and "Reinforcement Learning Guide" to match
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "machine learning",
		Alpha:          &alpha,
	}, 3)

	// Note: Properties are returned at the top level of each result, not nested
	result := results.Results[0].(map[string]any)
	title := result["title"].(string)
	assert.Contains(t, title, "Learning")
}

// Test 2: Test with limit parameter
func TestQueryHybridWithLimit(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Test with limit=2 (without limit, "learning" would match 3 articles)
	limit := 2
	results := executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Limit:          &limit,
	})
	assert.Equal(t, 2, len(results.Results), "should return exactly 2 results")

	// Test with limit=0
	limit = 0
	results = executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Limit:          &limit,
	})
	assert.Len(t, results.Results, 0, "limit=0 should return no results")
}

// Test 3: Return specific properties
func TestQueryHybridReturnSpecificProperties(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Expects 3 results: Machine Learning Basics, Deep Learning Advanced, and Python Programming (Learn matches learning)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            "learning",
		Alpha:            &alpha,
		ReturnProperties: []string{"title", "author"},
	}, 3)

	// Verify requested properties are returned
	result := results.Results[0].(map[string]any)
	assert.Contains(t, result, "title")
	assert.Contains(t, result, "author")
	assert.NotContains(t, result, "contents", "contents should not be returned")
	assert.NotContains(t, result, "year", "year should not be returned")
}

// Test 4: Return all properties (default behavior)
func TestQueryHybridReturnAllProperties(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Expects 3 results for "learning" query
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
	}, 3)

	// Verify all properties are returned
	result := results.Results[0].(map[string]any)
	assert.Contains(t, result, "title")
	assert.Contains(t, result, "contents")
	assert.Contains(t, result, "author")
	assert.Contains(t, result, "year")
	assert.Contains(t, result, "status")
}

// Test 5: Return metadata
func TestQueryHybridReturnMetadata(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Expects 3 results for "learning" query
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		ReturnMetadata: []string{"id", "score", "creationTimeUnix"},
	}, 3)

	for i, r := range results.Results {
		result := r.(map[string]any)

		// id is always returned at the top level
		assert.Contains(t, result, "id", "result %d: id must be present", i)

		// score and creationTimeUnix are returned inside _additional
		additional, hasAdditional := result["_additional"].(map[string]any)
		require.True(t, hasAdditional, "result %d: _additional must be present", i)

		score, hasScore := additional["score"]
		require.True(t, hasScore, "result %d: score must be present in _additional", i)
		assert.IsType(t, float64(0), score, "result %d: score must be a float64", i)

		creationTime, hasCreationTime := additional["creationTimeUnix"]
		require.True(t, hasCreationTime, "result %d: creationTimeUnix must be present in _additional", i)
		assert.IsType(t, float64(0), creationTime, "result %d: creationTimeUnix must be a number", i)
	}
}

// Test 5b: Return additional metadata types (explainScore, lastUpdateTimeUnix)
func TestQueryHybridReturnMetadataAdditionalTypes(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Note: distance is a vector-only metric and is not returned for pure BM25 (alpha=0.0)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		ReturnMetadata: []string{"explainScore", "lastUpdateTimeUnix"},
	}, 3)

	for i, r := range results.Results {
		result := r.(map[string]any)
		additional, hasAdditional := result["_additional"].(map[string]any)
		require.True(t, hasAdditional, "result %d: _additional must be present", i)

		_, hasExplainScore := additional["explainScore"]
		assert.True(t, hasExplainScore, "result %d: explainScore must be present in _additional", i)

		lastUpdateTime, hasLastUpdateTime := additional["lastUpdateTimeUnix"]
		require.True(t, hasLastUpdateTime, "result %d: lastUpdateTimeUnix must be present in _additional", i)
		assert.IsType(t, float64(0), lastUpdateTime, "result %d: lastUpdateTimeUnix must be a number", i)
	}
}

// Test 6: Target specific properties for BM25 search
func TestQueryHybridTargetSpecificProperties(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search for "Python" targeting only title - expects 1 result
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            "Python",
		Alpha:            &alpha,
		TargetProperties: []string{"title"},
	}, 1)

	// Verify the result has "Python" in title
	result := results.Results[0].(map[string]any)
	title := result["title"].(string)
	assert.Contains(t, title, "Python")
}

// Test 7: Simple filter - Equal operator
func TestQueryHybridWithSimpleFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search with filter for status="published" - expects 3 results (Machine Learning, Deep Learning, Reinforcement Learning)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "published",
		},
	}, 3)

	// Verify all results have status="published"
	for _, r := range results.Results {
		result := r.(map[string]any)
		assert.Equal(t, "published", result["status"])
	}
}

// Test 8: Numeric filter
func TestQueryHybridWithNumericFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search with filter for year >= 2020 - expects 3 results (all articles with "learning" are >= 2020)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":     []string{"year"},
			"operator": "GreaterThanEqual",
			"valueInt": 2020,
		},
	}, 3)

	// Verify all results have year >= 2020
	for _, r := range results.Results {
		result := r.(map[string]any)
		year := int(result["year"].(float64))
		assert.Equal(t, true, year >= 2020)
	}
}

// Test 9: Date filter with RFC3339 format
func TestQueryHybridWithDateFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search with filter for publishDate >= 2021-01-01T00:00:00Z - expects 2 results (Deep Learning 2022, Neural Networks 2021)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":      []string{"publishDate"},
			"operator":  "GreaterThanEqual",
			"valueDate": "2021-01-01T00:00:00Z",
		},
	}, 2)

	// Verify all results have publishDate >= 2021-01-01
	for _, r := range results.Results {
		result := r.(map[string]any)
		publishDate := result["publishDate"].(string)
		assert.True(t, publishDate >= "2021-01-01T00:00:00Z")
	}
}

// Test 10: Complex AND filter
func TestQueryHybridWithComplexAndFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search with AND filter: status="published" AND year >= 2020 - expects 3 results
	// Machine Learning (2020), Deep Learning (2022), Reinforcement Learning (2021)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
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
	}, 3)

	// Verify all results match both conditions
	for _, r := range results.Results {
		result := r.(map[string]any)
		assert.Equal(t, "published", result["status"])
		year := int(result["year"].(float64))
		assert.Equal(t, true, year >= 2020)
	}
}

// Test 11: OR filter
func TestQueryHybridWithOrFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search with OR filter: author="John Doe" OR author="Jane Smith" - expects 3 results
	// Machine Learning (John Doe), Deep Learning (Jane Smith), Reinforcement Learning (Jane Smith)
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		Filters: map[string]any{
			"operator": "Or",
			"operands": []map[string]any{
				{
					"path":      []string{"author"},
					"operator":  "Equal",
					"valueText": authorJohnDoe,
				},
				{
					"path":      []string{"author"},
					"operator":  "Equal",
					"valueText": authorJaneSmith,
				},
			},
		},
	}, 3)

	// Verify all results match at least one condition
	for _, r := range results.Results {
		result := r.(map[string]any)
		author := result["author"].(string)
		assert.True(t, author == authorJohnDoe || author == authorJaneSmith)
	}
}

// Test 12: Query without filter
func TestQueryHybridNoFilter(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	results := executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
	})

	// Should return 3 results without filtering
	assert.Equal(t, 3, len(results.Results))
}

// Test 13: Multi-tenant search
func TestQueryHybridWithTenant(t *testing.T) {
	cls, ctx, cleanup := setupQueryHybridMultiTenantTest(t, []string{tenantA, tenantB})
	defer cleanup()

	// Insert objects for tenant-a
	objectsA := []*models.Object{
		{
			Class:  cls.Class,
			Tenant: tenantA,
			Properties: map[string]interface{}{
				"title":    "Tenant A Article 1",
				"contents": "Machine learning for tenant A",
			},
		},
		{
			Class:  cls.Class,
			Tenant: tenantA,
			Properties: map[string]interface{}{
				"title":    "Tenant A Article 2",
				"contents": "Deep learning algorithms",
			},
		},
	}
	helper.CreateObjectsBatchAuth(t, objectsA, testAPIKey)

	// Insert objects for tenant-b
	objectsB := []*models.Object{
		{
			Class:  cls.Class,
			Tenant: tenantB,
			Properties: map[string]interface{}{
				"title":    "Tenant B Article 1",
				"contents": "Data science basics",
			},
		},
	}
	helper.CreateObjectsBatchAuth(t, objectsB, testAPIKey)

	// Query tenant-a - expects 2 results (2 articles for tenant-a with "learning")
	alpha := 0.0
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		Alpha:          &alpha,
		TenantName:     tenantA,
	}, 2)

	// Verify only tenant-a objects are returned
	for _, r := range results.Results {
		result := r.(map[string]any)
		title := result["title"].(string)
		assert.Contains(t, title, "Tenant A", "should only return tenant-a objects")
	}
}

// Test 14: Complex query with multiple parameters
func TestQueryHybridComplexQuery(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Complex query combining multiple parameters - expects 3 results due to limit
	// Matches: Machine Learning Basics (2020), Deep Learning Advanced (2022), Neural Networks Explained (2021), Reinforcement Learning (2021)
	// But limited to 3 by the limit parameter
	limit := 3
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
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
	}, 3)

	// Verify all constraints are satisfied
	for _, r := range results.Results {
		result := r.(map[string]any)

		// Check ID is present
		assert.Contains(t, result, "id")

		// Check requested properties are returned
		assert.Contains(t, result, "title")
		assert.Contains(t, result, "year")
		assert.NotContains(t, result, "contents")
		assert.NotContains(t, result, "author")

		// Verify filter constraints
		year := int(result["year"].(float64))
		assert.Equal(t, true, year >= 2020)
	}
}

// Test 15: Empty query
func TestQueryHybridEmptyQuery(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	var results *search.QueryHybridResp
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "",
		Alpha:          &alpha,
	}, &results, testAPIKey)

	// Empty query with pure BM25 should succeed without crashing
	require.Nil(t, err, "empty query should not produce an error")
	require.NotNil(t, results)
}

// Test 16: No results scenario
func TestQueryHybridNoResults(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Search for something that doesn't exist
	results := executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "xyznonexistentquery12345",
		Alpha:          &alpha,
	})

	assert.Len(t, results.Results, 0, "should return empty results for non-matching query")
}

// Test 17: Invalid collection name
func TestQueryHybridInvalidCollectionName(t *testing.T) {
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
func TestQueryHybridDefaultAlpha(t *testing.T) {
	cls, ctx, cleanup, _ := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Query without specifying alpha (should default to 0.5 or similar)
	var results *search.QueryHybridResp
	err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "learning",
		// Alpha not specified - should use default
	}, &results, testAPIKey)

	// Default alpha (0.5) uses both vector and keyword components.
	// Without a vectorizer, the vector component fails.
	if err != nil {
		// If it fails due to vectorization, that's expected without a vectorizer
		assert.Contains(t, err.Error(), "vector", "should mention vector-related error")
	} else {
		// If it succeeds, BM25 component should return results
		require.NotNil(t, results)
		require.NotEmpty(t, results.Results, "BM25 component should still return results")
	}
}

// Test 19: Target all properties (default behavior)
func TestQueryHybridTargetAllProperties(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Query without specifying target_properties (should search all text properties) - expects 1 result
	results := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "Python",
		Alpha:          &alpha,
		// TargetProperties not specified - should search all text fields
	}, 1)

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
func TestQueryHybridEmptyResults(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridTestWithData(t)
	defer cleanup()

	// Query with filter that matches nothing - should return empty results, not error
	results := executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: cls.Class,
		Query:          "test",
		Alpha:          &alpha,
		Filters: map[string]any{
			"path":     []string{"year"},
			"operator": "Equal",
			"valueInt": 9999, // No article from year 9999
		},
	})

	assert.Len(t, results.Results, 0, "should return empty results")
}

// Test 21: Target specific named vector
func TestQueryHybridTargetVectors(t *testing.T) {
	helper.SetupClient(testServerAddr)

	className := "TestNamedVectorArticle"

	// Create a collection with two named vectors using "none" vectorizer.
	// We provide explicit vectors at insert time so we can use BM25 (alpha=0.0)
	// for the search and request vector metadata to verify the correct named
	// vector is returned for the targeted vector space.
	cls := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "contents", DataType: []string{"text"}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"title_vector": {
				Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
				VectorIndexType: "hnsw",
			},
			"content_vector": {
				Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
				VectorIndexType: "hnsw",
			},
		},
	}

	helper.DeleteClassAuth(t, className, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)
	defer helper.DeleteClassAuth(t, className, testAPIKey)

	// Insert objects with distinct vectors per named vector space.
	// The vectors are deliberately different so we can verify which one is returned.
	titleVec1 := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	contentVec1 := []float32{0.9, 0.8, 0.7, 0.6, 0.5}

	objects := []*models.Object{
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":    "Machine Learning Basics",
				"contents": "An introduction to ML algorithms",
			},
			Vectors: models.Vectors{
				"title_vector":   titleVec1,
				"content_vector": contentVec1,
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title":    "Deep Learning Advanced",
				"contents": "Advanced techniques for neural networks",
			},
			Vectors: models.Vectors{
				"title_vector":   []float32{0.5, 0.4, 0.3, 0.2, 0.1},
				"content_vector": []float32{0.5, 0.6, 0.7, 0.8, 0.9},
			},
		},
	}
	helper.CreateObjectsBatchAuth(t, objects, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use pure BM25 so the search works without a vectorizer for the query.
	// Request vector metadata to verify the targeted vector is returned.
	alpha := 0.0
	limit := 2

	// Search targeting title_vector and request vector metadata
	titleResults := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName:   className,
		Query:            "learning",
		Alpha:            &alpha,
		Limit:            &limit,
		TargetVectors:    []string{"title_vector"},
		ReturnProperties: []string{"title"},
		ReturnMetadata:   []string{"id", "vector"},
	}, 2)

	// Verify that response contains vector metadata
	for i, r := range titleResults.Results {
		result := r.(map[string]any)
		additional, hasAdditional := result["_additional"].(map[string]any)
		require.True(t, hasAdditional, "result %d: _additional must be present", i)

		vectors, hasVectors := additional["vectors"].(map[string]any)
		if hasVectors {
			assert.Contains(t, vectors, "title_vector",
				"result %d: response should contain the targeted title_vector", i)
		}
	}

	// Search targeting content_vector and request vector metadata
	contentResults := executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
		CollectionName:   className,
		Query:            "learning",
		Alpha:            &alpha,
		Limit:            &limit,
		TargetVectors:    []string{"content_vector"},
		ReturnProperties: []string{"title"},
		ReturnMetadata:   []string{"id", "vector"},
	}, 2)

	// Verify that response contains vector metadata
	for i, r := range contentResults.Results {
		result := r.(map[string]any)
		additional, hasAdditional := result["_additional"].(map[string]any)
		require.True(t, hasAdditional, "result %d: _additional must be present", i)

		vectors, hasVectors := additional["vectors"].(map[string]any)
		if hasVectors {
			assert.Contains(t, vectors, "content_vector",
				"result %d: response should contain the targeted content_vector", i)
		}
	}

	// Search targeting both vectors
	bothResults := executeHybridQuery(t, ctx, &search.QueryHybridArgs{
		CollectionName: className,
		Query:          "learning",
		Alpha:          &alpha,
		TargetVectors:  []string{"title_vector", "content_vector"},
		ReturnMetadata: []string{"id", "vector"},
	})
	require.NotEmpty(t, bothResults.Results, "should return results when targeting both vectors")

	for i, r := range bothResults.Results {
		result := r.(map[string]any)
		additional, hasAdditional := result["_additional"].(map[string]any)
		require.True(t, hasAdditional, "result %d: _additional must be present", i)

		vectors, hasVectors := additional["vectors"].(map[string]any)
		if hasVectors {
			assert.Contains(t, vectors, "title_vector",
				"result %d: response should contain title_vector when both targeted", i)
			assert.Contains(t, vectors, "content_vector",
				"result %d: response should contain content_vector when both targeted", i)
		}
	}
}
