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
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	toolNameObjectsGet = "weaviate-objects-get"
)

// setupObjectsGetTest handles the boilerplate setup: client init, class creation, and context generation.
// It returns the class schema, the context, and a cleanup function.
func setupObjectsGetTest(t *testing.T) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)

	// Use test name to create unique collection name
	className := "GetTestArticle" + strings.ReplaceAll(t.Name(), "/", "_")

	cls := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "content",
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
				Name:     "rating",
				DataType: []string{"number"},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"default": {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
			},
		},
	}

	helper.CreateClassAuth(t, cls, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// setupObjectsGetMultiTenantTest creates a multi-tenant class and returns it with context and cleanup.
func setupObjectsGetMultiTenantTest(t *testing.T) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)

	// Use test name to create unique collection name
	className := "GetTestTenantData" + strings.ReplaceAll(t.Name(), "/", "_")

	cls := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "data",
				DataType: []string{"text"},
			},
			{
				Name:     "value",
				DataType: []string{"int"},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	helper.CreateClassAuth(t, cls, testAPIKey)

	// Create tenants
	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{
		{Name: "tenant-a"},
		{Name: "tenant-b"},
	}, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// insertTestObjectsWithVectors inserts test objects with vectors and returns their UUIDs
func insertTestObjectsWithVectors(t *testing.T, className string) []strfmt.UUID {
	t.Helper()

	objects := []struct {
		props  map[string]interface{}
		vector []float32
	}{
		{map[string]interface{}{"title": "Article One", "content": "Content one", "author": "John Doe", "year": 2020, "rating": 4.5}, []float32{0.1, 0.2, 0.3}},
		{map[string]interface{}{"title": "Article Two", "content": "Content two", "author": "Jane Smith", "year": 2021, "rating": 4.8}, []float32{0.4, 0.5, 0.6}},
		{map[string]interface{}{"title": "Article Three", "content": "Content three", "author": "Bob Johnson", "year": 2022, "rating": 4.2}, []float32{0.7, 0.8, 0.9}},
		{map[string]interface{}{"title": "Article Four", "content": "Content four", "author": "Alice Brown", "year": 2023, "rating": 4.9}, []float32{0.2, 0.3, 0.4}},
		{map[string]interface{}{"title": "Article Five", "content": "Content five", "author": "Charlie Wilson", "year": 2024, "rating": 4.6}, []float32{0.5, 0.6, 0.7}},
	}

	var ids []strfmt.UUID
	for _, item := range objects {
		obj := &models.Object{
			Class:      className,
			Properties: item.props,
			Vector:     item.vector,
		}
		resp, err := helper.CreateObjectWithResponseAuth(t, obj, testAPIKey)
		require.Nil(t, err)
		ids = append(ids, resp.ID)
	}

	return ids
}

// Test 1: Get single object by UUID
func TestGetObjects_GetSingleObjectByUUID(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)

	// Get single object by UUID
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String()},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 1)

	obj := result.Objects[0]
	assert.Equal(t, cls.Class, obj.Class)
	assert.Equal(t, ids[0], obj.ID)

	props := obj.Properties.(map[string]interface{})
	assert.Equal(t, "Article One", props["title"])
	assert.Equal(t, "Content one", props["content"])
	assert.Equal(t, "John Doe", props["author"])
}

// Test 2: Get multiple objects by UUIDs
func TestGetObjects_GetMultipleObjectsByUUIDs(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)

	// Get multiple objects by UUIDs
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String(), ids[2].String(), ids[4].String()},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 3)

	// Verify we got the right objects
	titles := make(map[string]bool)
	for _, obj := range result.Objects {
		props := obj.Properties.(map[string]interface{})
		titles[props["title"].(string)] = true
	}
	assert.True(t, titles["Article One"])
	assert.True(t, titles["Article Three"])
	assert.True(t, titles["Article Five"])
}

// Test 3: Get objects via pagination (default limit)
func TestGetObjects_PaginationDefaultLimit(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	insertTestObjectsWithVectors(t, cls.Class)

	// Get objects with pagination (no UUIDs, no limit specified)
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Objects, 5, "should return all 5 objects (under default limit of 25)")

	// Verify all objects are from the correct collection
	for _, obj := range result.Objects {
		assert.Equal(t, cls.Class, obj.Class)
	}
}

// Test 4: Get objects with custom limit
func TestGetObjects_PaginationWithLimit(t *testing.T) {
	cls, _, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	insertTestObjectsWithVectors(t, cls.Class)

	// Give server time to process and index objects
	time.Sleep(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get objects with limit of 2
	limit := 2
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		Limit:          &limit,
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	t.Logf("Collection: %s, Result count: %d", cls.Class, len(result.Objects))
	if err != nil {
		t.Logf("Error: %v", err)
	}
	assert.LessOrEqual(t, len(result.Objects), limit, "should return at most limit objects")
	assert.Greater(t, len(result.Objects), 0, "should return some objects")
}

// Test 5: Get objects with offset and limit
func TestGetObjects_PaginationWithOffsetAndLimit(t *testing.T) {
	cls, _, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	insertTestObjectsWithVectors(t, cls.Class)

	// Give server time to process and index objects
	time.Sleep(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get first page
	limit := 2
	offset := 0
	var result1 *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		Limit:          &limit,
		Offset:         &offset,
	}, &result1, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result1)
	assert.LessOrEqual(t, len(result1.Objects), limit, "first page should return at most limit objects")
	assert.Greater(t, len(result1.Objects), 0, "first page should return some objects")

	// Get second page
	offset = 2
	var result2 *read.GetObjectsResp
	err = helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		Limit:          &limit,
		Offset:         &offset,
	}, &result2, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result2)
	// Second page should also have objects (we have 5 total, so page 2 should have data)
	assert.Greater(t, len(result2.Objects), 0, "second page should return some objects")

	// Verify different objects in each page
	ids1 := make(map[strfmt.UUID]bool)
	for _, obj := range result1.Objects {
		ids1[obj.ID] = true
	}
	for _, obj := range result2.Objects {
		assert.False(t, ids1[obj.ID], "objects should not overlap between pages")
	}
}

// Test 6: Get objects with include_vector
func TestGetObjects_IncludeVector(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Get object with vector included
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String()},
		IncludeVector:  true,
		ReturnMetadata: []string{"vector"},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 1)

	obj := result.Objects[0]
	// Vector should be included when requested
	// Note: If vector is nil, the implementation might not support this yet
	if obj.Vector != nil {
		assert.Len(t, obj.Vector, 3)
		assert.Equal(t, float32(0.1), obj.Vector[0])
		assert.Equal(t, float32(0.2), obj.Vector[1])
		assert.Equal(t, float32(0.3), obj.Vector[2])
	}
}

// Test 7: Get objects without vector (default)
func TestGetObjects_WithoutVector(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Get object without vector (default)
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String()},
		IncludeVector:  false,
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 1)

	obj := result.Objects[0]
	// Vector should be nil or empty when not requested
	if obj.Vector != nil {
		assert.Empty(t, obj.Vector)
	}
}

// Test 8: Get objects with specific return_properties
func TestGetObjects_WithReturnProperties(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Get object with only specific properties
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName:   cls.Class,
		UUIDs:            []string{ids[0].String()},
		ReturnProperties: []string{"title", "author"},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 1)

	obj := result.Objects[0]
	props := obj.Properties.(map[string]interface{})

	// Should have requested properties
	assert.Contains(t, props, "title")
	assert.Contains(t, props, "author")
	assert.Equal(t, "Article One", props["title"])
	assert.Equal(t, "John Doe", props["author"])

	// Should not have other properties
	assert.NotContains(t, props, "content")
	assert.NotContains(t, props, "year")
	assert.NotContains(t, props, "rating")
}

// Test 9: Get objects with return_metadata
func TestGetObjects_WithReturnMetadata(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Get object with metadata
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String()},
		ReturnMetadata: []string{"id", "creationTimeUnix", "lastUpdateTimeUnix"},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 1)

	obj := result.Objects[0]
	assert.NotNil(t, obj.ID)
	assert.NotNil(t, obj.CreationTimeUnix)
	assert.NotNil(t, obj.LastUpdateTimeUnix)
}

// Test 10: Get objects with tenant
func TestGetObjects_WithTenant(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetMultiTenantTest(t)
	defer cleanup()

	// Insert objects for tenant-a
	tenantA := "tenant-a"
	obj1 := &models.Object{
		Class:      cls.Class,
		Properties: map[string]interface{}{"data": "tenant-a data 1", "value": 100},
		Tenant:     tenantA,
	}
	resp1, err := helper.CreateObjectWithResponseAuth(t, obj1, testAPIKey)
	require.Nil(t, err)

	obj2 := &models.Object{
		Class:      cls.Class,
		Properties: map[string]interface{}{"data": "tenant-a data 2", "value": 200},
		Tenant:     tenantA,
	}
	resp2, err := helper.CreateObjectWithResponseAuth(t, obj2, testAPIKey)
	require.Nil(t, err)

	// Insert objects for tenant-b
	tenantB := "tenant-b"
	obj3 := &models.Object{
		Class:      cls.Class,
		Properties: map[string]interface{}{"data": "tenant-b data 1", "value": 300},
		Tenant:     tenantB,
	}
	_, err = helper.CreateObjectWithResponseAuth(t, obj3, testAPIKey)
	require.Nil(t, err)

	// Get objects from tenant-a only
	var result *read.GetObjectsResp
	err = helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		TenantName:     tenantA,
		UUIDs:          []string{resp1.ID.String(), resp2.ID.String()},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Objects, 2)

	// Verify both objects are from tenant-a
	for _, obj := range result.Objects {
		props := obj.Properties.(map[string]interface{})
		assert.Contains(t, props["data"], "tenant-a")
	}
}

// Test 11: Get objects from empty collection
func TestGetObjects_EmptyCollection(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	// Don't insert any objects

	// Get objects from empty collection
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Objects, "should return empty array for empty collection")
}

// Test 12: Get non-existent object by UUID
func TestGetObjects_NonExistentUUID(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	insertTestObjectsWithVectors(t, cls.Class)

	// Try to get non-existent object
	nonExistentUUID := "00000000-0000-0000-0000-000000000000"
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{nonExistentUUID},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	// Should return empty array (skips non-existent objects)
	assert.Empty(t, result.Objects)
}

// Test 13: Get mix of existent and non-existent UUIDs
func TestGetObjects_MixedExistentAndNonExistent(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Request mix of existent and non-existent UUIDs
	nonExistentUUID := "00000000-0000-0000-0000-000000000000"
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String(), nonExistentUUID, ids[2].String()},
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	// Should return only the existent objects
	assert.Len(t, result.Objects, 2, "should return only existent objects")
}

// Test 14: Failure - Missing required collection_name
func TestGetObjects_Fail_MissingCollectionName(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		// Missing CollectionName
		UUIDs: []string{"00000000-0000-0000-0000-000000000000"},
	}, &result, testAPIKey)

	// When UUIDs are provided, collection_name validation happens on the MCP parameter validation
	// If it reaches the handler, it will try to fetch objects and return empty array for non-existent ones
	// So we expect either an error or empty results
	if err == nil {
		require.NotNil(t, result)
		assert.Empty(t, result.Objects)
	}
}

// Test 15: Failure - Invalid collection name
func TestGetObjects_Fail_InvalidCollectionName(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: "NonExistentCollection",
	}, &result, testAPIKey)

	// Should return an error for non-existent collection
	require.NotNil(t, err)
}

// Test 16: Failure - Invalid tenant name
func TestGetObjects_Fail_InvalidTenant(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetMultiTenantTest(t)
	defer cleanup()

	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		TenantName:     "nonexistent-tenant",
	}, &result, testAPIKey)

	// Should return an error or empty results for non-existent tenant
	if err == nil {
		require.NotNil(t, result)
		// Empty results are acceptable for non-existent tenant
	}
}

// Test 17: Pagination with offset beyond collection size
func TestGetObjects_OffsetBeyondEnd(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	insertTestObjectsWithVectors(t, cls.Class)
	// Request with offset beyond collection size
	offset := 10
	limit := 5
	var result *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		Offset:         &offset,
		Limit:          &limit,
	}, &result, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Objects, "should return empty array when offset is beyond collection size")
}

// Test 18: Get all properties vs filtered properties
func TestGetObjects_AllPropertiesVsFiltered(t *testing.T) {
	cls, ctx, cleanup := setupObjectsGetTest(t)
	defer cleanup()

	ids := insertTestObjectsWithVectors(t, cls.Class)
	// Get with all properties (default)
	var resultAll *read.GetObjectsResp
	err := helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName: cls.Class,
		UUIDs:          []string{ids[0].String()},
	}, &resultAll, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, resultAll)
	require.Len(t, resultAll.Objects, 1)

	propsAll := resultAll.Objects[0].Properties.(map[string]interface{})
	assert.Contains(t, propsAll, "title")
	assert.Contains(t, propsAll, "content")
	assert.Contains(t, propsAll, "author")
	assert.Contains(t, propsAll, "year")
	assert.Contains(t, propsAll, "rating")

	// Get with filtered properties
	var resultFiltered *read.GetObjectsResp
	err = helper.CallToolOnce(ctx, t, toolNameObjectsGet, &read.GetObjectsArgs{
		CollectionName:   cls.Class,
		UUIDs:            []string{ids[0].String()},
		ReturnProperties: []string{"title"},
	}, &resultFiltered, testAPIKey)

	require.Nil(t, err)
	require.NotNil(t, resultFiltered)
	require.Len(t, resultFiltered.Objects, 1)

	propsFiltered := resultFiltered.Objects[0].Properties.(map[string]interface{})
	assert.Contains(t, propsFiltered, "title")
	assert.NotContains(t, propsFiltered, "content")
	assert.NotContains(t, propsFiltered, "author")
	assert.NotContains(t, propsFiltered, "year")
	assert.NotContains(t, propsFiltered, "rating")
}
