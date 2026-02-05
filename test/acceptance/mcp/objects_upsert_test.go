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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	toolNameUpsert = "weaviate-objects-upsert"
)

// setupUpsertTest handles the boilerplate setup: client init, class creation, and context generation.
// It returns the class schema, the context, and a cleanup function.
func setupUpsertTest(t *testing.T) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)
	cls := articles.ParagraphsClass()

	// Clean start
	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// setupUpsertTestWithTenant creates a multi-tenant class for testing
func setupUpsertTestWithTenant(t *testing.T, tenantNames []string) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)
	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}

	// Clean start
	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	// Create tenants
	createTenantsForClass(t, cls.Class, tenantNames)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// setupUpsertTestWithComplexSchema creates a class with various property types for validation testing
func setupUpsertTestWithComplexSchema(t *testing.T) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)
	cls := &models.Class{
		Class: "ComplexObject",
		Properties: []*models.Property{
			{
				Name:     "textProp",
				DataType: []string{"text"},
			},
			{
				Name:     "intProp",
				DataType: []string{"int"},
			},
			{
				Name:     "numberProp",
				DataType: []string{"number"},
			},
			{
				Name:     "boolProp",
				DataType: []string{"boolean"},
			},
			{
				Name:     "dateProp",
				DataType: []string{"date"},
			},
			{
				Name:     "textArrayProp",
				DataType: []string{"text[]"},
			},
			{
				Name:     "intArrayProp",
				DataType: []string{"int[]"},
			},
		},
		Vectorizer: "none",
	}

	// Clean start
	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

// assertPartialBatchResults checks results for partial failures
func assertPartialBatchResults(t *testing.T, resp *create.UpsertObjectResp, expectedSuccess, expectedFails int) {
	t.Helper()
	require.NotNil(t, resp)
	require.Len(t, resp.Results, expectedSuccess+expectedFails)

	successCount := 0
	failCount := 0
	for _, result := range resp.Results {
		if result.Error != "" {
			failCount++
		} else {
			successCount++
			require.NotEmpty(t, result.ID)
		}
	}
	assert.Equal(t, expectedSuccess, successCount, "unexpected number of successful upserts")
	assert.Equal(t, expectedFails, failCount, "unexpected number of failed upserts")
}

// validateUpsertResults checks that the response exists, has the correct number of results,
// and that none of the results contain errors.
func validateUpsertResults(t *testing.T, resp *create.UpsertObjectResp, expectedCount int) {
	require.NotNil(t, resp)
	require.Len(t, resp.Results, expectedCount)
	for i, result := range resp.Results {
		require.Empty(t, result.Error, "object %d should not have error", i)
		require.NotEmpty(t, result.ID, "object %d should have UUID", i)
	}
}

// createTestObject helper creates a basic object in Weaviate for update tests
func createTestObject(t *testing.T, class, contents, title string) *models.Object {
	obj := &models.Object{
		Class: class,
		Properties: map[string]interface{}{
			"contents": contents,
			"title":    title,
		},
	}
	created, err := helper.CreateObjectWithResponseAuth(t, obj, testAPIKey)
	require.Nil(t, err)
	return created
}

func TestUpsertToolInsertOneObject(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Insert a single object
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Article Content",
					"title":    "Test Article",
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 1)

	// Verify the object was created
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Test Article Content", obj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Test Article", obj.Properties.(map[string]interface{})["title"])
}

func TestUpsertToolInsertMultipleObjects(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Insert multiple objects in a batch
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "First Article Content",
					"title":    "First Article",
				},
			},
			{
				Properties: map[string]any{
					"contents": "Second Article Content",
					"title":    "Second Article",
				},
			},
			{
				Properties: map[string]any{
					"contents": "Third Article Content",
					"title":    "Third Article",
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 3)

	// Verify all objects were created successfully
	expectedContents := []string{"First Article Content", "Second Article Content", "Third Article Content"}
	for i, result := range resp.Results {
		uuid := strfmt.UUID(result.ID)
		obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
		require.Nil(t, err)
		assert.Equal(t, expectedContents[i], obj.Properties.(map[string]interface{})["contents"])
	}
}

func TestUpsertToolUpdateOneObject(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// First, create an object to update
	createdObj := createTestObject(t, cls.Class, "Original Content", "Original Title")
	existingUUID := createdObj.ID.String()

	// Now update the object using upsert with the same UUID
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				UUID: existingUUID,
				Properties: map[string]any{
					"contents": "Updated Content",
					"title":    "Updated Title",
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 1)
	require.Equal(t, existingUUID, resp.Results[0].ID, "should return same UUID")

	// Verify the object was updated
	uuid := strfmt.UUID(existingUUID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "Updated Content", obj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Updated Title", obj.Properties.(map[string]interface{})["title"])
}

func TestUpsertToolUpdateMultipleObjects(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// First, create multiple objects to update
	var createdIDs []strfmt.UUID
	var updateArgs []create.ObjectToUpsert
	var expectedUUIDs []string

	for i := 1; i <= 3; i++ {
		// Create initial object
		obj := createTestObject(t, cls.Class,
			fmt.Sprintf("Original Content %d", i),
			fmt.Sprintf("Original Title %d", i))
		createdIDs = append(createdIDs, obj.ID)
		expectedUUIDs = append(expectedUUIDs, obj.ID.String())

		// Prepare update arg
		updateArgs = append(updateArgs, create.ObjectToUpsert{
			UUID: obj.ID.String(),
			Properties: map[string]any{
				"contents": fmt.Sprintf("Updated Content %d", i),
				"title":    fmt.Sprintf("Updated Title %d", i),
			},
		})
	}

	// Now update all objects in a batch
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects:        updateArgs,
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 3)

	// Verify UUIDs and content
	for i, result := range resp.Results {
		assert.Equal(t, expectedUUIDs[i], result.ID)

		updatedObj, err := helper.GetObjectAuth(t, cls.Class, createdIDs[i], testAPIKey)
		require.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("Updated Content %d", i+1), updatedObj.Properties.(map[string]interface{})["contents"])
	}
}

func TestUpsertToolMixedInsertAndUpdate(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Create one object to be updated later
	createdObj := createTestObject(t, cls.Class, "Existing Content", "Existing Title")
	existingUUID := createdObj.ID.String()

	// Perform a mixed batch: update existing object and insert new ones
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				UUID: existingUUID,
				Properties: map[string]any{
					"contents": "Updated Existing Content",
					"title":    "Updated Existing Title",
				},
			},
			{
				// No UUID - will be inserted
				Properties: map[string]any{
					"contents": "New Content 1",
					"title":    "New Title 1",
				},
			},
			{
				// No UUID - will be inserted
				Properties: map[string]any{
					"contents": "New Content 2",
					"title":    "New Title 2",
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 3)

	// Verify first object was updated (should have same UUID)
	assert.Equal(t, existingUUID, resp.Results[0].ID)
	updatedObj, err := helper.GetObjectAuth(t, cls.Class, createdObj.ID, testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "Updated Existing Content", updatedObj.Properties.(map[string]interface{})["contents"])

	// Verify second object was inserted (new UUID)
	assert.NotEqual(t, existingUUID, resp.Results[1].ID)
	newObj1, err := helper.GetObjectAuth(t, cls.Class, strfmt.UUID(resp.Results[1].ID), testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "New Content 1", newObj1.Properties.(map[string]interface{})["contents"])
}

func TestUpsertToolWithVectors(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Insert object with custom vector
	var resp *create.UpsertObjectResp
	customVector := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Article with custom vector",
					"title":    "Vector Test",
				},
				Vectors: map[string][]float32{
					"default": customVector,
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)

	validateUpsertResults(t, resp, 1)

	// Verify the object was created with the custom vector
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey, "vector")
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Article with custom vector", obj.Properties.(map[string]interface{})["contents"])
	// Verify the vector was set (if returned)
	if obj.Vector != nil {
		assert.Len(t, obj.Vector, len(customVector))
	}
}

func TestUpsertToolEmptyBatch(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to upsert with empty objects array
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects:        []create.ObjectToUpsert{},
	}, &resp, testAPIKey)

	// Should return an error for empty batch
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "at least one object is required")
}

func TestUpsertToolNonExistentCollection(t *testing.T) {
	_, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to upsert to a non-existent collection
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: "NonExistentCollection9999",
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return success but with error in results
	require.Nil(t, err, "should not return error at function level")
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error in result")
	assert.Contains(t, resp.Results[0].Error, "not present in schema")
}

// Multi-tenancy tests
func TestUpsertToolWithTenant(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithTenant(t, []string{"tenant1", "tenant2"})
	defer cleanup()

	// Insert object for tenant1
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		TenantName:     "tenant1",
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Tenant 1 Content",
					"title":    "Tenant 1 Title",
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Verify object exists for tenant1
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuthWithTenant(t, cls.Class, uuid, "tenant1", testAPIKey)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Tenant 1 Content", obj.Properties.(map[string]interface{})["contents"])

	// Verify object doesn't exist for tenant2
	_, err = helper.GetObjectAuthWithTenant(t, cls.Class, uuid, "tenant2", testAPIKey)
	require.NotNil(t, err, "object should not exist in tenant2")
}

func TestUpsertToolInvalidTenant(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithTenant(t, []string{"tenant1"})
	defer cleanup()

	// Try to upsert to non-existent tenant
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		TenantName:     "nonexistent",
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return success but with error in results
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error for non-existent tenant")
}

func TestUpsertToolMissingTenantWhenRequired(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithTenant(t, []string{"tenant1"})
	defer cleanup()

	// Try to upsert without tenant name when multi-tenancy is enabled
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		// TenantName is missing
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return success but with error in results
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error when tenant is missing")
}

// Data Validation Tests

func TestUpsertToolInvalidUUID(t *testing.T) {
	_, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to upsert with invalid UUID format
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: "Paragraph",
		Objects: []create.ObjectToUpsert{
			{
				UUID: "not-a-valid-uuid",
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return error at function level for invalid UUID
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid UUID")
}

func TestUpsertToolMissingRequiredProperty(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to upsert with missing properties (empty properties map)
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					// Only title, missing contents (though neither is strictly required in the schema)
					"title": "Test Title",
				},
			},
		},
	}, &resp, testAPIKey)

	// Should succeed even with missing properties (Weaviate allows this)
	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)
}

func TestUpsertToolInvalidPropertyType(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithComplexSchema(t)
	defer cleanup()

	// Try to upsert with wrong property type (string where int expected)
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"textProp": "Valid Text",
					"intProp":  "not-an-integer", // Wrong type
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return success but with error in results
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error for type mismatch")
}

func TestUpsertToolDatePropertyRFC3339(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithComplexSchema(t)
	defer cleanup()

	// Test valid RFC3339 date
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"textProp": "Test",
					"dateProp": "2023-01-15T10:30:00Z", // Valid RFC3339
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Test invalid date format (date only, no time)
	err = helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"textProp": "Test",
					"dateProp": "2023-01-15", // Invalid - missing time
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error for invalid date format")
}

// Vector Operations Tests
func TestUpsertToolMultipleNamedVectors(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Test that providing multiple named vectors returns error for single-vector schema
	var resp *create.UpsertObjectResp
	textVector := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	imageVector := []float32{0.6, 0.7, 0.8, 0.9, 1.0}

	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
				Vectors: map[string][]float32{
					"text":  textVector,
					"image": imageVector,
				},
			},
		},
	}, &resp, testAPIKey)

	// Should return success but with error in results for unsupported named vectors
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.NotEmpty(t, resp.Results[0].Error, "should have error for unsupported named vector")
	assert.Contains(t, resp.Results[0].Error, "image")
}

func TestUpsertToolVectorDimensionMismatch(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// First insert with 5-dimensional vector
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "First Object",
					"title":    "First",
				},
				Vectors: map[string][]float32{
					"default": {0.1, 0.2, 0.3, 0.4, 0.5},
				},
			},
		},
	}, &resp, testAPIKey)
	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Try to insert with different dimension (should fail or warn)
	err = helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Second Object",
					"title":    "Second",
				},
				Vectors: map[string][]float32{
					"default": {0.1, 0.2, 0.3}, // Different dimension
				},
			},
		},
	}, &resp, testAPIKey)

	// May succeed or fail depending on Weaviate config
	// If it succeeds, it should have error in results
	if err == nil && resp != nil && len(resp.Results) > 0 {
		// Check if there's an error in the result
		if resp.Results[0].Error != "" {
			assert.Contains(t, resp.Results[0].Error, "dimension")
		}
	}
}

// Error Handling Tests
func TestUpsertToolPartialBatchFailure(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithComplexSchema(t)
	defer cleanup()

	// Batch with some valid and some invalid objects
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				// Valid object
				Properties: map[string]any{
					"textProp": "Valid Text 1",
					"intProp":  42,
				},
			},
			{
				// Invalid object - wrong type
				Properties: map[string]any{
					"textProp": "Valid Text 2",
					"intProp":  "not-an-integer",
				},
			},
			{
				// Valid object
				Properties: map[string]any{
					"textProp": "Valid Text 3",
					"intProp":  100,
				},
			},
		},
	}, &resp, testAPIKey)

	require.Nil(t, err)
	assertPartialBatchResults(t, resp, 2, 1)

	// Verify the valid objects were created
	for _, result := range resp.Results {
		if result.Error == "" {
			uuid := strfmt.UUID(result.ID)
			obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
			require.Nil(t, err)
			require.NotNil(t, obj)
		}
	}
}

func TestUpsertToolUnauthorized(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to upsert with wrong API key
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
			},
		},
	}, &resp, "wrong-api-key")

	// Should return authorization error
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "unauthorized")
}

func TestUpsertToolLargeBatch(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Create a large batch (100 objects)
	batchSize := 100
	objects := make([]create.ObjectToUpsert, batchSize)
	for i := 0; i < batchSize; i++ {
		objects[i] = create.ObjectToUpsert{
			Properties: map[string]any{
				"contents": fmt.Sprintf("Content %d", i),
				"title":    fmt.Sprintf("Title %d", i),
			},
		}
	}

	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects:        objects,
	}, &resp, testAPIKey)

	require.Nil(t, err)
	validateUpsertResults(t, resp, batchSize)
}

func TestUpsertToolInvalidVectorValues(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Test empty vector array
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
				},
				Vectors: map[string][]float32{
					"default": {}, // Empty vector
				},
			},
		},
	}, &resp, testAPIKey)

	// Should succeed or have error in results
	if err == nil {
		require.NotNil(t, resp)
		// May or may not have error depending on Weaviate validation
	}
}

// Edge cases tests
func TestUpsertToolNoOpUpdate(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Create an object
	createdObj := createTestObject(t, cls.Class, "Original Content", "Original Title")
	existingUUID := createdObj.ID.String()

	// Update with exact same properties (no-op)
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				UUID: existingUUID,
				Properties: map[string]any{
					"contents": "Original Content",
					"title":    "Original Title",
				},
			},
		},
	}, &resp, testAPIKey)

	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)
	require.Equal(t, existingUUID, resp.Results[0].ID)

	// Verify object still has same properties
	obj, err := helper.GetObjectAuth(t, cls.Class, createdObj.ID, testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "Original Content", obj.Properties.(map[string]interface{})["contents"])
}

func TestUpsertToolArrayProperties(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithComplexSchema(t)
	defer cleanup()

	// Insert object with array properties
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"textProp":      "Test",
					"textArrayProp": []string{"value1", "value2", "value3"},
					"intArrayProp":  []int{1, 2, 3, 4, 5},
				},
			},
		},
	}, &resp, testAPIKey)

	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Verify arrays were stored correctly
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	assert.NotNil(t, obj.Properties.(map[string]interface{})["textArrayProp"])
	assert.NotNil(t, obj.Properties.(map[string]interface{})["intArrayProp"])
}

func TestUpsertToolNestedObjectProperties(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Try to insert with nested object (should be flattened by Weaviate)
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Test Content",
					"title":    "Test Title",
					"metadata": map[string]any{
						"author": "John Doe",
						"year":   2023,
					},
				},
			},
		},
	}, &resp, testAPIKey)

	// May succeed or fail depending on schema validation
	// This tests how MCP handles nested objects
	if err == nil {
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 1)
	}
}

func TestUpsertToolUnicodeInProperties(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Insert object with Unicode and emoji
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": "Hello 世界 🌍 Привет",
					"title":    "Ü​nicode テスト 🚀",
				},
			},
		},
	}, &resp, testAPIKey)

	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Verify Unicode was preserved
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "Hello 世界 🌍 Привет", obj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Ü​nicode テスト 🚀", obj.Properties.(map[string]interface{})["title"])
}

func TestUpsertToolVeryLongPropertyValues(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTest(t)
	defer cleanup()

	// Create very long text (10KB)
	longText := ""
	for i := 0; i < 10000; i++ {
		longText += "A"
	}

	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"contents": longText,
					"title":    "Long Text Test",
				},
			},
		},
	}, &resp, testAPIKey)

	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Verify long text was stored
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	assert.Len(t, obj.Properties.(map[string]interface{})["contents"].(string), 10000)
}

func TestUpsertToolNullPropertyValues(t *testing.T) {
	cls, ctx, cleanup := setupUpsertTestWithComplexSchema(t)
	defer cleanup()

	// Insert object with null property values
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				Properties: map[string]any{
					"textProp": "Valid Text",
					"intProp":  nil, // Null value
				},
			},
		},
	}, &resp, testAPIKey)

	// Should succeed - null values are typically allowed
	require.Nil(t, err)
	validateUpsertResults(t, resp, 1)

	// Verify object was created
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, testAPIKey)
	require.Nil(t, err)
	assert.Equal(t, "Valid Text", obj.Properties.(map[string]interface{})["textProp"])
}
