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
	testServerAddr = "localhost:8080"
	testAPIKey     = "admin-key"
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
