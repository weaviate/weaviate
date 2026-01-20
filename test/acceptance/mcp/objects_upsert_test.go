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

const testServerAddr = "localhost:8080"
const testAPIKey = "admin-key"
const toolNameUpsert = "weaviate-objects-upsert"

func TestUpsertToolInsertOneObject(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Error, "should not have error")
	require.NotEmpty(t, resp.Results[0].ID, "should have UUID")

	// Verify the object was created
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, apiKey)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Test Article Content", obj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Test Article", obj.Properties.(map[string]interface{})["title"])
}

func TestUpsertToolInsertMultipleObjects(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 3)

	// Verify all objects were created successfully
	expectedContents := []string{"First Article Content", "Second Article Content", "Third Article Content"}
	for i, result := range resp.Results {
		require.Empty(t, result.Error, "object %d should not have error", i)
		require.NotEmpty(t, result.ID, "object %d should have UUID", i)

		// Verify each object exists and has correct content
		uuid := strfmt.UUID(result.ID)
		obj, err := helper.GetObjectAuth(t, cls.Class, uuid, apiKey)
		require.Nil(t, err)
		require.NotNil(t, obj)
		assert.Equal(t, expectedContents[i], obj.Properties.(map[string]interface{})["contents"])
	}
}

func TestUpsertToolUpdateOneObject(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First, create an object to update
	initialObj := &models.Object{
		Class: cls.Class,
		Properties: map[string]interface{}{
			"contents": "Original Content",
			"title":    "Original Title",
		},
	}
	createdObj, err := helper.CreateObjectWithResponseAuth(t, initialObj, apiKey)
	require.Nil(t, err)
	require.NotNil(t, createdObj)

	existingUUID := createdObj.ID.String()

	// Now update the object using upsert with the same UUID
	var resp *create.UpsertObjectResp
	err = helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
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
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Error, "should not have error")
	require.Equal(t, existingUUID, resp.Results[0].ID, "should return same UUID")

	// Verify the object was updated
	uuid := strfmt.UUID(existingUUID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, apiKey)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Updated Content", obj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Updated Title", obj.Properties.(map[string]interface{})["title"])
}

func TestUpsertToolUpdateMultipleObjects(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First, create multiple objects to update
	obj1 := &models.Object{
		Class: cls.Class,
		Properties: map[string]interface{}{
			"contents": "Original Content 1",
			"title":    "Original Title 1",
		},
	}
	created1, err := helper.CreateObjectWithResponseAuth(t, obj1, apiKey)
	require.Nil(t, err)

	obj2 := &models.Object{
		Class: cls.Class,
		Properties: map[string]interface{}{
			"contents": "Original Content 2",
			"title":    "Original Title 2",
		},
	}
	created2, err := helper.CreateObjectWithResponseAuth(t, obj2, apiKey)
	require.Nil(t, err)

	obj3 := &models.Object{
		Class: cls.Class,
		Properties: map[string]interface{}{
			"contents": "Original Content 3",
			"title":    "Original Title 3",
		},
	}
	created3, err := helper.CreateObjectWithResponseAuth(t, obj3, apiKey)
	require.Nil(t, err)

	uuid1 := created1.ID.String()
	uuid2 := created2.ID.String()
	uuid3 := created3.ID.String()

	// Now update all three objects in a batch
	var resp *create.UpsertObjectResp
	err = helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects: []create.ObjectToUpsert{
			{
				UUID: uuid1,
				Properties: map[string]any{
					"contents": "Updated Content 1",
					"title":    "Updated Title 1",
				},
			},
			{
				UUID: uuid2,
				Properties: map[string]any{
					"contents": "Updated Content 2",
					"title":    "Updated Title 2",
				},
			},
			{
				UUID: uuid3,
				Properties: map[string]any{
					"contents": "Updated Content 3",
					"title":    "Updated Title 3",
				},
			},
		},
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 3)

	// Verify all objects were updated successfully
	expectedUUIDs := []string{uuid1, uuid2, uuid3}
	createdIDs := []strfmt.UUID{created1.ID, created2.ID, created3.ID}
	for i, result := range resp.Results {
		require.Empty(t, result.Error, "object %d should not have error", i)
		require.NotEmpty(t, result.ID, "object %d should have UUID", i)

		// Verify UUID matches
		assert.Equal(t, expectedUUIDs[i], result.ID)

		// Verify object was updated
		updatedObj, err := helper.GetObjectAuth(t, cls.Class, createdIDs[i], apiKey)
		require.Nil(t, err)
		assert.Equal(t, "Updated Content "+string(rune('1'+i)), updatedObj.Properties.(map[string]interface{})["contents"])
		assert.Equal(t, "Updated Title "+string(rune('1'+i)), updatedObj.Properties.(map[string]interface{})["title"])
	}
}

func TestUpsertToolMixedInsertAndUpdate(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create one object to be updated later
	existingObj := &models.Object{
		Class: cls.Class,
		Properties: map[string]interface{}{
			"contents": "Existing Content",
			"title":    "Existing Title",
		},
	}
	createdObj, err := helper.CreateObjectWithResponseAuth(t, existingObj, apiKey)
	require.Nil(t, err)
	existingUUID := createdObj.ID.String()

	// Perform a mixed batch: update existing object and insert new ones
	var resp *create.UpsertObjectResp
	err = helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
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
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 3)

	// Verify all operations succeeded
	for i, result := range resp.Results {
		require.Empty(t, result.Error, "object %d should not have error", i)
		require.NotEmpty(t, result.ID, "object %d should have UUID", i)
	}

	// Verify first object was updated (should have same UUID)
	assert.Equal(t, existingUUID, resp.Results[0].ID)
	updatedObj, err := helper.GetObjectAuth(t, cls.Class, createdObj.ID, apiKey)
	require.Nil(t, err)
	assert.Equal(t, "Updated Existing Content", updatedObj.Properties.(map[string]interface{})["contents"])
	assert.Equal(t, "Updated Existing Title", updatedObj.Properties.(map[string]interface{})["title"])

	// Verify second object was inserted (new UUID)
	assert.NotEqual(t, existingUUID, resp.Results[1].ID)
	newObj1, err := helper.GetObjectAuth(t, cls.Class, strfmt.UUID(resp.Results[1].ID), apiKey)
	require.Nil(t, err)
	assert.Equal(t, "New Content 1", newObj1.Properties.(map[string]interface{})["contents"])

	// Verify third object was inserted (new UUID)
	assert.NotEqual(t, existingUUID, resp.Results[2].ID)
	newObj2, err := helper.GetObjectAuth(t, cls.Class, strfmt.UUID(resp.Results[2].ID), apiKey)
	require.Nil(t, err)
	assert.Equal(t, "New Content 2", newObj2.Properties.(map[string]interface{})["contents"])
}

func TestUpsertToolWithVectors(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}, &resp, apiKey)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Error)
	require.NotEmpty(t, resp.Results[0].ID)

	// Verify the object was created with the custom vector
	uuid := strfmt.UUID(resp.Results[0].ID)
	obj, err := helper.GetObjectAuth(t, cls.Class, uuid, apiKey, "vector")
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "Article with custom vector", obj.Properties.(map[string]interface{})["contents"])
	// Verify the vector was set (if returned)
	if obj.Vector != nil {
		assert.Len(t, obj.Vector, len(customVector))
	}
}

func TestUpsertToolEmptyBatch(t *testing.T) {
	helper.SetupClient(testServerAddr)
	apiKey := testAPIKey

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to upsert with empty objects array
	var resp *create.UpsertObjectResp
	err := helper.CallToolOnce(ctx, t, toolNameUpsert, &create.UpsertObjectArgs{
		CollectionName: cls.Class,
		Objects:        []create.ObjectToUpsert{},
	}, &resp, apiKey)

	// Should return an error for empty batch
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "at least one object is required")
}
