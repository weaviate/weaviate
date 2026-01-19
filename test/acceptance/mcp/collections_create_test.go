//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/test/helper"
)

// Test 1: Simple collection creation with basic text properties
func TestCreateCollection_SimpleTextProperties(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "SimpleArticle"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "A simple article collection",
		Properties: []any{
			map[string]any{
				"name":        "title",
				"dataType":    []string{"text"},
				"description": "Title of the article",
			},
			map[string]any{
				"name":        "content",
				"dataType":    []string{"text"},
				"description": "Main content of the article",
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, collectionName, result.CollectionName)

	// Verify collection was created by getting its schema
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	assert.Equal(t, collectionName, schema.Class)
	assert.Equal(t, "A simple article collection", schema.Description)
	assert.Len(t, schema.Properties, 2)
	assert.Equal(t, "title", schema.Properties[0].Name)
	assert.Equal(t, "content", schema.Properties[1].Name)
}

// Test 2: Collection with multiple data types
func TestCreateCollection_MultipleDataTypes(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "Product"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Product catalog",
		Properties: []any{
			map[string]any{
				"name":     "name",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "description",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "price",
				"dataType": []string{"number"},
			},
			map[string]any{
				"name":     "inStock",
				"dataType": []string{"boolean"},
			},
			map[string]any{
				"name":     "quantity",
				"dataType": []string{"int"},
			},
			map[string]any{
				"name":     "releaseDate",
				"dataType": []string{"date"},
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, collectionName, result.CollectionName)

	// Verify collection has all property types
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	assert.Len(t, schema.Properties, 6)

	// Check each property type
	propertyTypes := make(map[string]string)
	for _, prop := range schema.Properties {
		propertyTypes[prop.Name] = prop.DataType[0]
	}
	assert.Equal(t, "text", propertyTypes["name"])
	assert.Equal(t, "text", propertyTypes["description"])
	assert.Equal(t, "number", propertyTypes["price"])
	assert.Equal(t, "boolean", propertyTypes["inStock"])
	assert.Equal(t, "int", propertyTypes["quantity"])
	assert.Equal(t, "date", propertyTypes["releaseDate"])
}

// Test 3: Collection with custom inverted index configuration
func TestCreateCollection_WithInvertedIndexConfig(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "BlogPost"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Blog posts with custom indexing",
		Properties: []any{
			map[string]any{
				"name":     "title",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "body",
				"dataType": []string{"text"},
			},
		},
		InvertedIndexConfig: map[string]any{
			"bm25": map[string]any{
				"b":  0.75,
				"k1": 1.2,
			},
			"stopwords": map[string]any{
				"preset": "en",
			},
			"indexTimestamps":      true,
			"indexNullState":       true,
			"indexPropertyLength":  true,
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify inverted index config
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.InvertedIndexConfig)
	assert.NotNil(t, schema.InvertedIndexConfig.Bm25)
	assert.Equal(t, float32(0.75), schema.InvertedIndexConfig.Bm25.B)
	assert.Equal(t, float32(1.2), schema.InvertedIndexConfig.Bm25.K1)
	assert.True(t, schema.InvertedIndexConfig.IndexTimestamps)
	assert.True(t, schema.InvertedIndexConfig.IndexNullState)
	assert.True(t, schema.InvertedIndexConfig.IndexPropertyLength)
}

// Test 4: Collection with single vector configuration (none vectorizer)
func TestCreateCollection_WithVectorConfig(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "Document"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Documents with custom vectors",
		Properties: []any{
			map[string]any{
				"name":     "title",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "content",
				"dataType": []string{"text"},
			},
		},
		VectorConfig: map[string]any{
			"default": map[string]any{
				"vectorizer": map[string]any{
					"none": map[string]any{},
				},
				"vectorIndexType": "hnsw",
				"vectorIndexConfig": map[string]any{
					"ef":              100,
					"efConstruction":  128,
					"maxConnections":  64,
					"distance":        "cosine",
				},
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify vector config
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.VectorConfig)
	assert.Contains(t, schema.VectorConfig, "default")
}

// Test 5: Collection with text2vec-transformers vectorizer
func TestCreateCollection_WithTransformersVectorizer(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "VectorizedArticle"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Articles with transformer vectorization",
		Properties: []any{
			map[string]any{
				"name":     "title",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "content",
				"dataType": []string{"text"},
			},
		},
		VectorConfig: map[string]any{
			"default": map[string]any{
				"vectorizer": map[string]any{
					"text2vec-transformers": map[string]any{
						"poolingStrategy": "masked_mean",
						"vectorizeClassName": false,
					},
				},
				"vectorIndexType": "hnsw",
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify vectorizer is configured
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.VectorConfig)
	assert.Contains(t, schema.VectorConfig, "default")
}

// Test 6: Collection with multiple named vectors
func TestCreateCollection_WithMultipleNamedVectors(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "MultiVectorProduct"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Products with multiple vector representations",
		Properties: []any{
			map[string]any{
				"name":     "name",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":     "description",
				"dataType": []string{"text"},
			},
		},
		VectorConfig: map[string]any{
			"text_vector": map[string]any{
				"vectorizer": map[string]any{
					"text2vec-transformers": map[string]any{
						"properties": []string{"name", "description"},
					},
				},
				"vectorIndexType": "hnsw",
			},
			"image_vector": map[string]any{
				"vectorizer": map[string]any{
					"none": map[string]any{},
				},
				"vectorIndexType": "flat",
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify multiple vectors
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.VectorConfig)
	assert.Contains(t, schema.VectorConfig, "text_vector")
	assert.Contains(t, schema.VectorConfig, "image_vector")
	assert.Len(t, schema.VectorConfig, 2)
}

// Test 7: Collection with multi-tenancy enabled
func TestCreateCollection_WithMultiTenancy(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "TenantData"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Multi-tenant collection",
		Properties: []any{
			map[string]any{
				"name":     "data",
				"dataType": []string{"text"},
			},
		},
		MultiTenancyConfig: map[string]any{
			"enabled":             true,
			"autoTenantCreation":  false,
			"autoTenantActivation": false,
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify multi-tenancy config
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.MultiTenancyConfig)
	assert.True(t, schema.MultiTenancyConfig.Enabled)
	assert.False(t, schema.MultiTenancyConfig.AutoTenantCreation)
	assert.False(t, schema.MultiTenancyConfig.AutoTenantActivation)
}

// Test 8: Collection with auto-tenant features enabled
func TestCreateCollection_WithAutoTenantFeatures(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "AutoTenantData"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":     "value",
				"dataType": []string{"text"},
			},
		},
		MultiTenancyConfig: map[string]any{
			"enabled":             true,
			"autoTenantCreation":  true,
			"autoTenantActivation": true,
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify auto-tenant config
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.MultiTenancyConfig)
	assert.True(t, schema.MultiTenancyConfig.Enabled)
	assert.True(t, schema.MultiTenancyConfig.AutoTenantCreation)
	assert.True(t, schema.MultiTenancyConfig.AutoTenantActivation)
}

// Test 9: Complex collection with all features combined
func TestCreateCollection_ComplexWithAllFeatures(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "ComplexCollection"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "A complex collection with all features",
		Properties: []any{
			map[string]any{
				"name":            "title",
				"dataType":        []string{"text"},
				"description":     "Title field",
				"indexSearchable": true,
				"indexFilterable": true,
				"tokenization":    "word",
			},
			map[string]any{
				"name":            "description",
				"dataType":        []string{"text"},
				"indexSearchable": true,
			},
			map[string]any{
				"name":     "price",
				"dataType": []string{"number"},
			},
			map[string]any{
				"name":     "active",
				"dataType": []string{"boolean"},
			},
		},
		InvertedIndexConfig: map[string]any{
			"bm25": map[string]any{
				"b":  0.8,
				"k1": 1.5,
			},
			"stopwords": map[string]any{
				"preset": "en",
			},
		},
		VectorConfig: map[string]any{
			"text_embedding": map[string]any{
				"vectorizer": map[string]any{
					"text2vec-transformers": map[string]any{
						"poolingStrategy": "masked_mean",
					},
				},
				"vectorIndexType": "hnsw",
			},
			"custom_embedding": map[string]any{
				"vectorizer": map[string]any{
					"none": map[string]any{},
				},
				"vectorIndexType": "flat",
			},
		},
		MultiTenancyConfig: map[string]any{
			"enabled":             true,
			"autoTenantCreation":  true,
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, collectionName, result.CollectionName)

	// Verify all features are configured
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	assert.Equal(t, "A complex collection with all features", schema.Description)
	assert.Len(t, schema.Properties, 4)
	assert.NotNil(t, schema.InvertedIndexConfig)
	assert.NotNil(t, schema.VectorConfig)
	assert.Len(t, schema.VectorConfig, 2)
	assert.NotNil(t, schema.MultiTenancyConfig)
	assert.True(t, schema.MultiTenancyConfig.Enabled)
}

// Test 10: Collection with property indexing configuration
func TestCreateCollection_WithPropertyIndexConfig(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "IndexedProperties"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":            "searchableField",
				"dataType":        []string{"text"},
				"indexSearchable": true,
				"indexFilterable": false,
				"tokenization":    "word",
			},
			map[string]any{
				"name":            "filterableField",
				"dataType":        []string{"text"},
				"indexSearchable": false,
				"indexFilterable": true,
				"tokenization":    "field",
			},
			map[string]any{
				"name":            "bothField",
				"dataType":        []string{"text"},
				"indexSearchable": true,
				"indexFilterable": true,
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify property indexing config
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	assert.Len(t, schema.Properties, 3)

	// Check each property's index config
	for _, prop := range schema.Properties {
		switch prop.Name {
		case "searchableField":
			require.NotNil(t, prop.IndexSearchable)
			assert.True(t, *prop.IndexSearchable)
			require.NotNil(t, prop.IndexFilterable)
			assert.False(t, *prop.IndexFilterable)
		case "filterableField":
			require.NotNil(t, prop.IndexSearchable)
			assert.False(t, *prop.IndexSearchable)
			require.NotNil(t, prop.IndexFilterable)
			assert.True(t, *prop.IndexFilterable)
		case "bothField":
			require.NotNil(t, prop.IndexSearchable)
			assert.True(t, *prop.IndexSearchable)
			require.NotNil(t, prop.IndexFilterable)
			assert.True(t, *prop.IndexFilterable)
		}
	}
}

// Test 11: Failure - Missing required collection_name
func TestCreateCollection_Fail_MissingCollectionName(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		Description: "Collection without name",
		Properties: []any{
			map[string]any{
				"name":     "field",
				"dataType": []string{"text"},
			},
		},
	}, &result)

	// Should fail due to missing collection_name
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "collection")
}

// Test 12: Failure - Duplicate collection name
func TestCreateCollection_Fail_DuplicateName(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collectionName := "DuplicateCollection"

	// Create first collection
	var result1 *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":     "field",
				"dataType": []string{"text"},
			},
		},
	}, &result1)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)

	// Try to create duplicate
	var result2 *create.CreateCollectionResp
	err = helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":     "field2",
				"dataType": []string{"text"},
			},
		},
	}, &result2)

	// Should fail due to duplicate name
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

// Test 13: Failure - Invalid vectorizer module
func TestCreateCollection_Fail_InvalidVectorizer(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "InvalidVectorizerCollection"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":     "field",
				"dataType": []string{"text"},
			},
		},
		VectorConfig: map[string]any{
			"default": map[string]any{
				"vectorizer": map[string]any{
					"text2vec-nonexistent": map[string]any{},
				},
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	// Should fail due to unavailable vectorizer
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "vectorizer")
}

// Test 14: Failure - Invalid property data type
func TestCreateCollection_Fail_InvalidDataType(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "InvalidDataTypeCollection"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Properties: []any{
			map[string]any{
				"name":     "field",
				"dataType": []string{"invalidType"},
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	// Should fail due to invalid data type
	assert.NotNil(t, err)
}

// Test 15: Collection with cross-reference property
func TestCreateCollection_WithCrossReference(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create first collection to reference
	authorCollection := "Author"
	var authorResult *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: authorCollection,
		Properties: []any{
			map[string]any{
				"name":     "name",
				"dataType": []string{"text"},
			},
		},
	}, &authorResult)
	defer helper.DeleteClassAuth(t, authorCollection, apiKey)
	require.Nil(t, err)

	// Create collection with cross-reference
	bookCollection := "Book"
	var bookResult *create.CreateCollectionResp
	err = helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: bookCollection,
		Properties: []any{
			map[string]any{
				"name":     "title",
				"dataType": []string{"text"},
			},
			map[string]any{
				"name":        "writtenBy",
				"dataType":    []string{"Author"},
				"description": "Reference to author",
			},
		},
	}, &bookResult)
	defer helper.DeleteClassAuth(t, bookCollection, apiKey)

	require.Nil(t, err)
	require.NotNil(t, bookResult)

	// Verify cross-reference property
	schema := helper.GetClassAuth(t, bookCollection, apiKey)
	require.NotNil(t, schema)
	assert.Len(t, schema.Properties, 2)

	// Find the cross-reference property
	var hasRefProp bool
	for _, prop := range schema.Properties {
		if prop.Name == "writtenBy" {
			hasRefProp = true
			assert.Equal(t, []string{"Author"}, prop.DataType)
			break
		}
	}
	assert.True(t, hasRefProp, "should have writtenBy cross-reference property")
}

// Test 16: Minimal collection (only required field)
func TestCreateCollection_MinimalConfiguration(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "MinimalCollection"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, collectionName, result.CollectionName)

	// Verify collection exists with defaults
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	assert.Equal(t, collectionName, schema.Class)
}

// Test 17: Collection with CLIP multi2vec vectorizer
func TestCreateCollection_WithCLIPVectorizer(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := "CLIPCollection"
	var result *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, "weaviate-collections-create", &create.CreateCollectionArgs{
		CollectionName: collectionName,
		Description:    "Collection with CLIP vectorizer",
		Properties: []any{
			map[string]any{
				"name":     "text",
				"dataType": []string{"text"},
			},
		},
		VectorConfig: map[string]any{
			"default": map[string]any{
				"vectorizer": map[string]any{
					"multi2vec-clip": map[string]any{
						"textFields": []string{"text"},
					},
				},
				"vectorIndexType": "hnsw",
			},
		},
	}, &result)
	defer helper.DeleteClassAuth(t, collectionName, apiKey)

	require.Nil(t, err)
	require.NotNil(t, result)

	// Verify CLIP vectorizer is configured
	schema := helper.GetClassAuth(t, collectionName, apiKey)
	require.NotNil(t, schema)
	require.NotNil(t, schema.VectorConfig)
	assert.Contains(t, schema.VectorConfig, "default")
}
