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

package create

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
	mcpmetrics "github.com/weaviate/weaviate/adapters/handlers/mcp/metrics"
	"github.com/weaviate/weaviate/entities/models"
)

// Request types

type ObjectToUpsert struct {
	UUID       string               `json:"uuid,omitempty" jsonschema_description:"UUID of the object. If not provided, a new UUID will be generated"`
	Properties map[string]any       `json:"properties" jsonschema:"required" jsonschema_description:"Properties of the object. For date properties, use RFC3339 format with time and timezone (e.g., '2020-01-01T00:00:00Z'), not just the date."`
	Vectors    map[string][]float32 `json:"vectors,omitempty" jsonschema_description:"Named vectors for the object (e.g., {'default': [0.1, 0.2, ...], 'image': [0.3, 0.4, ...]})"`
}

type UpsertObjectArgs struct {
	CollectionName string           `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to upsert objects into"`
	TenantName     string           `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant the objects belong to (for multi-tenant collections)"`
	Objects        []ObjectToUpsert `json:"objects" jsonschema:"required" jsonschema_description:"Array of objects to upsert. Each object should have 'properties' (required) and optionally 'uuid' and 'vectors'. Minimum 1 object required."`
}

type CreateCollectionArgs struct {
	CollectionName      string           `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of the collection to create. Letters, digits and underscores only; the first letter is stored capitalized (e.g., 'articles' becomes 'Articles')."`
	Description         string           `json:"description,omitempty" jsonschema_description:"Description of the collection"`
	Properties          []map[string]any `json:"properties,omitempty" jsonschema_description:"Properties in the REST property shape (e.g., [{'name': 'title', 'dataType': ['text']}]). dataType is one of text, int, number, boolean, date, uuid (each also as a [] array), geoCoordinates, phoneNumber, blob, object or object[]; 'object' takes nestedProperties; a collection name, written as stored with its first letter capitalized, makes a cross-reference. Property names are stored with a lowercase first letter."`
	VectorConfig        map[string]any   `json:"vector_config,omitempty" jsonschema_description:"Named vectors in the REST vectorConfig shape, keyed by vector name; these names are the keys of 'vectors' in weaviate-objects-upsert. The vectorizer key is a module enabled on this server (e.g., {'default': {'vectorizer': {'text2vec-openai': {'properties': ['title']}}}}), or {'default': {'vectorizer': {'none': {}}}} to supply vectors yourself. Omit for a collection without vectors."`
	ModuleConfig        map[string]any   `json:"module_config,omitempty" jsonschema_description:"REST moduleConfig shape for class-level module settings such as generative or reranker modules, keyed by module name (e.g., {'generative-openai': {'model': 'gpt-4o-mini'}}). Vectorizer settings belong in vector_config."`
	MultiTenancyConfig  map[string]any   `json:"multi_tenancy_config,omitempty" jsonschema_description:"REST multiTenancyConfig shape (e.g., {'enabled': true, 'autoTenantCreation': true})"`
	InvertedIndexConfig map[string]any   `json:"inverted_index_config,omitempty" jsonschema_description:"REST invertedIndexConfig shape (e.g., {'indexTimestamps': true})"`
	ReplicationConfig   map[string]any   `json:"replication_config,omitempty" jsonschema_description:"REST replicationConfig shape (e.g., {'factor': 3})"`
	ObjectTTLConfig     map[string]any   `json:"object_ttl_config,omitempty" jsonschema_description:"REST objectTtlConfig shape. defaultTtl is seconds added to deleteOn, which is '_creationTimeUnix', '_lastUpdateTimeUnix' or a date property (e.g., {'enabled': true, 'deleteOn': '_creationTimeUnix', 'defaultTtl': 3600})."`
}

// Response types

type UpsertObjectResult struct {
	ID    string `json:"id,omitempty" jsonschema_description:"UUID of the upserted object (only present if successful)"`
	Error string `json:"error,omitempty" jsonschema_description:"Error message if the upsert failed for this object"`
}

type UpsertObjectResp struct {
	Results []UpsertObjectResult `json:"results" jsonschema_description:"Results for each object in the batch, in the same order as the input"`
}

type CreateCollectionResp struct {
	Collection *models.Class `json:"collection" jsonschema_description:"The stored collection configuration"`
}

// Tool registration

func Tools(creator *WeaviateCreator, configs map[string]internal.ToolConfig, m *mcpmetrics.MCPMetrics) []server.ServerTool {
	toolName := "weaviate-objects-upsert"
	tool := mcp.NewTool(
		toolName,
		mcp.WithDescription(internal.GetDescription(configs, toolName,
			"Upserts (inserts or updates) one or more objects into a collection in batch. Supports batch operations for efficient bulk inserts and updates.")),
		mcp.WithInputSchema[UpsertObjectArgs](),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
	)
	internal.ApplySchemaDescriptions(&tool, toolName, configs)
	internal.AllowNullForOptionalArguments(&tool)

	createName := "weaviate-collections-create"
	createTool := mcp.NewTool(
		createName,
		mcp.WithDescription(internal.GetDescription(configs, createName,
			"Creates a collection. Nested arguments use the REST schema shapes that weaviate-collections-get-config returns. Fails if a collection with that name already exists.")),
		mcp.WithInputSchema[CreateCollectionArgs](),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(false),
	)
	internal.ApplySchemaDescriptions(&createTool, createName, configs)
	internal.AllowNullForOptionalArguments(&createTool)

	return []server.ServerTool{
		{Tool: tool, Handler: mcp.NewStructuredToolHandler(mcpmetrics.Instrument(m, toolName, creator.UpsertObject))},
		{Tool: createTool, Handler: mcp.NewStructuredToolHandler(mcpmetrics.Instrument(m, createName, creator.CreateCollection))},
	}
}
