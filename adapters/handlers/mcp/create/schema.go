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
	Objects        []ObjectToUpsert `json:"objects" jsonschema:"required,minItems=1" jsonschema_description:"Array of objects to upsert. Each object should have 'properties' (required) and optionally 'uuid' and 'vectors'. Minimum 1 object required."`
}

// Response types

type UpsertObjectResult struct {
	ID    string `json:"id,omitempty" jsonschema_description:"UUID of the upserted object (only present if successful)"`
	Error string `json:"error,omitempty" jsonschema_description:"Error message if the upsert failed for this object"`
}

type UpsertObjectResp struct {
	Results []UpsertObjectResult `json:"results" jsonschema_description:"Results for each object in the batch, in the same order as the input"`
}

// Tool registration

func Tools(creator *WeaviateCreator, configs map[string]internal.ToolConfig) []server.ServerTool {
	toolName := "weaviate-objects-upsert"
	tool := mcp.NewTool(
		toolName,
		mcp.WithDescription(internal.GetDescription(configs, toolName,
			"Upserts (inserts or updates) one or more objects into a collection in batch. Supports batch operations for efficient bulk inserts and updates.")),
		mcp.WithInputSchema[UpsertObjectArgs](),
	)
	internal.ApplySchemaDescriptions(&tool, toolName, configs)
	return []server.ServerTool{
		{Tool: tool, Handler: mcp.NewStructuredToolHandler(creator.UpsertObject)},
	}
}
