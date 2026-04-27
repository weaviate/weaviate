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

package read

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
	"github.com/weaviate/weaviate/entities/models"
)

// Request types

type GetCollectionConfigArgs struct {
	CollectionName string `json:"collection_name,omitempty" jsonschema_description:"Name of specific collection to get config for. If not provided, returns all collections"`
}

type GetTenantsArgs struct {
	CollectionName string `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to get tenants from"`
}

// Response types

type GetCollectionConfigResp struct {
	Collections []*models.Class `json:"collections" jsonschema_description:"The returned collection configurations"`
}

type GetTenantsResp struct {
	Tenants []*models.Tenant `json:"tenants" jsonschema_description:"The returned tenants"`
}

// Tool registration

func Tools(reader *WeaviateReader, configs map[string]internal.ToolConfig) []server.ServerTool {
	getConfigName := "weaviate-collections-get-config"
	getConfigTool := mcp.NewTool(
		getConfigName,
		mcp.WithDescription(internal.GetDescription(configs, getConfigName,
			"Retrieves collection configuration(s). If collection_name is provided, returns only that collection's config. Otherwise returns all collections.")),
		mcp.WithInputSchema[GetCollectionConfigArgs](),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
	internal.ApplySchemaDescriptions(&getConfigTool, getConfigName, configs)

	tenantsName := "weaviate-tenants-list"
	tenantsTool := mcp.NewTool(
		tenantsName,
		mcp.WithDescription(internal.GetDescription(configs, tenantsName,
			"Lists the tenants of a collection in the database.")),
		mcp.WithInputSchema[GetTenantsArgs](),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
	internal.ApplySchemaDescriptions(&tenantsTool, tenantsName, configs)

	return []server.ServerTool{
		{Tool: getConfigTool, Handler: mcp.NewStructuredToolHandler(reader.GetCollectionConfig)},
		{Tool: tenantsTool, Handler: mcp.NewStructuredToolHandler(reader.GetTenants)},
	}
}
