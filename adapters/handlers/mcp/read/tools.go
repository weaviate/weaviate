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

package read

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func Tools(reader *WeaviateReader) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"weaviate-collections-get-config",
				mcp.WithDescription("Retrieves collection configuration(s). If collection_name is provided, returns only that collection's config. Otherwise returns all collections."),
				mcp.WithInputSchema[GetCollectionConfigArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetCollectionConfig),
		},
		{
			Tool: mcp.NewTool(
				"weaviate-tenants-list",
				mcp.WithDescription("Lists the tenants of a collection in the database."),
				mcp.WithInputSchema[GetTenantsArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetTenants),
		},
	}
}
