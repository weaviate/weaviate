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
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
)

func Tools(reader *WeaviateReader, descriptions map[string]string) []server.ServerTool {
	tools := []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"weaviate-collections-get-config",
				mcp.WithDescription(internal.GetDescription(descriptions, "weaviate-collections-get-config",
					"Retrieves collection configuration(s). If collection_name is provided, returns only that collection's config. Otherwise returns all collections.")),
				mcp.WithInputSchema[GetCollectionConfigArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetCollectionConfig),
		},
		{
			Tool: mcp.NewTool(
				"weaviate-tenants-list",
				mcp.WithDescription(internal.GetDescription(descriptions, "weaviate-tenants-list",
					"Lists the tenants of a collection in the database.")),
				mcp.WithInputSchema[GetTenantsArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetTenants),
		},
		{
			Tool: mcp.NewTool(
				"weaviate-objects-get",
				mcp.WithDescription(internal.GetDescription(descriptions, "weaviate-objects-get",
					"Retrieves one or more objects from a collection. Can fetch specific objects by UUID or retrieve a paginated list of objects.")),
				mcp.WithInputSchema[GetObjectsArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetObjects),
		},
	}

	// Conditionally add logs tool if enabled
	logsEnabled := os.Getenv("MCP_SERVER_READ_LOGS_ENABLED")
	if strings.ToLower(logsEnabled) == "true" {
		tools = append(tools, server.ServerTool{
			Tool: mcp.NewTool(
				"weaviate-logs-fetch",
				mcp.WithDescription(internal.GetDescription(descriptions, "weaviate-logs-fetch",
					"Fetches Weaviate server logs from the in-memory buffer with pagination support. Supports optional limit (default 2000, max 50000) and offset (default 0) parameters.")),
				mcp.WithInputSchema[FetchLogsArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(reader.FetchLogs),
		})
	}

	return tools
}
