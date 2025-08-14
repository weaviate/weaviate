package read

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func Tools(reader *WeaviateReader) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"get-schema",
				mcp.WithDescription("Retrieves the schema of the database."),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetSchema),
		},
		{
			Tool: mcp.NewTool(
				"get-tenants",
				mcp.WithDescription("Retrieves the tenants of a collection in the database."),
			),
			Handler: mcp.NewStructuredToolHandler(reader.GetTenants),
		},
	}
}
