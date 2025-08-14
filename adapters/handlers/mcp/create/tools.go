package create

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func Tools(creator *WeaviateCreator) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"insert-one",
				mcp.WithDescription("Insert a single object into a collection in the database."),
			),
			Handler: mcp.NewStructuredToolHandler(creator.InsertOne),
		},
	}
}
