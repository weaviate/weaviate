package search

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func Tools(searcher *WeaviateSearcher) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"search-with-hybrid",
				mcp.WithDescription("Search for data from a collection in the database."),
			),
			Handler: mcp.NewStructuredToolHandler(searcher.Hybrid),
		},
	}
}
