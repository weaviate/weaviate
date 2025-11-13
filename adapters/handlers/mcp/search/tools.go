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
