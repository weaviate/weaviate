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

// getDescription returns custom description if available, otherwise returns default
func getDescription(descriptions map[string]string, toolName, defaultDesc string) string {
	if descriptions != nil {
		if customDesc, ok := descriptions[toolName]; ok {
			return customDesc
		}
	}
	return defaultDesc
}

func Tools(searcher *WeaviateSearcher, descriptions map[string]string) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"weaviate-query-hybrid",
				mcp.WithDescription(getDescription(descriptions, "weaviate-query-hybrid",
					"Performs hybrid search (vector + keyword) for data in a collection.")),
				mcp.WithInputSchema[QueryHybridArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(searcher.Hybrid),
		},
	}
}
