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

package create

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

func Tools(creator *WeaviateCreator, descriptions map[string]string) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"weaviate-objects-upsert",
				mcp.WithDescription(getDescription(descriptions, "weaviate-objects-upsert",
					"Upserts (inserts or updates) a single object into a collection in the database.")),
				mcp.WithInputSchema[UpsertObjectArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(creator.UpsertObject),
		},
	}
}
