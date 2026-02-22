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

func Tools(creator *WeaviateCreator, descriptions map[string]string) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool(
				"weaviate-objects-upsert",
				mcp.WithDescription(internal.GetDescription(descriptions, "weaviate-objects-upsert",
					"Upserts (inserts or updates) one or more objects into a collection in batch. Supports batch operations for efficient bulk inserts and updates.")),
				mcp.WithInputSchema[UpsertObjectArgs](),
			),
			Handler: mcp.NewStructuredToolHandler(creator.UpsertObject),
		},
	}
}
