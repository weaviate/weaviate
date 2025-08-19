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
