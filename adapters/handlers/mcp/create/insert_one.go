//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package create

import (
	"context"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
)

func (c *WeaviateCreator) InsertOne(ctx context.Context, principal *models.Principal, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	targetCol := c.parseTargetCollection(req)
	props := req.Params.Arguments.(map[string]any)["properties"]

	obj := models.Object{
		Class:      targetCol,
		Properties: props,
	}
	res, err := c.objectsManager.AddObject(ctx, principal, &obj, nil)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("failed to insert object", err), nil
	}
	return mcp.NewToolResultText(res.ID.String()), nil
}

func (c *WeaviateCreator) parseTargetCollection(req mcp.CallToolRequest) string {
	targetCol := c.defaultCollection
	col, ok := req.Params.Arguments.(map[string]any)["collection"].(string)
	if ok {
		targetCol = col
	}
	return targetCol
}
