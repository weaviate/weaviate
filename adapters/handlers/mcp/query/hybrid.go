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

package query

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func (q *WeaviateQuerier) Hybrid(ctx context.Context, principal *models.Principal, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	targetCol := q.parseTargetCollection(req)
	query := req.Params.Arguments.(map[string]any)["query"].(string)
	// TODO: how to enforce `Required` within the sdk so we don't have to validate here
	props := req.Params.Arguments.(map[string]any)["targetProperties"].([]string)
	selectProps := make(search.SelectProperties, len(props))
	for i, prop := range props {
		selectProps[i] = search.SelectProperty{
			Name:        prop,
			IsPrimitive: true,
		}
	}
	res, err := q.traverser.GetClass(ctx, principal, dto.GetParams{
		ClassName:  targetCol,
		Properties: selectProps,
		HybridSearch: &searchparams.HybridSearch{
			Query: query,
		},
	})
	if err != nil {
		return mcp.NewToolResultErrorFromErr("failed to query class", err), nil
	}
	jsonData, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return mcp.NewToolResultErrorFromErr("failed to encode JSON", err), nil
	}
	return mcp.NewToolResultText(string(jsonData)), nil
}

func (q *WeaviateQuerier) parseTargetCollection(req mcp.CallToolRequest) string {
	targetCol := q.defaultCollection
	col, ok := req.Params.Arguments.(map[string]any)["collection"].(string)
	if ok {
		targetCol = col
	}
	return targetCol
}
