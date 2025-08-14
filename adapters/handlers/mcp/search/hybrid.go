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

package search

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func (s *WeaviateSearcher) Hybrid(ctx context.Context, req mcp.CallToolRequest, args SearchWithHybridArgs) (any, error) {
	principal, err := s.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get principal: %w", err)
	}

	selectProps := make(search.SelectProperties, len(args.TargetProperties))
	for i, prop := range args.TargetProperties {
		selectProps[i] = search.SelectProperty{
			Name:        prop,
			IsPrimitive: true,
		}
	}
	res, err := s.traverser.GetClass(ctx, principal, dto.GetParams{
		ClassName:  args.Collection,
		Properties: selectProps,
		HybridSearch: &searchparams.HybridSearch{
			Query: args.Query,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get class: %w", err)
	}
	return res, nil
}
