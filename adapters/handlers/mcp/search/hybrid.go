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
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (s *WeaviateSearcher) Hybrid(ctx context.Context, req mcp.CallToolRequest, args QueryHybridArgs) (*QueryHybridResp, error) {
	principal, err := s.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}

	// Build select properties from return_properties
	var selectProps search.SelectProperties
	if len(args.ReturnProperties) > 0 {
		selectProps = make(search.SelectProperties, len(args.ReturnProperties))
		for i, prop := range args.ReturnProperties {
			selectProps[i] = search.SelectProperty{
				Name:        prop,
				IsPrimitive: true,
			}
		}
	}

	// Build additional properties from return_metadata
	additionalProps := buildAdditionalProperties(args.ReturnMetadata)

	// Set alpha with default of 0.5
	alpha := 0.5
	if args.Alpha != nil {
		alpha = *args.Alpha
	}

	// Build hybrid search params
	hybridSearch := &searchparams.HybridSearch{
		Query:         args.Query,
		Alpha:         alpha,
		TargetVectors: args.TargetVectors,
		Properties:    args.TargetProperties,
	}

	// Build pagination
	var pagination *filters.Pagination
	if args.Limit != nil {
		pagination = &filters.Pagination{
			Limit: *args.Limit,
		}
	}

	res, err := s.traverser.GetClass(ctx, principal, dto.GetParams{
		ClassName:            args.CollectionName,
		Tenant:               args.TenantName,
		Properties:           selectProps,
		HybridSearch:         hybridSearch,
		Pagination:           pagination,
		AdditionalProperties: additionalProps,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute hybrid search: %w", err)
	}
	return &QueryHybridResp{Results: res}, nil
}

func buildAdditionalProperties(metadata []string) additional.Properties {
	props := additional.Properties{}

	for _, meta := range metadata {
		switch strings.ToLower(meta) {
		case "id":
			props.ID = true
		case "vector":
			props.Vector = true
		case "distance":
			props.Distance = true
		case "score":
			props.Score = true
		case "explainscore":
			props.ExplainScore = true
		case "creationtimeunix":
			props.CreationTimeUnix = true
		case "lastupdatetimeunix":
			props.LastUpdateTimeUnix = true
		case "certainty":
			props.Certainty = true
		}
	}

	return props
}
