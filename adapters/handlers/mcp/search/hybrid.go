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

package search

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (s *WeaviateSearcher) Hybrid(ctx context.Context, req mcp.CallToolRequest, args QueryHybridArgs) (*QueryHybridResp, error) {
	// Authorize the request
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

	// Parse filters if provided
	var localFilter *filters.LocalFilter
	if args.Filters != nil {
		// Convert map to WhereFilter
		filterJSON, err := json.Marshal(args.Filters)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal filters: %w", err)
		}

		var whereFilter models.WhereFilter
		if err := json.Unmarshal(filterJSON, &whereFilter); err != nil {
			return nil, fmt.Errorf("failed to unmarshal filters: %w", err)
		}

		// Parse to LocalFilter
		localFilter, err = filterext.Parse(&whereFilter, args.CollectionName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filters: %w", err)
		}
	}

	res, err := s.traverser.GetClass(ctx, principal, dto.GetParams{
		ClassName:            args.CollectionName,
		Tenant:               args.TenantName,
		Properties:           selectProps,
		HybridSearch:         hybridSearch,
		Pagination:           pagination,
		Filters:              localFilter,
		AdditionalProperties: additionalProps,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute hybrid search: %w", err)
	}

	// Ensure res is not nil to prevent panic downstream
	// Empty results are valid, so convert nil to empty slice
	if res == nil {
		res = []any{}
	}

	// Filter properties if specific properties were requested
	if len(selectProps) > 0 {
		res = filterResultProperties(res, selectProps)
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

// filterResultProperties filters each result to only include requested properties
// while preserving _additional metadata and id fields. This follows a similar pattern
// to the gRPC handler's extractPropertiesAnswer, but for generic JSON results.
func filterResultProperties(results []any, selectProps search.SelectProperties) []any {
	if len(results) == 0 {
		return results
	}

	// Build a set of requested property names for fast lookup
	propSet := make(map[string]bool, len(selectProps))
	for _, prop := range selectProps {
		propSet[prop.Name] = true
	}

	// Filter each result
	filtered := make([]any, len(results))
	for i, result := range results {
		resultMap, ok := result.(map[string]any)
		if !ok {
			// Not a map, keep as-is
			filtered[i] = result
			continue
		}

		// Create new map with only requested properties
		newResult := make(map[string]any)
		for key, value := range resultMap {
			// Always preserve _additional metadata and id
			if key == "_additional" || key == "id" {
				newResult[key] = value
			} else if propSet[key] {
				// Include if in requested properties
				newResult[key] = value
			}
			// Otherwise skip this property
		}
		filtered[i] = newResult
	}

	return filtered
}
