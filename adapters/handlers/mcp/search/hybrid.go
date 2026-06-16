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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (s *WeaviateSearcher) Hybrid(ctx context.Context, req mcp.CallToolRequest, args QueryHybridArgs) (resp *QueryHybridResp, retErr error) {
	// Authorize the request: first check MCP-level permission, then collection-level data permission
	principal, err := s.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = namespacing.StripErrForPrincipal(principal, retErr) }()

	resolved, _, err := namespacing.Resolve(principal, s.schemaManager, s.namespacesEnabled, args.CollectionName)
	if err != nil {
		return nil, err
	}
	args.CollectionName = resolved

	log := s.logger.WithFields(logrus.Fields{
		"tool":       "weaviate-query-hybrid",
		"collection": args.CollectionName,
	})
	log.Debug("executing hybrid query")

	if err := s.AuthorizeCollectionData(ctx, principal, authorization.READ, args.CollectionName, args.TenantName); err != nil {
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

	alpha := common_filters.DefaultAlpha
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

		localFilter, err = filterext.Parse(&whereFilter, args.CollectionName, s.namespacesEnabled, principal)
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
		log.Warnf("hybrid query failed: %v", err)
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

	// Strip caller's NS from nested LocalRef.Class — mirror of the gRPC
	// extractRefPropertiesAnswer strip.
	res = stripResultsOwnNamespace(principal, res)

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

// maxStripDepth caps recursion in stripResultValue as belt-and-suspenders
// against pathological inputs (cyclic references via shared maps/slices,
// extreme nesting). Realistic schemas nest a handful of levels at most.
const maxStripDepth = 64

// stripResultsOwnNamespace deep-copies results with every nested
// search.LocalRef.Class stripped of the caller's own NS. Recurses into
// Fields so deeper cross-refs are stripped too. No-op for global /
// NS-disabled principals.
func stripResultsOwnNamespace(principal *models.Principal, results []any) []any {
	if principal == nil || principal.IsGlobalOperator || principal.Namespace == "" || len(results) == 0 {
		return results
	}
	out := make([]any, len(results))
	for i, r := range results {
		out[i] = stripResultValue(principal, r, 0)
	}
	return out
}

// stripResultValue handles LocalRef / map / slice; other types pass through.
// Beyond maxStripDepth the value is returned untouched (defensive cap).
func stripResultValue(principal *models.Principal, val any, depth int) any {
	if depth >= maxStripDepth {
		return val
	}
	switch v := val.(type) {
	case search.LocalRef:
		return search.LocalRef{
			Class:  namespacing.StripOwnNamespace(principal, v.Class),
			Fields: stripResultMap(principal, v.Fields, depth+1),
		}
	case map[string]any:
		return stripResultMap(principal, v, depth+1)
	case []any:
		out := make([]any, len(v))
		for i, vv := range v {
			out[i] = stripResultValue(principal, vv, depth+1)
		}
		return out
	default:
		return val
	}
}

func stripResultMap(principal *models.Principal, m map[string]any, depth int) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = stripResultValue(principal, v, depth)
	}
	return out
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
