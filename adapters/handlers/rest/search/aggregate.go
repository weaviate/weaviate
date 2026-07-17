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
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// IsAggregateRoute reports whether urlPath is under the static REST
// aggregate namespace, /v1/aggregate/{collection}. Like the search routes,
// aggregations are POSTs (an HTTP write method) but semantically reads; the
// operational-mode middleware uses this for the same read classification.
//
// path.Clean matches the router's normalization, so trailing/doubled/dot
// slashes are classified the same way the router routes them.
func IsAggregateRoute(urlPath string) bool {
	parts := strings.Split(path.Clean(urlPath), "/")
	// ["", "v1", "aggregate", {collection}]
	return len(parts) == 4 && parts[0] == "" && parts[1] == "v1" &&
		parts[2] == "aggregate" && parts[3] != ""
}

// Aggregate executes a counts aggregation over collection: the number of
// objects matching the optional where filter, in total or per group of the
// groupBy property. It returns the 200 payload or an APIError carrying the
// HTTP status.
func (h *Handler) Aggregate(ctx context.Context, principal *models.Principal,
	collection string, body *models.AggregateRequest,
) (*models.AggregateResponse, *APIError) {
	before := time.Now()

	// error messages must never leak cross-namespace schema
	strip := func(apiErr *APIError) *APIError {
		return &APIError{Status: apiErr.Status, Err: namespacing.StripErrForPrincipal(principal, apiErr.Err)}
	}

	// reserved fields are rejected before any schema access, so an
	// unauthorized caller cannot probe the collection
	if apiErr := checkAggregateReservedFields(body); apiErr != nil {
		return nil, strip(apiErr)
	}

	ctx, class, resolved, getClass, apiErr := h.resolveAuthorizedClass(ctx, principal, collection, body.Tenant)
	if apiErr != nil {
		return nil, strip(apiErr)
	}

	params, groupByIsRef, apiErr := h.buildAggregateParams(class, resolved, body, getClass, principal)
	if apiErr != nil {
		return nil, strip(apiErr)
	}

	res, err := h.traverser.Aggregate(ctx, principal, params)
	if err != nil {
		return nil, strip(statusFromError(err))
	}

	reply, err := buildAggregateResponse(res, params.GroupBy != nil, groupByIsRef, principal, time.Since(before))
	if err != nil {
		return nil, strip(&APIError{Status: http.StatusInternalServerError, Err: err})
	}

	return reply, nil
}

// checkAggregateReservedFields rejects reserved (not yet supported) fields
// and unsupported returnMetrics entries with 422. over/objectLimit are the
// aggregate-over-search pair; the "property:statistic" returnMetrics
// grammar is the per-property statistics of a later phase. Keep the set in
// lock-step with AggregateRequest in openapi-specs/schema.json.
func checkAggregateReservedFields(body *models.AggregateRequest) *APIError {
	if body.Over != nil {
		return newAPIError(http.StatusUnprocessableEntity, "over is not yet supported")
	}
	if body.ObjectLimit != nil {
		return newAPIError(http.StatusUnprocessableEntity, "objectLimit is not yet supported")
	}
	for _, metric := range body.ReturnMetrics {
		if metric == "count" {
			continue
		}
		if strings.Contains(metric, ":") {
			return newAPIError(http.StatusUnprocessableEntity,
				"returnMetrics entry %q is not yet supported; only \"count\" is", metric)
		}
		return newAPIError(http.StatusUnprocessableEntity,
			"unknown returnMetrics entry %q, expected \"count\"", metric)
	}
	return nil
}

// buildAggregateParams converts the aggregate request into the
// aggregation.Params consumed by traverser.Aggregate. Behavior must stay in
// sync with the gRPC parser (adapters/handlers/grpc/v1/
// parse_aggregate_request.go). The returned groupByIsRef reports whether the
// groupBy property is a cross-reference — its group values are beacon URIs
// the reply builder must namespace-strip.
func (h *Handler) buildAggregateParams(class *models.Class, className string,
	body *models.AggregateRequest, getClass classGetterFunc, principal *models.Principal,
) (*aggregation.Params, bool, *APIError) {
	params := &aggregation.Params{
		ClassName: schema.ClassName(className),
		Tenant:    body.Tenant,
		// count is the only phase-1 metric; the ungrouped aggregators
		// populate the count only when asked (grouped ones always do)
		IncludeMetaCount: true,
	}

	groupByIsRef := false
	if body.GroupBy != "" {
		// check before GetPropertyByName, which matches the segment before a dot
		if strings.Contains(body.GroupBy, ".") {
			return nil, false, newAPIError(http.StatusUnprocessableEntity,
				"groupBy %q is not yet supported, groupBy must be a bare property name", body.GroupBy)
		}
		normalized := schema.LowercaseFirstLetter(body.GroupBy)
		prop, err := schema.GetPropertyByName(class, normalized)
		if err != nil {
			// the grouper silently returns zero groups for an unknown
			// property; reject it deterministically instead
			return nil, false, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		groupByIsRef = schema.IsRefDataType(prop.DataType)
		params.GroupBy = &filters.Path{
			Class:    schema.ClassName(className),
			Property: schema.PropertyName(normalized),
		}
	}

	if body.Limit != nil {
		if body.GroupBy == "" {
			return nil, false, newAPIError(http.StatusBadRequest,
				"limit is the maximum number of groups and requires groupBy")
		}
		if *body.Limit <= 0 {
			return nil, false, newAPIError(http.StatusBadRequest,
				"limit must be positive, got %d", *body.Limit)
		}
		limit := int(*body.Limit)
		params.Limit = &limit
	}

	filter, apiErr := parseWhere(body.Where, className, h.namespacesEnabled, principal, getClass)
	if apiErr != nil {
		return nil, false, apiErr
	}
	params.Filters = filter

	return params, groupByIsRef, nil
}

// buildAggregateResponse converts the traverser's aggregation result into
// the models.AggregateResponse: ungrouped aggregations return the flat
// {count, tookMs} form, grouped ones {groups, tookMs} with each group's
// {groupedBy, count}, ordered by descending count (engine order). A grouped
// aggregation may produce no groups (nothing matched, or no matching object
// carries the property); groups is then omitted.
func buildAggregateResponse(res any, grouped, groupByIsRef bool,
	principal *models.Principal, took time.Duration,
) (*models.AggregateResponse, error) {
	tookMs := took.Milliseconds()
	reply := &models.AggregateResponse{TookMs: &tookMs}

	result, ok := res.(*aggregation.Result)
	if !ok && res != nil {
		return nil, fmt.Errorf("unexpected aggregate result type: %T", res)
	}

	if !grouped {
		// the aggregators return the ungrouped count as a single group
		if result == nil || len(result.Groups) == 0 {
			return nil, errors.New("no groups found in aggregate result")
		}
		count := int64(result.Groups[0].Count)
		reply.Count = &count
		return reply, nil
	}

	if result == nil {
		return reply, nil
	}
	groups := make([]*models.AggregateGroup, len(result.Groups))
	for i := range result.Groups {
		groupedBy := result.Groups[i].GroupedBy
		if groupedBy == nil {
			return nil, fmt.Errorf("missing groupedBy on group %d of aggregate result", i)
		}
		count := int64(result.Groups[i].Count)
		groups[i] = &models.AggregateGroup{
			Count: &count,
			GroupedBy: &models.AggregateGroupedBy{
				Path:  groupedBy.Path,
				Value: groupValue(groupedBy.Value, groupByIsRef, principal),
			},
		}
	}
	reply.Groups = groups
	return reply, nil
}

const beaconScheme = "weaviate://"

// groupValue renders one group's value. Grouping by a ref-typed property
// yields beacon URIs: strip the caller's own "<ns>:" from the embedded class
// (foreign prefixes stay) — gRPC replier parity (prepare_aggregate_reply.go).
// The scheme guard matters: crossref.Parse ignores the scheme and
// Ref.String() rewrites to weaviate:// dropping any query/fragment, so a
// non-beacon string must pass through untouched. Every other value type is
// returned verbatim — JSON carries the property's native type.
func groupValue(value any, groupByIsRef bool, principal *models.Principal) any {
	if !groupByIsRef {
		return value
	}
	str, ok := value.(string)
	if !ok {
		// remote-shard beacons arrive as plain strings after the JSON
		// round-trip; a stray strfmt.URI from a local shard still needs
		// the strip
		uri, isURI := value.(strfmt.URI)
		if !isURI {
			return value
		}
		str = string(uri)
	}
	if !strings.HasPrefix(str, beaconScheme) {
		return str
	}
	ref, err := crossref.Parse(str)
	if err != nil {
		return str
	}
	ref.Class = namespacing.StripOwnNamespace(principal, ref.Class)
	return ref.String()
}
