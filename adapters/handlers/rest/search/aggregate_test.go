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
	"net/http"
	"testing"
	"time"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
)

// mustAggregateModel is mustModel for the aggregate request model.
func mustAggregateModel(t *testing.T, body string) *models.AggregateRequest {
	t.Helper()
	var req models.AggregateRequest
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

// doAggregate runs the aggregate handler the way the generated operation
// wiring does, with the typed, already-decoded request model.
func doAggregate(t *testing.T, deps *testDeps, principal *models.Principal,
	collection, body string,
) (*models.AggregateResponse, *APIError) {
	t.Helper()
	return deps.handler.Aggregate(context.Background(), principal, collection, mustAggregateModel(t, body))
}

// ungroupedCount is the engine's shape for an ungrouped aggregation: a
// single group carrying the total.
func ungroupedCount(count int) *aggregation.Result {
	return &aggregation.Result{Groups: []aggregation.Group{{Count: count}}}
}

func TestIsAggregateRoute(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/v1/aggregate/Movie", true},
		{"/v1/aggregate/movie", true},
		// the collection segment can be any name, including a reserved root
		// spelling or the literal "aggregate"
		{"/v1/aggregate/objects", true},
		{"/v1/aggregate/aggregate", true},
		// non-canonical spellings the router still routes must classify as
		// aggregate
		{"/v1/aggregate/Movie/", true},
		{"/v1//aggregate/Movie", true},
		{"/v1/aggregate/sub/../Movie", true},
		{"/v1/aggregate", false},
		{"/v1/aggregate/", false},
		{"/v1/aggregate/Movie/extra", false},
		{"/v2/aggregate/Movie", false},
		{"/v1/Aggregate/Movie", false},
		// a collection-first path is not under the aggregate namespace
		{"/v1/Movie/aggregate", false},
		// the sibling search namespace is not an aggregate route
		{"/v1/search/Movie/near-text", false},
		{"/v1", false},
		{"/", false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, IsAggregateRoute(tt.path))
		})
	}
}

// TestAggregateHandlerHappyPath: an empty body is a valid request and returns
// the collection's total count in the flat form. The built params must be the
// engine's count-star shape so the fast path stays reachable.
func TestAggregateHandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.aggregateRes = ungroupedCount(42)

	payload, apiErr := doAggregate(t, deps, nil, "Movie", `{}`)
	require.Nil(t, apiErr)

	require.NotNil(t, payload.Count)
	assert.Equal(t, int64(42), *payload.Count)
	assert.Nil(t, payload.Groups)
	require.NotNil(t, payload.TookMs)

	params := deps.searcher.lastAggregateParams
	require.NotNil(t, params)
	assert.Equal(t, "Movie", params.ClassName.String())
	assert.True(t, params.IncludeMetaCount)
	assert.True(t, aggregation.IsCountStar(params),
		"an empty body must build the engine's count-star params")
}

// TestAggregateHandlerReturnMetricsCount: "count", the empty array and an
// omitted return_metrics are equivalent (count is the only phase-1 metric).
func TestAggregateHandlerReturnMetricsCount(t *testing.T) {
	for _, body := range []string{`{}`, `{"return_metrics":[]}`, `{"return_metrics":["count"]}`, `{"return_metrics":["count","count"]}`} {
		t.Run(body, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.searcher.aggregateRes = ungroupedCount(7)

			payload, apiErr := doAggregate(t, deps, nil, "Movie", body)
			require.Nil(t, apiErr)
			require.NotNil(t, payload.Count)
			assert.Equal(t, int64(7), *payload.Count)
		})
	}
}

func TestAggregateReservedFieldsRejected(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"over", `{"over":{"near_text":{"query":["space"]}}}`, "over is not yet supported"},
		{"over empty object", `{"over":{}}`, "over is not yet supported"},
		{"object_limit", `{"object_limit":100}`, "object_limit is not yet supported"},
		{"metric grammar", `{"return_metrics":["price:mean"]}`, `"price:mean" is not yet supported`},
		{"unknown metric", `{"return_metrics":["mean"]}`, `unknown return_metrics entry "mean"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newTestHandler(t)

			_, apiErr := doAggregate(t, deps, nil, "Movie", tt.body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
			assert.Contains(t, apiErr.Error(), tt.want)
			// reserved fields are rejected before any schema access
			assert.Empty(t, deps.authorizer.Calls())
		})
	}
}

// TestAggregateReservedFieldsAbsentOrNullAllowed: an explicit null reserved
// field is treated as absent, like everywhere in the family (the SearchCommon
// pin is TestReservedFieldsAbsentOrNullAllowed). over is the family's first
// interface{}-typed reserved field — null must decode to a nil interface,
// not a typed non-nil one — so its decode is pinned explicitly alongside the
// pointer-typed object_limit.
func TestAggregateReservedFieldsAbsentOrNullAllowed(t *testing.T) {
	assert.Nil(t, mustAggregateModel(t, `{"over":null}`).Over)
	assert.Nil(t, mustAggregateModel(t, `{"object_limit":null}`).ObjectLimit)

	tests := []struct {
		body string
		res  *aggregation.Result
	}{
		{`{"over":null}`, ungroupedCount(1)},
		{`{"object_limit":null}`, ungroupedCount(1)},
		// nulls combined with valid functional fields
		{`{"over":null,"object_limit":null,"return_metrics":["count"],"group_by":"year"}`, &aggregation.Result{}},
	}
	for _, tt := range tests {
		t.Run(tt.body, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.searcher.aggregateRes = tt.res

			_, apiErr := doAggregate(t, deps, nil, "Movie", tt.body)
			assert.Nil(t, apiErr)
		})
	}
}

func TestAggregateHandlerUnknownBodyFieldIgnored(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.aggregateRes = ungroupedCount(1)

	// unknown fields are silently ignored (platform parity)
	_, apiErr := doAggregate(t, deps, nil, "Movie", `{"not_a_field":1}`)
	assert.Nil(t, apiErr)
}

// TestAggregateHandlerGroupBy: group_by flows into params.GroupBy as a
// single-segment path, limit caps the number of groups, and the reply is the
// grouped form.
func TestAggregateHandlerGroupBy(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.aggregateRes = &aggregation.Result{Groups: []aggregation.Group{
		{GroupedBy: &aggregation.GroupedBy{Path: []string{"year"}, Value: float64(1999)}, Count: 3},
		{GroupedBy: &aggregation.GroupedBy{Path: []string{"year"}, Value: float64(2021)}, Count: 1},
	}}

	payload, apiErr := doAggregate(t, deps, nil, "Movie", `{"group_by":"year","limit":2}`)
	require.Nil(t, apiErr)

	assert.Nil(t, payload.Count, "a grouped reply must not carry the flat count")
	require.Len(t, payload.Groups, 2)
	require.NotNil(t, payload.Groups[0].Count)
	assert.Equal(t, int64(3), *payload.Groups[0].Count)
	require.NotNil(t, payload.Groups[0].GroupedBy)
	assert.Equal(t, []string{"year"}, payload.Groups[0].GroupedBy.Path)
	assert.Equal(t, float64(1999), payload.Groups[0].GroupedBy.Value)

	params := deps.searcher.lastAggregateParams
	require.NotNil(t, params)
	require.NotNil(t, params.GroupBy)
	assert.Equal(t, "year", params.GroupBy.Property.String())
	assert.Equal(t, "Movie", params.GroupBy.Class.String())
	require.NotNil(t, params.Limit)
	assert.Equal(t, 2, *params.Limit)
	assert.True(t, params.IncludeMetaCount)
}

// TestAggregateHandlerGroupByNormalized: an uppercase first letter is
// normalized to the schema's canonical property spelling — the grouper
// matches property names exactly and would otherwise return zero groups.
func TestAggregateHandlerGroupByNormalized(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.aggregateRes = &aggregation.Result{}

	_, apiErr := doAggregate(t, deps, nil, "Movie", `{"group_by":"Year"}`)
	require.Nil(t, apiErr)
	require.NotNil(t, deps.searcher.lastAggregateParams.GroupBy)
	assert.Equal(t, "year", deps.searcher.lastAggregateParams.GroupBy.Property.String())
}

func TestAggregateGroupByValidation(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		want       string
	}{
		// the engine would silently return zero groups for an unknown
		// property; the handler rejects it deterministically
		{"unknown property", `{"group_by":"nope"}`, http.StatusBadRequest, "nope"},
		{"dotted path", `{"group_by":"hasAuthor.name"}`, http.StatusUnprocessableEntity, "not yet supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newTestHandler(t)

			_, apiErr := doAggregate(t, deps, nil, "Movie", tt.body)
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status)
			assert.Contains(t, apiErr.Error(), tt.want)
		})
	}
}

func TestAggregateLimitValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"limit without group_by", `{"limit":5}`, "requires group_by"},
		{"limit zero", `{"group_by":"year","limit":0}`, "must be positive"},
		{"limit negative", `{"group_by":"year","limit":-3}`, "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newTestHandler(t)

			_, apiErr := doAggregate(t, deps, nil, "Movie", tt.body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), tt.want)
		})
	}
}

// TestAggregateHandlerWhere: the where filter reuses the family's parser —
// a valid filter lands in params.Filters, an unknown property is a 400.
func TestAggregateHandlerWhere(t *testing.T) {
	t.Run("valid filter", func(t *testing.T) {
		deps := newTestHandler(t)
		deps.searcher.aggregateRes = ungroupedCount(1)

		_, apiErr := doAggregate(t, deps, nil, "Movie",
			`{"where":{"operator":"Equal","path":["title"],"valueText":"Dune"}}`)
		require.Nil(t, apiErr)
		params := deps.searcher.lastAggregateParams
		require.NotNil(t, params.Filters)
		assert.False(t, aggregation.IsCountStar(params),
			"a filtered aggregation must not take the count-star fast path")
	})

	t.Run("unknown filter property", func(t *testing.T) {
		deps := newTestHandler(t)

		_, apiErr := doAggregate(t, deps, nil, "Movie",
			`{"where":{"operator":"Equal","path":["nope"],"valueText":"x"}}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestAggregateHandlerDisabled(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)

	_, apiErr := doAggregate(t, deps, nil, "Movie", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "not enabled")
	assert.Contains(t, apiErr.Error(), "EXPERIMENTAL_REST_SEARCH_ENABLED")
}

// TestAggregateHandlerDisabledMissingCollection: a disabled endpoint answers
// 422 even for a missing collection (the not-enabled check runs after authz,
// before existence).
func TestAggregateHandlerDisabledMissingCollection(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)

	_, apiErr := doAggregate(t, deps, nil, "NoSuchCollection", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "not enabled")
}

// TestAggregateHandlerDisabledUnauthorized: an unauthorized caller gets 403,
// not the not-enabled 422 (a denied caller must not learn the endpoint is
// off).
func TestAggregateHandlerDisabledUnauthorized(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)
	deps.authorizer.SetErr(autherrs.NewForbidden(&models.Principal{Username: "someone"}, "read", "collections/Movie"))

	_, apiErr := doAggregate(t, deps, nil, "Movie", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
}

func TestAggregateHandlerAuthorizesBeforeSchemaAccess(t *testing.T) {
	deps := newTestHandler(t)
	deps.authorizer.SetErr(autherrs.NewForbidden(&models.Principal{Username: "someone"}, "read", "collections/Unknown"))

	// unknown collection AND unauthorized: authz runs first, so the caller
	// must not learn whether the collection exists
	_, apiErr := doAggregate(t, deps, nil, "Unknown", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
}

func TestAggregateHandlerUnknownCollection(t *testing.T) {
	deps := newTestHandler(t)

	_, apiErr := doAggregate(t, deps, nil, "Unknown", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "could not find collection")
}

func TestAggregateHandlerTenantAuthorization(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.aggregateRes = ungroupedCount(1)

	_, apiErr := doAggregate(t, deps, nil, "Movie", `{"tenant":"tenantA"}`)
	require.Nil(t, apiErr)

	calls := deps.authorizer.Calls()
	require.NotEmpty(t, calls)
	assert.Contains(t, calls[0].Resources[0], "tenantA")
	assert.Equal(t, "tenantA", deps.searcher.lastAggregateParams.Tenant)
}

func TestAggregateHandlerResolvesAliases(t *testing.T) {
	deps := newTestHandler(t)
	deps.schemaReader.aliases = map[string]string{"Films": "Movie"}
	deps.searcher.aggregateRes = ungroupedCount(1)

	_, apiErr := doAggregate(t, deps, nil, "Films", `{}`)
	require.Nil(t, apiErr)
	assert.Equal(t, "Movie", deps.searcher.lastAggregateParams.ClassName.String())
}

func TestAggregateHandlerStripsNamespaceFromErrors(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.namespacesEnabled = true
	principal := &models.Principal{Username: "someone", Namespace: "ns1"}

	// unknown collection: the internal error names the qualified collection
	// ("ns1:Unknown"); the caller must only ever see its own short name
	_, apiErr := doAggregate(t, deps, principal, "Unknown", `{}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "could not find collection Unknown")
	assert.NotContains(t, apiErr.Error(), "ns1:")
}

// TestAggregateTraverserErrorMapping pins the traverser-error statuses for
// the aggregate path. The tenant sentinels arrive the way the router
// attaches them on read-routing-plan failures (cluster/router/router.go),
// wrapped the way Index.aggregate wraps shard errors.
func TestAggregateTraverserErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name: "tenant not found (sentinel through the router chain)",
			err: pkgerrors.Wrapf(
				objects.NewErrMultiTenancy(fmt.Errorf("%w: %q", enterrors.ErrTenantNotFound, "unknownTenant")),
				"aggregate"),
			wantStatus: http.StatusNotFound,
		},
		{
			name: "tenant not active (sentinel through the router chain)",
			err: pkgerrors.Wrapf(
				objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", enterrors.ErrTenantNotActive, "coldTenant")),
				"aggregate"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "tenant on non-multi-tenant collection",
			err: objects.NewErrMultiTenancy(
				fmt.Errorf("class Movie has multi-tenancy disabled, but request was with tenant")),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "missing tenant on multi-tenant collection",
			err: objects.NewErrMultiTenancy(
				fmt.Errorf("class Movie has multi-tenancy enabled, but request was without tenant")),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			// the traverser revalidates filters and wraps failures in the
			// same "invalid 'where' filter" marker the handler's parser uses
			name:       "invalid where filter",
			err:        fmt.Errorf("invalid 'where' filter: no such prop with name 'nope' found"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "filter on property without inverted index",
			err: pkgerrors.Wrapf(
				inverted.NewMissingFilterableIndexError("rating"),
				"aggregate"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "class deleted mid-request (upstream string fallback)",
			err:        fmt.Errorf("could not find class Movie in schema"),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unclassified",
			err:        fmt.Errorf("something exploded"),
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.searcher.aggregateErr = tt.err

			_, apiErr := doAggregate(t, deps, nil, "Movie", `{}`)
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status, apiErr.Error())
		})
	}
}

func TestBuildAggregateResponseUngrouped(t *testing.T) {
	t.Run("count zero still serializes", func(t *testing.T) {
		reply, err := buildAggregateResponse(ungroupedCount(0), false, false, nil, 5*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, reply.Count)
		assert.Equal(t, int64(0), *reply.Count)

		// the wire shape is flat: count + took_ms, no groups key
		raw, err := json.Marshal(reply)
		require.NoError(t, err)
		var asMap map[string]any
		require.NoError(t, json.Unmarshal(raw, &asMap))
		assert.Equal(t, float64(0), asMap["count"])
		assert.Contains(t, asMap, "took_ms")
		assert.NotContains(t, asMap, "groups")
	})

	t.Run("nil result is an internal error", func(t *testing.T) {
		_, err := buildAggregateResponse(nil, false, false, nil, 0)
		assert.ErrorContains(t, err, "no groups")
	})

	t.Run("empty result is an internal error", func(t *testing.T) {
		_, err := buildAggregateResponse(&aggregation.Result{}, false, false, nil, 0)
		assert.ErrorContains(t, err, "no groups")
	})

	t.Run("unexpected result type is an internal error", func(t *testing.T) {
		_, err := buildAggregateResponse("bogus", false, false, nil, 0)
		assert.ErrorContains(t, err, "unexpected aggregate result type")
	})
}

func TestBuildAggregateResponseGrouped(t *testing.T) {
	t.Run("groups carry typed values verbatim", func(t *testing.T) {
		res := &aggregation.Result{Groups: []aggregation.Group{
			{GroupedBy: &aggregation.GroupedBy{Path: []string{"title"}, Value: "Dune"}, Count: 2},
			{GroupedBy: &aggregation.GroupedBy{Path: []string{"inStock"}, Value: true}, Count: 1},
			{GroupedBy: &aggregation.GroupedBy{Path: []string{"year"}, Value: float64(1999)}, Count: 1},
		}}
		reply, err := buildAggregateResponse(res, true, false, nil, 3*time.Millisecond)
		require.NoError(t, err)

		assert.Nil(t, reply.Count)
		require.Len(t, reply.Groups, 3)
		assert.Equal(t, "Dune", reply.Groups[0].GroupedBy.Value)
		assert.Equal(t, true, reply.Groups[1].GroupedBy.Value)
		assert.Equal(t, float64(1999), reply.Groups[2].GroupedBy.Value)

		// zero-valued group values must survive serialization (no omitempty
		// on the required value field)
		raw, err := json.Marshal(&models.AggregateGroup{
			Count:     reply.Groups[1].Count,
			GroupedBy: &models.AggregateGroupedBy{Path: []string{"inStock"}, Value: false},
		})
		require.NoError(t, err)
		assert.Contains(t, string(raw), `"value":false`)
	})

	t.Run("no groups omits the groups key", func(t *testing.T) {
		reply, err := buildAggregateResponse(&aggregation.Result{}, true, false, nil, 0)
		require.NoError(t, err)
		assert.Nil(t, reply.Count)

		raw, err := json.Marshal(reply)
		require.NoError(t, err)
		var asMap map[string]any
		require.NoError(t, json.Unmarshal(raw, &asMap))
		assert.NotContains(t, asMap, "groups")
		assert.NotContains(t, asMap, "count")
		assert.Contains(t, asMap, "took_ms")
	})

	t.Run("nil result behaves like no groups", func(t *testing.T) {
		reply, err := buildAggregateResponse(nil, true, false, nil, 0)
		require.NoError(t, err)
		assert.Nil(t, reply.Groups)
	})

	t.Run("missing groupedBy is an internal error", func(t *testing.T) {
		res := &aggregation.Result{Groups: []aggregation.Group{{Count: 1}}}
		_, err := buildAggregateResponse(res, true, false, nil, 0)
		assert.ErrorContains(t, err, "missing groupedBy")
	})
}

// TestGroupValueBeaconStrip: grouping by a ref property yields beacon URIs;
// the caller's own namespace prefix is stripped from the embedded class
// (gRPC replier parity), foreign prefixes and non-beacon values stay
// untouched.
func TestGroupValueBeaconStrip(t *testing.T) {
	ns1 := &models.Principal{Username: "someone", Namespace: "ns1"}

	tests := []struct {
		name      string
		value     any
		isRef     bool
		principal *models.Principal
		want      any
	}{
		{
			name:      "own namespace stripped from beacon",
			value:     "weaviate://localhost/ns1:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			isRef:     true,
			principal: ns1,
			want:      "weaviate://localhost/Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
		},
		{
			name:      "foreign namespace stays",
			value:     "weaviate://localhost/ns2:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			isRef:     true,
			principal: ns1,
			want:      "weaviate://localhost/ns2:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
		},
		{
			name:      "global principal leaves beacon untouched",
			value:     "weaviate://localhost/Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			isRef:     true,
			principal: nil,
			want:      "weaviate://localhost/Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
		},
		{
			// the scheme guard: a ref-typed group with a non-beacon value
			// must not be run through the beacon rewriter
			name:      "non-beacon string on ref group passes through",
			value:     "ns1:not-a-beacon",
			isRef:     true,
			principal: ns1,
			want:      "ns1:not-a-beacon",
		},
		{
			name:      "non-ref group never touches values",
			value:     "weaviate://localhost/ns1:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			isRef:     false,
			principal: ns1,
			want:      "weaviate://localhost/ns1:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
		},
		{
			name:      "non-string value on ref group passes through",
			value:     float64(3),
			isRef:     true,
			principal: ns1,
			want:      float64(3),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, groupValue(tt.value, tt.isRef, tt.principal))
		})
	}
}

// TestAggregateGroupByRefDetection: the handler flags a ref-typed group_by so
// the reply builder strips beacon namespaces; a non-ref group_by is not
// flagged. Exercised through the full handler with a namespaced principal.
func TestAggregateGroupByRefDetection(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.namespacesEnabled = true
	principal := &models.Principal{Username: "someone", Namespace: "ns1"}
	// the namespaced schema stores the qualified class name
	deps.schemaReader.classes["ns1:Movie"] = movieClass()
	deps.searcher.aggregateRes = &aggregation.Result{Groups: []aggregation.Group{
		{
			GroupedBy: &aggregation.GroupedBy{
				Path:  []string{"hasAuthor"},
				Value: "weaviate://localhost/ns1:Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			},
			Count: 2,
		},
	}}

	payload, apiErr := doAggregate(t, deps, principal, "Movie", `{"group_by":"hasAuthor"}`)
	require.Nil(t, apiErr)
	require.Len(t, payload.Groups, 1)
	assert.Equal(t, "weaviate://localhost/Author/73f2eb5f-5abf-447a-81ca-74b1dd168247",
		payload.Groups[0].GroupedBy.Value)
}
