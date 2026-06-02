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

package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/restrictions"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// fakeQuerier records what it was called with and returns canned results.
type fakeQuerier struct {
	calledSearch    bool
	calledAggregate bool
	gotSearchReq    *pbv1.SearchRequest
	gotAggregateReq *pbv1.AggregateRequest
	gotPrincipal    *models.Principal
	gotWhere        *models.WhereFilter

	searchReply    *pbv1.SearchReply
	searchErr      error
	aggregateReply *pbv1.AggregateReply
	aggregateErr   error
}

func (f *fakeQuerier) SearchWithPrincipal(_ context.Context, principal *models.Principal, req *pbv1.SearchRequest, where *models.WhereFilter) (*pbv1.SearchReply, error) {
	f.calledSearch = true
	f.gotSearchReq = req
	f.gotPrincipal = principal
	f.gotWhere = where
	return f.searchReply, f.searchErr
}

func (f *fakeQuerier) AggregateWithPrincipal(_ context.Context, principal *models.Principal, req *pbv1.AggregateRequest, where *models.WhereFilter) (*pbv1.AggregateReply, error) {
	f.calledAggregate = true
	f.gotAggregateReq = req
	f.gotPrincipal = principal
	f.gotWhere = where
	return f.aggregateReply, f.aggregateErr
}

func newTestQueryHandler(querier *fakeQuerier) *restQueryHandler {
	return &restQueryHandler{
		querier: querier,
		// Composer that rejects everything; tests that exercise the happy path
		// use anonymous access so the composer is never consulted.
		authComposer: func(token string, _ []string) (*models.Principal, error) {
			return nil, fmt.Errorf("auth not configured for token %q", token)
		},
		allowAnonymousAccess: true,
		disabled:             false,
		maxBodyBytes:         1 << 20,
		logger:               logrus.New(),
	}
}

func TestMatchRESTQueryPath(t *testing.T) {
	tests := []struct {
		path     string
		wantCol  string
		wantKind restQueryKind
		wantOK   bool
	}{
		{"/v1/Article/query", "", 0, false}, // universal /query removed
		{"/v1/Article/aggregate", "Article", kindAggregate, true},
		{"/v1/My%20Class/query/near-vector", "My Class", kindQueryNearVector, true}, // url-decode
		{"/v1/Article/query/near-vector", "Article", kindQueryNearVector, true},
		{"/v1/Article/query/near-text", "Article", kindQueryNearText, true},
		{"/v1/Article/query/bm25", "Article", kindQueryBM25, true},
		{"/v1/Article/query/hybrid", "Article", kindQueryHybrid, true},
		{"/v1/Article/query/fetch", "Article", kindQueryFetch, true},
		{"/v1/Article/query/near-media", "Article", kindQueryNearMedia, true},
		{"/v1/Article/query/near-imu", "", 0, false},          // media routes collapsed into near-media
		{"/v1/Article/query/near-image", "", 0, false},        // media routes collapsed into near-media
		{"/v1/Article/query/bogus", "", 0, false},             // unknown sub-method
		{"/v1/Article/query/near-vector/extra", "", 0, false}, // too deep
		{"/v1/Article", "", 0, false},
		{"/v1/Article/get", "", 0, false},
		{"/v1//query", "", 0, false},
		{"/v1/graphql", "", 0, false},
		{"/v1/objects/validate", "", 0, false},
		{"/v2/Article/query", "", 0, false},
		{"/Article/query", "", 0, false},
		{"", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			col, kind, ok := matchRESTQueryPath(tt.path)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.wantCol, col)
				assert.Equal(t, tt.wantKind, kind)
			}
		})
	}
}

func TestHTTPStatusForQueryError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"unauthenticated", authzerrors.NewUnauthenticated(), http.StatusUnauthorized},
		{"forbidden", authzerrors.NewForbidden(&models.Principal{Username: "u"}, "read", "X"), http.StatusForbidden},
		{"wrapped forbidden", fmt.Errorf("outer: %w", authzerrors.NewForbidden(&models.Principal{Username: "u"}, "read", "X")), http.StatusForbidden},
		{"limit exceeded", usagelimits.NewLimitExceededError("", usagelimits.LimitObjects, 10), http.StatusTooManyRequests},
		{"restriction violation", restrictions.NewViolationError("", restrictions.RestrictionCompression, "pq", []string{"none"}), http.StatusUnprocessableEntity},
		{"generic", fmt.Errorf("could not find class Foo"), http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, httpStatusForQueryError(tt.err))
		})
	}
}

func doQueryRequest(t *testing.T, h *restQueryHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusTeapot) // sentinel for "fell through"
	})
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.middleware(next).ServeHTTP(rec, req)
	if rec.Code == http.StatusTeapot && nextCalled {
		t.Logf("request fell through to next handler")
	}
	return rec
}

func TestRESTQuery_SearchHappyPath_CollectionFromPathOverridesBody(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{Took: 0.5}}
	h := newTestQueryHandler(q)

	// Body sets a different collection and a limit; path must win for collection.
	rec := doQueryRequest(t, h, http.MethodPost, "/v1/RightName/query/fetch", `{"collection":"WrongName","limit":7}`)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.True(t, q.calledSearch)
	assert.Equal(t, "RightName", q.gotSearchReq.Collection, "path collection must override body")
	assert.Equal(t, uint32(7), q.gotSearchReq.Limit, "body fields must be parsed")
	assert.Contains(t, rec.Body.String(), "took")
}

func TestRESTQuery_EmptyBodyIsEmptyRequest(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", "")

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, q.calledSearch)
	assert.Equal(t, "Article", q.gotSearchReq.Collection)
	assert.Equal(t, uint32(0), q.gotSearchReq.Limit)
}

func TestRESTQuery_AggregateHappyPath(t *testing.T) {
	q := &fakeQuerier{aggregateReply: &pbv1.AggregateReply{Took: 0.1}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/aggregate", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, q.calledAggregate)
	assert.Equal(t, "Article", q.gotAggregateReq.Collection)
}

func TestRESTQuery_MalformedBody(t *testing.T) {
	q := &fakeQuerier{}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{not valid json`)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.False(t, q.calledSearch, "querier must not be called on parse failure")
}

func TestRESTQuery_Disabled(t *testing.T) {
	q := &fakeQuerier{}
	h := newTestQueryHandler(q)
	h.disabled = true

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{}`)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "rest query api is disabled")
	assert.False(t, q.calledSearch)
}

func TestRESTQuery_ErrorMappingFromPipeline(t *testing.T) {
	q := &fakeQuerier{searchErr: authzerrors.NewForbidden(&models.Principal{Username: "u"}, "read", "Article")}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{}`)

	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestRESTQuery_AnonymousAllowed_PassesNilPrincipal(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, q.calledSearch)
	assert.Nil(t, q.gotPrincipal)
}

func TestRESTQuery_AnonymousDisabled_RejectsMissingToken(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)
	h.allowAnonymousAccess = false // composer will reject the empty token

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{}`)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, q.calledSearch)
}

func TestRESTQuery_BearerTokenIsValidated(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)
	h.allowAnonymousAccess = false
	gotToken := ""
	h.authComposer = func(token string, _ []string) (*models.Principal, error) {
		gotToken = token
		return &models.Principal{Username: "alice"}, nil
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Fatal("should not fall through") })
	req := httptest.NewRequest(http.MethodPost, "/v1/Article/query/fetch", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer s3cret")
	rec := httptest.NewRecorder()
	h.middleware(next).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "s3cret", gotToken)
	require.NotNil(t, q.gotPrincipal)
	assert.Equal(t, "alice", q.gotPrincipal.Username)
}

func TestRESTQuery_WhereFilterParsedAndPassed(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch",
		`{"limit":5,"where":{"operator":"Equal","path":["category"],"valueText":"fantasy"}}`)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, q.calledSearch)
	// The `where` field is stripped before the protojson parse, so the gRPC
	// request still parses (limit=5) and carries no protobuf filter.
	assert.Equal(t, uint32(5), q.gotSearchReq.Limit)
	assert.Nil(t, q.gotSearchReq.Filters)
	// The WhereFilter is parsed and handed to the pipeline.
	require.NotNil(t, q.gotWhere)
	assert.Equal(t, "Equal", q.gotWhere.Operator)
	assert.Equal(t, []string{"category"}, q.gotWhere.Path)
	require.NotNil(t, q.gotWhere.ValueText)
	assert.Equal(t, "fantasy", *q.gotWhere.ValueText)
}

func TestRESTQuery_WhereOnAggregate(t *testing.T) {
	q := &fakeQuerier{aggregateReply: &pbv1.AggregateReply{}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/aggregate",
		`{"objectsCount":true,"where":{"operator":"Equal","path":["category"],"valueText":"scifi"}}`)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, q.calledAggregate)
	require.NotNil(t, q.gotWhere)
	assert.Equal(t, "Equal", q.gotWhere.Operator)
}

func TestRESTQuery_WhereAndFiltersConflict(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)

	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{
		"where":{"operator":"Equal","path":["category"],"valueText":"fantasy"},
		"filters":{"operator":"OPERATOR_EQUAL","valueText":"fantasy","target":{"property":"category"}}
	}`)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "not both")
	assert.False(t, q.calledSearch, "querier must not be called when both filter forms are set")
}

func TestRESTQuery_MalformedWhere(t *testing.T) {
	q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
	h := newTestQueryHandler(q)

	// `where` must be an object; a scalar fails to parse into a WhereFilter.
	rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch", `{"where":123}`)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.False(t, q.calledSearch)
}

func TestRESTQuery_ConsistencyLevelShorthand(t *testing.T) {
	cases := []struct {
		in   string
		want pbv1.ConsistencyLevel
	}{
		{"ONE", pbv1.ConsistencyLevel_CONSISTENCY_LEVEL_ONE},
		{"QUORUM", pbv1.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM},
		{"all", pbv1.ConsistencyLevel_CONSISTENCY_LEVEL_ALL},                         // case-insensitive
		{"CONSISTENCY_LEVEL_QUORUM", pbv1.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM}, // full form still works
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
			h := newTestQueryHandler(q)
			rec := doQueryRequest(t, h, http.MethodPost, "/v1/Article/query/fetch",
				`{"consistencyLevel":"`+tc.in+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)
			require.True(t, q.calledSearch)
			require.NotNil(t, q.gotSearchReq.ConsistencyLevel)
			assert.Equal(t, tc.want, *q.gotSearchReq.ConsistencyLevel)
		})
	}
}

func TestRESTQuery_PerMethodEndpoints_Accept(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		want func(*pbv1.SearchRequest) bool
	}{
		{"near-vector", "/v1/Article/query/near-vector", `{"nearVector":{"vector":[1,0,0]}}`, func(r *pbv1.SearchRequest) bool { return r.NearVector != nil }},
		{"bm25", "/v1/Article/query/bm25", `{"bm25Search":{"query":"x"}}`, func(r *pbv1.SearchRequest) bool { return r.Bm25Search != nil }},
		{"hybrid", "/v1/Article/query/hybrid", `{"hybridSearch":{"query":"x"}}`, func(r *pbv1.SearchRequest) bool { return r.HybridSearch != nil }},
		{"near-object", "/v1/Article/query/near-object", `{"nearObject":{"id":"abc"}}`, func(r *pbv1.SearchRequest) bool { return r.NearObject != nil }},
		{"fetch", "/v1/Article/query/fetch", `{"limit":3}`, func(r *pbv1.SearchRequest) bool { return r.Limit == 3 }},
		// near-media accepts any one of the six media search fields.
		{"near-media image", "/v1/Article/query/near-media", `{"nearImage":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearImage != nil }},
		{"near-media audio", "/v1/Article/query/near-media", `{"nearAudio":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearAudio != nil }},
		{"near-media video", "/v1/Article/query/near-media", `{"nearVideo":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearVideo != nil }},
		{"near-media depth", "/v1/Article/query/near-media", `{"nearDepth":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearDepth != nil }},
		{"near-media thermal", "/v1/Article/query/near-media", `{"nearThermal":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearThermal != nil }},
		{"near-media imu", "/v1/Article/query/near-media", `{"nearImu":{}}`, func(r *pbv1.SearchRequest) bool { return r.NearImu != nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
			h := newTestQueryHandler(q)
			rec := doQueryRequest(t, h, http.MethodPost, tc.path, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)
			require.True(t, q.calledSearch)
			assert.True(t, tc.want(q.gotSearchReq), "request did not carry the expected field")
		})
	}
}

func TestRESTQuery_PerMethodEndpoints_RejectMismatch(t *testing.T) {
	cases := []struct {
		name, path, body string
	}{
		{"bm25 body on near-vector", "/v1/Article/query/near-vector", `{"bm25Search":{"query":"x"}}`},
		{"nearVector body on bm25", "/v1/Article/query/bm25", `{"nearVector":{"vector":[1,0,0]}}`},
		{"search method on fetch", "/v1/Article/query/fetch", `{"nearVector":{"vector":[1,0,0]}}`},
		{"empty body on near-vector (missing field)", "/v1/Article/query/near-vector", `{}`},
		{"two methods on hybrid", "/v1/Article/query/hybrid", `{"hybridSearch":{"query":"x"},"bm25Search":{"query":"x"}}`},
		{"non-media method on near-media", "/v1/Article/query/near-media", `{"nearVector":{"vector":[1,0,0]}}`},
		{"no method on near-media", "/v1/Article/query/near-media", `{}`},
		{"two media methods on near-media", "/v1/Article/query/near-media", `{"nearImage":{},"nearAudio":{}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeQuerier{searchReply: &pbv1.SearchReply{}}
			h := newTestQueryHandler(q)
			rec := doQueryRequest(t, h, http.MethodPost, tc.path, tc.body)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			assert.False(t, q.calledSearch, "querier must not be called when body doesn't match the endpoint")
		})
	}
}

func TestRESTQuery_NonMatchingRequestsFallThrough(t *testing.T) {
	q := &fakeQuerier{}
	h := newTestQueryHandler(q)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/Article/query"},            // universal /query removed
		{http.MethodGet, "/v1/Article/query/near-vector"}, // right path, wrong method
		{http.MethodPost, "/v1/objects"},                  // unrelated route
		{http.MethodPost, "/v1/Article/get"},              // wrong verb
		{http.MethodPost, "/v1/schema/Article"},           // unrelated route
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := doQueryRequest(t, h, tc.method, tc.path, `{}`)
			assert.Equal(t, http.StatusTeapot, rec.Code, "should fall through to next")
			assert.False(t, q.calledSearch)
			assert.False(t, q.calledAggregate)
		})
	}
}
