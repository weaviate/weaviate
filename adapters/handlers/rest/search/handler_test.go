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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	pkgerrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/configvalidation"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
)

type fakeSearcher struct {
	lastParams dto.GetParams
	res        []any
	err        error

	lastAggregateParams *aggregation.Params
	aggregateRes        any
	aggregateErr        error
}

func (f *fakeSearcher) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]any, error) {
	f.lastParams = params
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
}

func (f *fakeSearcher) Aggregate(ctx context.Context, principal *models.Principal,
	params *aggregation.Params,
) (any, error) {
	f.lastAggregateParams = params
	if f.aggregateErr != nil {
		return nil, f.aggregateErr
	}
	return f.aggregateRes, nil
}

type fakeSchemaReader struct {
	classes map[string]*models.Class
	aliases map[string]string
}

func (f *fakeSchemaReader) ReadOnlyClass(name string) *models.Class {
	return f.classes[name]
}

func (f *fakeSchemaReader) ResolveAlias(alias string) string {
	return f.aliases[alias]
}

func movieClass() *models.Class {
	return &models.Class{
		Class:      "Movie",
		Vectorizer: "text2vec-contextionary",
		VectorIndexConfig: hnsw.UserConfig{
			Distance: "cosine",
		},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			{Name: "poster", DataType: schema.DataTypeBlob.PropString()},
			{Name: "hasAuthor", DataType: []string{"Author"}},
		},
	}
}

func authorClass() *models.Class {
	return &models.Class{
		Class:      "Author",
		Vectorizer: "text2vec-contextionary",
		VectorIndexConfig: hnsw.UserConfig{
			Distance: "cosine",
		},
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
			{Name: "age", DataType: schema.DataTypeInt.PropString()},
		},
	}
}

type testDeps struct {
	searcher     *fakeSearcher
	schemaReader *fakeSchemaReader
	authorizer   *mocks.FakeAuthorizer
	handler      *Handler
}

func newTestHandler(t *testing.T) *testDeps {
	t.Helper()
	deps := &testDeps{
		searcher: &fakeSearcher{},
		schemaReader: &fakeSchemaReader{
			classes: map[string]*models.Class{
				"Movie":  movieClass(),
				"Author": authorClass(),
			},
		},
		authorizer: mocks.NewMockAuthorizer(),
	}
	deps.handler = NewHandler(HandlerConfig{
		Traverser:      deps.searcher,
		SchemaReader:   deps.schemaReader,
		Authorizer:     deps.authorizer,
		DefaultLimit:   10,
		MaximumResults: 10000,
		// happy-path fixture: the experimental feature is enabled
		Enabled: runtime.NewDynamicValue(true),
		Logger:  logrus.New(),
	})
	return deps
}

// mustModel unmarshals a JSON body into the typed request model the way the
// swagger JSON consumer does — unknown fields are ignored, type mismatches
// (e.g. a string `query`) fail. It requires success, so tests that exercise
// the handler pass bodies that decode; the decode-failure contract is tested
// separately in TestQueryStringFormRejectedAtDecode.
func mustModel(t *testing.T, body string) *models.SearchNearTextRequest {
	t.Helper()
	var req models.SearchNearTextRequest
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

// doNearText runs the handler the way the generated operation wiring does,
// with the typed, already-decoded request model.
func doNearText(t *testing.T, deps *testDeps, principal *models.Principal,
	collection, body string,
) (*models.SearchResponse, *APIError) {
	t.Helper()
	return deps.handler.NearText(context.Background(), principal, collection, mustModel(t, body))
}

// mustBm25Model is mustModel for the bm25 request model.
func mustBm25Model(t *testing.T, body string) *models.SearchBm25Request {
	t.Helper()
	var req models.SearchBm25Request
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

// doBm25 runs the bm25 handler the way the generated operation wiring does,
// with the typed, already-decoded request model.
func doBm25(t *testing.T, deps *testDeps, principal *models.Principal,
	collection, body string,
) (*models.SearchResponse, *APIError) {
	t.Helper()
	return deps.handler.Bm25(context.Background(), principal, collection, mustBm25Model(t, body))
}

func TestIsSearchRoute(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/v1/search/Movie/near-text", true},
		{"/v1/search/movie/near-text", true},
		// the collection segment can be any name, including a reserved root
		// spelling or the literal "search"
		{"/v1/search/objects/near-text", true},
		{"/v1/search/backups/near-text", true},
		{"/v1/search/search/near-text", true},
		// any search-type under the namespace counts
		{"/v1/search/Movie/hybrid", true},
		{"/v1/search/Movie/bm25", true},
		// non-canonical spellings the router still routes must classify as search
		{"/v1/search/Movie/near-text/", true},
		{"/v1//search/Movie/near-text", true},
		{"/v1/search/Movie/./near-text", true},
		{"/v1/search/Movie/sub/../near-text", true},
		{"/v1/search/Movie", false},
		{"/v1/search/near-text", false},
		// doubled slash = missing collection segment; router wouldn't route it
		{"/v1/search//near-text", false},
		{"/v2/search/Movie/near-text", false},
		{"/v1/Search/Movie/near-text", false},
		// a collection-first path is not under the search namespace
		{"/v1/Movie/search/near-text", false},
		{"/v1", false},
		{"/", false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSearchRoute(tt.path))
		})
	}
}

// TestExecuteIsSearchTypeAgnostic exercises the generic orchestrator
// directly with a stub params builder — the seam every search type reuses.
// It confirms execute() runs the fixed flow (authz, traverser, reply) and
// delegates only the dto.GetParams construction.
func TestExecuteIsSearchTypeAgnostic(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
		},
	}

	var gotClass string
	build := func(class *models.Class, className string,
		getClass classGetterFunc,
	) (dto.GetParams, *APIError) {
		gotClass = className
		return dto.GetParams{
			ClassName:            className,
			Properties:           search.SelectProperties{{Name: "title", IsPrimitive: true}},
			AdditionalProperties: additional.Properties{ID: true},
		}, nil
	}

	payload, apiErr := deps.handler.execute(context.Background(), nil, "Movie", "",
		&models.SearchCommon{}, build)
	require.Nil(t, apiErr)
	assert.Equal(t, "Movie", gotClass)
	require.Len(t, payload.Results, 1)
	assert.Equal(t, "Dune", payload.Results[0].Properties["title"])

	// execute honors the not-enabled gate and the reserved-field gate,
	// before ever calling the builder
	deps.handler.enabled = runtime.NewDynamicValue(false)
	_, apiErr = deps.handler.execute(context.Background(), nil, "Movie", "",
		&models.SearchCommon{}, build)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

	deps.handler.enabled = runtime.NewDynamicValue(true)
	rerankProp := "title"
	rerank := &models.SearchRerank{Property: &rerankProp}
	_, apiErr = deps.handler.execute(context.Background(), nil, "Movie", "",
		&models.SearchCommon{Rerank: rerank}, build)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "rerank")
}

func TestHandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"year":  float64(2021),
			"_additional": map[string]any{
				"distance": float32(0.12),
			},
		},
	}

	payload, apiErr := doNearText(t, deps, nil, "Movie",
		`{"query":["space opera"],"limit":5,"returnProperties":["title","year"],"returnMetadata":["distance"]}`)
	require.Nil(t, apiErr)

	require.Len(t, payload.Results, 1)
	obj := payload.Results[0]
	assert.Equal(t, "Dune", obj.Properties["title"])
	assert.Equal(t, float64(2021), obj.Properties["year"])
	require.NotNil(t, obj.ID)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", obj.ID.String())
	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Distance)
	assert.Equal(t, float32(0.12), *obj.Metadata.Distance)
	require.NotNil(t, payload.TookMs)
	assert.GreaterOrEqual(t, *payload.TookMs, int64(0))

	// the traverser was called with the parsed params
	params := deps.searcher.lastParams
	assert.Equal(t, "Movie", params.ClassName)
	assert.Equal(t, 5, params.Pagination.Limit)
	require.Contains(t, params.ModuleParams, "nearText")
}

// TestHandlerIDAlwaysReturned: every hit carries its id on the envelope,
// with or without returnMetadata, and an id-only request produces no
// metadata block.
func TestHandlerIDAlwaysReturned(t *testing.T) {
	for name, body := range map[string]string{
		"returnMetadata omitted": `{"query":["space"]}`,
		"returnMetadata empty":   `{"query":["space"],"returnMetadata":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.searcher.res = []any{
				map[string]any{
					"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
					"title": "Dune",
				},
			}

			payload, apiErr := doNearText(t, deps, nil, "Movie", body)
			require.Nil(t, apiErr)
			require.Len(t, payload.Results, 1)
			obj := payload.Results[0]
			require.NotNil(t, obj.ID)
			assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", obj.ID.String())
			assert.Nil(t, obj.Metadata)
		})
	}
}

func TestHandlerDisabled(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	// mirrors DISABLE_GRAPHQL: the operation stays registered and rejects
	// requests with 422
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "not enabled")
	assert.Contains(t, apiErr.Error(), "EXPERIMENTAL_REST_SEARCH_ENABLED")
}

// TestHandlerDefaultDisabled guards the new opt-in default: a handler whose
// flag is off (the unset-env default, NewDynamicValue(false)) rejects every
// search with the not-enabled 422.
func TestHandlerDefaultDisabled(t *testing.T) {
	deps := newTestHandler(t)
	// simulate the process default: EXPERIMENTAL_REST_SEARCH_ENABLED unset
	deps.handler.enabled = runtime.NewDynamicValue(false)

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "not enabled")
}

// TestHandlerDisabledMissingCollection: a disabled endpoint answers 422 even
// for a missing collection (the not-enabled check runs after authz, before
// existence).
func TestHandlerDisabledMissingCollection(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)

	_, apiErr := doNearText(t, deps, nil, "NoSuchCollection", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "not enabled")
}

// TestHandlerDisabledUnauthorized: unauthorized caller gets 403, not the
// not-enabled 422 (a denied caller must not learn the endpoint is off).
func TestHandlerDisabledUnauthorized(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.enabled = runtime.NewDynamicValue(false)
	deps.authorizer.SetErr(autherrs.NewForbidden(&models.Principal{Username: "someone"}, "read", "collections/Movie"))

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
}

func TestHandlerUnknownBodyFieldIgnored(t *testing.T) {
	deps := newTestHandler(t)

	// unknown fields are silently ignored
	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"],"not_a_field":1}`)
	assert.Nil(t, apiErr)
}

func TestQueryStringFormRejectedAtDecode(t *testing.T) {
	// query is array-only: the string form does not decode into the model
	var req models.SearchNearTextRequest
	err := json.Unmarshal([]byte(`{"query":"space"}`), &req)
	assert.Error(t, err)

	require.NoError(t, json.Unmarshal([]byte(`{"query":["space"]}`), &req))
	assert.Equal(t, []string{"space"}, req.Query)
}

func TestHandlerEmptyQuery(t *testing.T) {
	deps := newTestHandler(t)

	// swagger's required validation rejects an absent query with 422; an
	// explicit empty array reaches the handler and is a 400
	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":[]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "query")
}

func TestHandlerAuthorizationFailure(t *testing.T) {
	deps := newTestHandler(t)
	deps.authorizer.SetErr(autherrs.NewForbidden(&models.Principal{Username: "someone"}, "read", "collections/Movie"))

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
}

func TestHandlerAuthorizesBeforeSchemaAccess(t *testing.T) {
	deps := newTestHandler(t)
	deps.authorizer.SetErr(autherrs.NewForbidden(&models.Principal{Username: "someone"}, "read", "collections/Unknown"))

	// unknown collection AND unauthorized: authz runs first, so the caller
	// must not learn whether the collection exists
	_, apiErr := doNearText(t, deps, nil, "Unknown", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
}

func TestHandlerUnknownCollection(t *testing.T) {
	deps := newTestHandler(t)

	_, apiErr := doNearText(t, deps, nil, "Unknown", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "could not find collection")
}

func TestHandlerTenantAuthorization(t *testing.T) {
	deps := newTestHandler(t)

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"],"tenant":"tenantA"}`)
	require.Nil(t, apiErr)

	calls := deps.authorizer.Calls()
	require.NotEmpty(t, calls)
	assert.Contains(t, calls[0].Resources[0], "tenantA")
	assert.Equal(t, "tenantA", deps.searcher.lastParams.Tenant)
}

func TestHandlerResolvesAliases(t *testing.T) {
	deps := newTestHandler(t)
	deps.schemaReader.aliases = map[string]string{"Films": "Movie"}

	_, apiErr := doNearText(t, deps, nil, "Films", `{"query":["space"]}`)
	require.Nil(t, apiErr)
	assert.Equal(t, "Movie", deps.searcher.lastParams.ClassName)
}

// rbacLikeAuthorizer denies every request with a Forbidden derived from the
// resources and wrapped like the real RBAC authorizer ("rbac: %w"), so a
// denial's shape depends on the resolved collection. The shared FakeAuthorizer
// returns a fixed error and can't catch the alias oracle these tests guard.
type rbacLikeAuthorizer struct{}

func (rbacLikeAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	p := principal
	if p == nil {
		p = &models.Principal{Username: "anonymous"}
	}
	return fmt.Errorf("rbac: %w", autherrs.NewForbidden(p, verb, resources...))
}

func (a rbacLikeAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a rbacLikeAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	return nil, a.Authorize(ctx, principal, verb, resources...)
}

// TestHandlerAliasForbiddenDoesNotLeakTarget: a denied request against an
// alias must not disclose the alias's target collection in the 403.
func TestHandlerAliasForbiddenDoesNotLeakTarget(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.authorizer = rbacLikeAuthorizer{}
	deps.schemaReader.aliases = map[string]string{"Films": "Movie"}

	_, apiErr := doNearText(t, deps, nil, "Films", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
	assert.NotContains(t, apiErr.Error(), "Movie", "403 must not name the alias target")
	assert.Contains(t, apiErr.Error(), "Films")
}

// TestHandlerAliasForbiddenNoExistenceOracle: for one name, a denial must be
// byte-identical whether the name is a registered alias or a plain unknown
// collection — else the difference reveals the alias to a denied caller.
func TestHandlerAliasForbiddenNoExistenceOracle(t *testing.T) {
	// registered alias Films -> Movie, caller denied everything
	aliased := newTestHandler(t)
	aliased.handler.authorizer = rbacLikeAuthorizer{}
	aliased.schemaReader.aliases = map[string]string{"Films": "Movie"}
	_, aliasErr := doNearText(t, aliased, nil, "Films", `{"query":["space"]}`)
	require.NotNil(t, aliasErr)

	// same name, not an alias (and not a class), same denied caller
	plain := newTestHandler(t)
	plain.handler.authorizer = rbacLikeAuthorizer{}
	_, plainErr := doNearText(t, plain, nil, "Films", `{"query":["space"]}`)
	require.NotNil(t, plainErr)

	assert.Equal(t, plainErr.Status, aliasErr.Status)
	assert.Equal(t, plainErr.Error(), aliasErr.Error(),
		"alias and non-alias denials must be indistinguishable")
}

// denyCollections denies READ on the named collections (by resource path) and
// allows the rest, to test cross-collection authz for refs and filters.
type denyCollections struct {
	denied   map[string]bool
	requests []mocks.AuthZReq
}

func (a *denyCollections) authorize(principal *models.Principal, verb string, resources []string) error {
	a.requests = append(a.requests, mocks.AuthZReq{Principal: principal, Verb: verb, Resources: resources})
	for _, r := range resources {
		for name := range a.denied {
			if strings.Contains(r, "/collections/"+name+"/") {
				p := principal
				if p == nil {
					p = &models.Principal{Username: "anonymous"}
				}
				return fmt.Errorf("rbac: %w", autherrs.NewForbidden(p, verb, resources...))
			}
		}
	}
	return nil
}

func (a *denyCollections) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.authorize(principal, verb, resources)
}

func (a *denyCollections) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.authorize(principal, verb, resources)
}

func (a *denyCollections) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.authorize(principal, verb, resources); err != nil {
		return nil, err
	}
	return resources, nil
}

func (a *denyCollections) Calls() []mocks.AuthZReq { return a.requests }

// TestHandlerAuthorizesReferencedCollections: collections reached via a
// reference selection or a where filter are authorized, not just the primary.
func TestHandlerAuthorizesReferencedCollections(t *testing.T) {
	for name, body := range map[string]string{
		"reference selection": `{"query":["space"],"returnProperties":["hasAuthor.name"]}`,
		"where filter across a reference": `{"query":["space"],"where":` +
			`{"path":["hasAuthor","Author","name"],"operator":"Equal","valueText":"x"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			deps := newTestHandler(t)
			authz := &denyCollections{denied: map[string]bool{}}
			deps.handler.authorizer = authz

			_, apiErr := doNearText(t, deps, nil, "Movie", body)
			require.Nil(t, apiErr)

			var authorizedAuthor bool
			for _, c := range authz.Calls() {
				for _, r := range c.Resources {
					if strings.Contains(r, "/collections/Author/") {
						authorizedAuthor = true
					}
				}
			}
			assert.True(t, authorizedAuthor,
				"the referenced Author collection must be authorized, not just Movie")
		})
	}
}

// TestHandlerCompoundFilterForbiddenIs403: a READ denial inside a compound
// (And/Or) where filter must be 403 like the same clause alone — the typed
// error must survive the operand merge. The two-failing-operand case exercises
// the multi-error merge path.
func TestHandlerCompoundFilterForbiddenIs403(t *testing.T) {
	bodies := map[string]string{
		"single clause": `{"query":["space"],"where":` +
			`{"path":["hasAuthor","Author","name"],"operator":"Equal","valueText":"x"}}`,
		// one failing operand (Author ref denied); the year clause is valid
		"compound, one failing operand": `{"query":["space"],"where":{"operator":"And","operands":[` +
			`{"path":["hasAuthor","Author","name"],"operator":"Equal","valueText":"x"},` +
			`{"path":["year"],"operator":"GreaterThan","valueInt":2000}]}}`,
		// two failing operands (denial + wrong-type year): merge must keep the
		// Forbidden reachable so status stays 403
		"compound, two failing operands": `{"query":["space"],"where":{"operator":"And","operands":[` +
			`{"path":["hasAuthor","Author","name"],"operator":"Equal","valueText":"x"},` +
			`{"path":["year"],"operator":"Equal","valueText":"not-an-int"}]}}`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.handler.authorizer = &denyCollections{denied: map[string]bool{"Author": true}}

			_, apiErr := doNearText(t, deps, nil, "Movie", body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusForbidden, apiErr.Status, apiErr.Error())
		})
	}
}

func TestHandlerLowercasesCollection(t *testing.T) {
	deps := newTestHandler(t)

	_, apiErr := doNearText(t, deps, nil, "movie", `{"query":["space"]}`)
	require.Nil(t, apiErr)
	assert.Equal(t, "Movie", deps.searcher.lastParams.ClassName)
}

// TestHandlerTraverserErrorMapping builds each error via its real producer
// and the real pkg/errors wrap chain — if a producer stops attaching its
// typed error or a wrap goes back to a chain-breaking %v, a case fails.
func TestHandlerTraverserErrorMapping(t *testing.T) {
	// certainty error via the real producer, wrapped as explorer.go does
	l2Class := movieClass()
	l2Class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
	certaintyErr := configvalidation.CheckCertaintyCompatibility(l2Class, nil)
	require.Error(t, certaintyErr)

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name: "embedding provider failure",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(fmt.Errorf("remote client vectorize: connection refused")),
				"explorer: get class: vectorize params"),
			wantStatus: http.StatusBadGateway,
		},
		{
			// ORDERING GUARD: the no-vectorizer error arrives wrapped inside
			// ErrQueryVectorization; 422 must win over 502
			name: "no vectorizer configured",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(
					enterrors.NewErrNoVectorizerModule(fmt.Errorf("could not vectorize input for collection Movie with search-type nearText, targetVector  and parameters &{}. Make sure a vectorizer module is configured for this class"))),
				"explorer: get class: vectorize params"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			// a class deleted mid-request, in bm25_searcher's phrasing —
			// classified 404 by the not-found marker. index.go's twin
			// ("class ... not found in schema") does not match and stays a
			// 500 (known residual until an ErrClassNotFound sentinel lands).
			name:       "class deleted mid-request",
			err:        pkgerrors.Wrap(fmt.Errorf("could not find class Movie in schema"), "explorer: get class"),
			wantStatus: http.StatusNotFound,
		},
		{
			// typed rate-limit error via the real constructor (drift-guarded)
			name:       "rate limit (typed ErrRateLimit)",
			err:        enterrors.NewErrRateLimit(),
			wantStatus: http.StatusTooManyRequests,
		},
		{
			// sentinel attached the way the MT validator + explorer chain does
			name: "tenant not found (sentinel through the MT/explorer chain)",
			err: pkgerrors.Wrapf(
				objects.NewErrMultiTenancy(fmt.Errorf("%w: %q", enterrors.ErrTenantNotFound, "unknownTenant")),
				"explorer: get class: vector search"),
			wantStatus: http.StatusNotFound,
		},
		{
			name: "tenant not active (sentinel through the MT/explorer chain)",
			err: pkgerrors.Wrapf(
				objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", enterrors.ErrTenantNotActive, "coldTenant")),
				"explorer: get class: vector search"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "tenant on non-multi-tenant collection",
			err: pkgerrors.Wrapf(
				objects.NewErrMultiTenancy(fmt.Errorf("class Movie has multi-tenancy disabled, but request was with tenant")),
				"explorer: get class: vector search"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "missing tenant on multi-tenant collection",
			err: objects.NewErrMultiTenancy(
				fmt.Errorf("class Movie has multi-tenancy enabled, but request was without tenant")),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "collection not found (ours: sentinel)",
			err:        fmt.Errorf("%w %s in schema", errCollectionNotFound, "Movie"),
			wantStatus: http.StatusNotFound,
		},
		{
			// the one remaining string fallback (see errClassNotFoundMarker)
			name:       "class not found (upstream string fallback)",
			err:        fmt.Errorf("could not find class Movie in schema"),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "certainty on non-cosine",
			err:        fmt.Errorf("additional: %w for class: %v", certaintyErr, "Movie"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "filter on property without inverted index",
			err: pkgerrors.Wrapf(
				inverted.NewMissingFilterableIndexError("rating"),
				"explorer: get class: vector search"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "invalid where filter",
			err:        fmt.Errorf("invalid 'where' filter: no such prop with name 'nope' found"),
			wantStatus: http.StatusBadRequest,
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
			deps.searcher.err = tt.err

			_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status, apiErr.Error())
		})
	}
}

// TestBm25HandlerHappyPath: the bm25 wrapper drives the same execute() flow
// as near-text, with KeywordRanking params instead of module params, and the
// envelope carries score/explainScore metadata. Deliberately the ONLY
// per-endpoint handler test: Bm25 is a thin execute() wrapper, and the
// shared gates (disabled, reserved fields, authz order, unknown collection,
// tenant) are pinned once in TestExecuteIsSearchTypeAgnostic and the
// near-text handler tests, plus live in the acceptance suite.
func TestBm25HandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]any{
				"score":        float32(1.5),
				"explainScore": "BM25F: title term frequency",
			},
		},
	}

	payload, apiErr := doBm25(t, deps, nil, "Movie",
		`{"query":"space opera","limit":5,"queryProperties":["title"],"returnProperties":["title"],"returnMetadata":["score","explainScore"]}`)
	require.Nil(t, apiErr)

	require.Len(t, payload.Results, 1)
	obj := payload.Results[0]
	assert.Equal(t, "Dune", obj.Properties["title"])
	require.NotNil(t, obj.ID)
	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Score)
	assert.Equal(t, float32(1.5), *obj.Metadata.Score)
	require.NotNil(t, obj.Metadata.ExplainScore)
	assert.Equal(t, "BM25F: title term frequency", *obj.Metadata.ExplainScore)
	require.NotNil(t, payload.TookMs)

	// the traverser was called with keyword-ranking params, no module params
	params := deps.searcher.lastParams
	assert.Equal(t, "Movie", params.ClassName)
	assert.Equal(t, 5, params.Pagination.Limit)
	require.NotNil(t, params.KeywordRanking)
	assert.Equal(t, "space opera", params.KeywordRanking.Query)
	assert.Equal(t, []string{"title"}, params.KeywordRanking.Properties)
	assert.Empty(t, params.ModuleParams)
}

// mustNearObjectModel is mustModel for the near-object request model.
func mustNearObjectModel(t *testing.T, body string) *models.SearchNearObjectRequest {
	t.Helper()
	var req models.SearchNearObjectRequest
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

// doNearObject runs the near-object handler the way the generated operation
// wiring does, with the typed, already-decoded request model.
func doNearObject(t *testing.T, deps *testDeps, principal *models.Principal,
	collection, body string,
) (*models.SearchResponse, *APIError) {
	t.Helper()
	return deps.handler.NearObject(context.Background(), principal, collection, mustNearObjectModel(t, body))
}

// TestNearObjectHandlerHappyPath: the near-object wrapper drives the same
// execute() flow as the other search types, with NearObject params instead
// of module, keyword or hybrid params.
// Deliberately one of only two near-object handler tests (with the typed
// source-object error mapping below): NearObject is a thin execute()
// wrapper; the shared gates are pinned in TestExecuteIsSearchTypeAgnostic,
// the near-text handler tests and the acceptance suite.
func TestNearObjectHandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]any{
				"distance": float32(0.12),
			},
		},
	}

	payload, apiErr := doNearObject(t, deps, nil, "Movie",
		`{"id":"73f2eb5f-5abf-447a-81ca-74b1dd168247","limit":5,"returnProperties":["title"],"returnMetadata":["distance"]}`)
	require.Nil(t, apiErr)

	require.Len(t, payload.Results, 1)
	obj := payload.Results[0]
	assert.Equal(t, "Dune", obj.Properties["title"])
	require.NotNil(t, obj.ID)
	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Distance)
	assert.Equal(t, float32(0.12), *obj.Metadata.Distance)
	require.NotNil(t, payload.TookMs)

	// the traverser was called with near-object params, nothing else
	params := deps.searcher.lastParams
	assert.Equal(t, "Movie", params.ClassName)
	assert.Equal(t, 5, params.Pagination.Limit)
	require.NotNil(t, params.NearObject)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", params.NearObject.ID)
	assert.Empty(t, params.ModuleParams)
	assert.Nil(t, params.KeywordRanking)
	assert.Nil(t, params.HybridSearch)
}

// TestNearObjectSourceObjectErrorMapping builds the source-object errors via
// their real producer types and the real explorer wrap chain (the explorer
// wraps every vector-resolution failure in ErrQueryVectorization): the typed
// matches must win over the 502 mapping, or an unknown id would surface as
// an embedding-provider failure.
func TestNearObjectSourceObjectErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name: "unknown source object id",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(
					fmt.Errorf("nearObject params: %w", enterrors.NewErrSourceObjectNotFound(fmt.Errorf("vector not found")))),
				"explorer: get class: vectorize search vector"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "source object has no vector",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(
					fmt.Errorf("nearObject params: %w", enterrors.NewErrSourceObjectNoVector(
						fmt.Errorf("nearObject search-object with id 73f2eb5f-5abf-447a-81ca-74b1dd168247 has no vector")))),
				"explorer: get class: vectorize search vector"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "source object has no vector for the target",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(
					fmt.Errorf("nearObject params: %w", enterrors.NewErrSourceObjectNoVector(
						fmt.Errorf("vector not found for target: title_vec")))),
				"explorer: get class: vectorize search vector"),
			wantStatus: http.StatusUnprocessableEntity,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newTestHandler(t)
			deps.searcher.err = tt.err

			_, apiErr := doNearObject(t, deps, nil, "Movie", `{"id":"73f2eb5f-5abf-447a-81ca-74b1dd168247"}`)
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status, apiErr.Error())
		})
	}
}

// mustHybridModel is mustModel for the hybrid request model.
func mustHybridModel(t *testing.T, body string) *models.SearchHybridRequest {
	t.Helper()
	var req models.SearchHybridRequest
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

// doHybrid runs the hybrid handler the way the generated operation wiring
// does, with the typed, already-decoded request model.
func doHybrid(t *testing.T, deps *testDeps, principal *models.Principal,
	collection, body string,
) (*models.SearchResponse, *APIError) {
	t.Helper()
	return deps.handler.Hybrid(context.Background(), principal, collection, mustHybridModel(t, body))
}

// TestHybridHandlerHappyPath: the hybrid wrapper drives the same execute()
// flow as near-text and bm25, with HybridSearch params instead of module or
// keyword params, and the envelope carries score metadata. Deliberately the
// ONLY per-endpoint handler test — the shared gates are pinned once in
// TestExecuteIsSearchTypeAgnostic, the near-text handler tests and the
// acceptance suite.
func TestHybridHandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]any{
				"score": float32(0.9),
			},
		},
	}

	payload, apiErr := doHybrid(t, deps, nil, "Movie",
		`{"query":"space opera","limit":5,"alpha":0.6,"fusionType":"ranked","returnProperties":["title"],"returnMetadata":["score"]}`)
	require.Nil(t, apiErr)

	require.Len(t, payload.Results, 1)
	obj := payload.Results[0]
	assert.Equal(t, "Dune", obj.Properties["title"])
	require.NotNil(t, obj.ID)
	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Score)
	assert.Equal(t, float32(0.9), *obj.Metadata.Score)
	require.NotNil(t, payload.TookMs)

	// the traverser was called with hybrid params, no module or keyword params
	params := deps.searcher.lastParams
	assert.Equal(t, "Movie", params.ClassName)
	assert.Equal(t, 5, params.Pagination.Limit)
	require.NotNil(t, params.HybridSearch)
	assert.Equal(t, "space opera", params.HybridSearch.Query)
	assert.Equal(t, 0.6, params.HybridSearch.Alpha)
	assert.Empty(t, params.ModuleParams)
	assert.Nil(t, params.KeywordRanking)
}

func TestHandlerStripsNamespaceFromErrors(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.namespacesEnabled = true
	principal := &models.Principal{Username: "someone", Namespace: "ns1"}

	// unknown collection: the internal error names the qualified collection
	// ("ns1:Unknown"); the caller must only ever see its own short name
	_, apiErr := doNearText(t, deps, principal, "Unknown", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "could not find collection Unknown")
	assert.NotContains(t, apiErr.Error(), "ns1:")
}
