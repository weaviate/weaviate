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

	"github.com/go-openapi/strfmt"
	pkgerrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
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
	res        []interface{}
	err        error
}

func (f *fakeSearcher) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	f.lastParams = params
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
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
		Traverser:    deps.searcher,
		SchemaReader: deps.schemaReader,
		Authorizer:   deps.authorizer,
		DefaultLimit: 10,
		Logger:       logrus.New(),
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
		{"/v1/search/Movie/near-text/", false},
		{"/v1/search/Movie", false},
		{"/v1/search/near-text", false},
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
	deps.searcher.res = []interface{}{
		map[string]interface{}{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
		},
	}

	var gotClass string
	build := func(class *models.Class, className string,
		getClass func(string) (*models.Class, error),
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
	assert.Equal(t, "Dune", payload.Results[0].(map[string]interface{})["title"])

	// execute honors the disabled flag and the reserved-field gate, before
	// ever calling the builder
	deps.handler.disabled = runtime.NewDynamicValue(true)
	_, apiErr = deps.handler.execute(context.Background(), nil, "Movie", "",
		&models.SearchCommon{}, build)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

	deps.handler.disabled = runtime.NewDynamicValue(false)
	rerank := "title"
	_, apiErr = deps.handler.execute(context.Background(), nil, "Movie", "",
		&models.SearchCommon{RerankProperty: &rerank}, build)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "rerank_property")
}

func TestHandlerHappyPath(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []interface{}{
		map[string]interface{}{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"year":  float64(2021),
			"_additional": map[string]interface{}{
				"distance": float32(0.12),
			},
		},
	}

	payload, apiErr := doNearText(t, deps, nil, "Movie",
		`{"query":["space opera"],"limit":5,"return_properties":["title","year"],"return_metadata":["id","distance"]}`)
	require.Nil(t, apiErr)

	require.Len(t, payload.Results, 1)
	obj, ok := payload.Results[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Dune", obj["title"])
	assert.Equal(t, float64(2021), obj["year"])
	metadata := obj[metadataKey].(map[string]interface{})
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", metadata["id"])
	assert.Equal(t, float32(0.12), metadata["distance"])
	assert.GreaterOrEqual(t, payload.TookMs, int64(0))

	// the traverser was called with the parsed params
	params := deps.searcher.lastParams
	assert.Equal(t, "Movie", params.ClassName)
	assert.Equal(t, 5, params.Pagination.Limit)
	require.Contains(t, params.ModuleParams, "nearText")
}

func TestHandlerMetadataIDByDefault(t *testing.T) {
	deps := newTestHandler(t)
	deps.searcher.res = []interface{}{
		map[string]interface{}{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
		},
	}

	payload, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.Nil(t, apiErr)
	require.Len(t, payload.Results, 1)
	metadata := payload.Results[0].(map[string]interface{})[metadataKey].(map[string]interface{})
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", metadata["id"])
}

func TestHandlerDisabled(t *testing.T) {
	deps := newTestHandler(t)
	deps.handler.disabled = runtime.NewDynamicValue(true)

	_, apiErr := doNearText(t, deps, nil, "Movie", `{"query":["space"]}`)
	require.NotNil(t, apiErr)
	// mirrors DISABLE_GRAPHQL: the operation stays registered and rejects
	// requests with 422
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, apiErr.Error(), "disabled")
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

func TestHandlerNilBody(t *testing.T) {
	deps := newTestHandler(t)

	// swagger's required-body validation should catch this first; the
	// handler's defensive fallback returns 400
	_, apiErr := deps.handler.NearText(context.Background(), nil, "Movie", nil)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
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

func TestHandlerLowercasesCollection(t *testing.T) {
	deps := newTestHandler(t)

	_, apiErr := doNearText(t, deps, nil, "movie", `{"query":["space"]}`)
	require.Nil(t, apiErr)
	assert.Equal(t, "Movie", deps.searcher.lastParams.ClassName)
}

// TestHandlerTraverserErrorMapping builds each error the way its real
// producer does — typed error from entities/errors (or usecases/objects),
// wrapped in the pkg/errors chain the traverser applies. Using pkgerrors
// here is deliberate: it pins that pkg/errors wraps preserve Unwrap, which
// is what lets the typed matching in statusFromError see through the
// explorer's prefixes. If a producer stops attaching its typed error, or a
// wrap in the chain goes back to a chain-breaking %v, these cases fail.
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
			// smoke-covered live (embedding provider failure), wrapped as
			// explorer.getClassVectorSearch does
			name: "embedding provider failure",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(fmt.Errorf("remote client vectorize: connection refused")),
				"explorer: get class: vectorize params"),
			wantStatus: http.StatusBadGateway,
		},
		{
			// ORDERING GUARD + smoke-covered: the no-vectorizer config error
			// (usecases/modules) surfaces wrapped inside the vectorization
			// path's ErrQueryVectorization; the 422 config case must win
			// over the 502 provider-outage case.
			name: "no vectorizer configured",
			err: pkgerrors.Wrapf(
				enterrors.NewErrQueryVectorization(
					enterrors.NewErrNoVectorizerModule(fmt.Errorf("could not vectorize input for collection Movie with search-type nearText, targetVector  and parameters &{}. Make sure a vectorizer module is configured for this class"))),
				"explorer: get class: vectorize params"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			// typed rate-limit error via the real constructor (drift-guarded)
			name:       "rate limit (typed ErrRateLimit)",
			err:        enterrors.NewErrRateLimit(),
			wantStatus: http.StatusTooManyRequests,
		},
		{
			// tenant sentinel attached the way the multitenancy validator
			// does (objects.NewErrMultiTenancy around the sentinel), wrapped
			// the way the db/explorer chain does
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
			// MT validation failures without a tenant sentinel map 422 via
			// the typed objects.ErrMultiTenancy
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
			// ours, produced by classGetterWithAuthz via the sentinel
			name:       "collection not found (ours: sentinel)",
			err:        fmt.Errorf("%w %s in schema", errCollectionNotFound, "Movie"),
			wantStatus: http.StatusNotFound,
		},
		{
			// upstream "could not find class %s in schema" has many
			// producers and no sentinel yet — the one remaining string
			// fallback (reachable when a collection is deleted mid-request)
			name:       "class not found (upstream string fallback)",
			err:        fmt.Errorf("could not find class Movie in schema"),
			wantStatus: http.StatusNotFound,
		},
		{
			// real producer (configvalidation), wrapped as explorer.go does
			name:       "certainty on non-cosine",
			err:        fmt.Errorf("additional: %w for class: %v", certaintyErr, "Movie"),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			// where filter on a property whose inverted index is disabled
			// (adapters/repos/db/inverted searcher), wrapped as the explorer
			// vector-search path does
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
