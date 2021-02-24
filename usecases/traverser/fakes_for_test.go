//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"net/http"

	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	modcontextionarygraphql "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/graphql"
	libprojector "github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/stretchr/testify/mock"
)

type fakeLocks struct{}

func (f *fakeLocks) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (f *fakeLocks) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

type fakeVectorizer struct{}

func (f *fakeVectorizer) Object(ctx context.Context, object *models.Object) ([]float32, error) {
	panic("not implemented")
}

func (f *fakeVectorizer) Corpi(ctx context.Context, corpi []string) ([]float32, error) {
	return []float32{1, 2, 3}, nil
}

func (f *fakeVectorizer) MoveTo(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v + 1
	}
	return res, nil
}

func (f *fakeVectorizer) MoveAwayFrom(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v - 0.5
	}
	return res, nil
}

func (f *fakeVectorizer) NormalizedDistance(source, target []float32) (float32, error) {
	return 0.5, nil
}

type fakeTxt2VecVectorizer struct{}

func (f *fakeTxt2VecVectorizer) Object(ctx context.Context, object *models.Object, icheck vectorizer.ClassIndexCheck) error {
	panic("not implemented")
}

func (f *fakeTxt2VecVectorizer) Corpi(ctx context.Context, corpi []string) ([]float32, error) {
	return []float32{1, 2, 3}, nil
}

func (f *fakeTxt2VecVectorizer) MoveTo(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v + 1
	}
	return res, nil
}

func (f *fakeTxt2VecVectorizer) MoveAwayFrom(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v - 0.5
	}
	return res, nil
}

type fakeVectorSearcher struct {
	mock.Mock
	calledWithVector []float32
	calledWithLimit  int
	results          []search.Result
}

func (f *fakeVectorSearcher) VectorSearch(ctx context.Context,
	vector []float32, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	f.calledWithVector = vector
	f.calledWithLimit = limit
	return f.results, nil
}

func (f *fakeVectorSearcher) Aggregate(ctx context.Context,
	params AggregateParams) (*aggregation.Result, error) {
	args := f.Called(params)
	return args.Get(0).(*aggregation.Result), args.Error(1)
}

func (f *fakeVectorSearcher) VectorClassSearch(ctx context.Context,
	params GetParams) ([]search.Result, error) {
	args := f.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

func (f *fakeVectorSearcher) ClassSearch(ctx context.Context,
	params GetParams) ([]search.Result, error) {
	args := f.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

func (f *fakeVectorSearcher) ObjectByID(ctx context.Context, id strfmt.UUID,
	props SelectProperties, additional AdditionalProperties) (*search.Result, error) {
	args := f.Called(id)
	return args.Get(0).(*search.Result), args.Error(1)
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

type fakeVectorRepo struct {
	mock.Mock
}

func (f *fakeVectorRepo) PutObject(ctx context.Context, index string,
	concept *models.Object, vector []float32) error {
	return nil
}

func (f *fakeVectorRepo) VectorSearch(ctx context.Context,
	vector []float32, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	return nil, nil
}

func (f *fakeVectorRepo) Aggregate(ctx context.Context,
	params AggregateParams) (*aggregation.Result, error) {
	args := f.Called(params)
	return args.Get(0).(*aggregation.Result), args.Error(1)
}

func (f *fakeVectorRepo) GetObject(ctx context.Context, uuid strfmt.UUID,
	res *models.Object) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Object)
	return args.Error(1)
}

type fakeExplorer struct{}

func (f *fakeExplorer) GetClass(ctx context.Context, p GetParams) ([]interface{}, error) {
	return nil, nil
}

func (f *fakeExplorer) Concepts(ctx context.Context, p ExploreParams) ([]search.Result, error) {
	return nil, nil
}

type fakeSchemaGetter struct {
	schema schema.Schema
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

type fakeExtender struct {
	returnArgs []search.Result
}

func (f *fakeExtender) Multi(ctx context.Context, in []search.Result, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

type fakeProjector struct {
	returnArgs []search.Result
}

func (f *fakeProjector) Reduce(in []search.Result, params *libprojector.Params) ([]search.Result, error) {
	return f.returnArgs, nil
}

type fakePathBuilder struct {
	returnArgs []search.Result
}

func (f *fakePathBuilder) CalculatePath(in []search.Result, params *sempath.Params) ([]search.Result, error) {
	return f.returnArgs, nil
}

type fakeText2vecContextionaryModule struct{}

func (m *fakeText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *fakeText2vecContextionaryModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *fakeText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

func (m *fakeText2vecContextionaryModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return modcontextionarygraphql.New().GetArguments(classname)
}

func (m *fakeText2vecContextionaryModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return modcontextionarygraphql.New().ExploreArguments()
}

func (m *fakeText2vecContextionaryModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return modcontextionarygraphql.New().ExtractFunctions()
}

func (m *fakeText2vecContextionaryModule) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return modcontextionarygraphql.New().ValidateFunctions()
}

func (m *fakeText2vecContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return modcontextionarygraphql.NewSearcher(&fakeTxt2VecVectorizer{}).VectorSearches()
}
