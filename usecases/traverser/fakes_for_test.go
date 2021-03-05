//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"net/http"

	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	modcontextionaryadditional "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional"
	modcontextionaryadditionalprojector "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/projector"
	modcontextionaryadditionalsempath "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/sempath"
	modcontextionaryneartext "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/neartext"
	modcontextionaryvectorizer "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/vectorizer"
	"github.com/stretchr/testify/mock"
)

type fakeLocks struct{}

func (f *fakeLocks) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (f *fakeLocks) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

type fakeTxt2VecVectorizer struct{}

func (f *fakeTxt2VecVectorizer) Object(ctx context.Context, object *models.Object, icheck modcontextionaryvectorizer.ClassIndexCheck) error {
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

func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
}

func (f *fakeExtender) AdditonalPropertyDefaultValue() interface{} {
	return true
}

type fakeProjector struct {
	returnArgs []search.Result
}

func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
}

func (f *fakeProjector) AdditonalPropertyDefaultValue() interface{} {
	return &modcontextionaryadditionalprojector.Params{}
}

type fakePathBuilder struct {
	returnArgs []search.Result
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
}

func (f *fakePathBuilder) AdditonalPropertyDefaultValue() interface{} {
	return &modcontextionaryadditionalsempath.Params{}
}

type fakeText2vecContextionaryModule struct {
	customExtender    *fakeExtender
	customProjector   *fakeProjector
	customPathBuilder *fakePathBuilder
}

func newFakeText2vecContextionaryModuleWithCustomExtender(
	customExtender *fakeExtender,
	customProjector *fakeProjector,
	customPathBuilder *fakePathBuilder,
) *fakeText2vecContextionaryModule {
	return &fakeText2vecContextionaryModule{customExtender, customProjector, customPathBuilder}
}

func (m *fakeText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *fakeText2vecContextionaryModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *fakeText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

func (m *fakeText2vecContextionaryModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return modcontextionaryneartext.New().Arguments()
}

func (m *fakeText2vecContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return modcontextionaryneartext.NewSearcher(&fakeTxt2VecVectorizer{}).VectorSearches()
}

func (m *fakeText2vecContextionaryModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return modcontextionaryadditional.New(m.getExtender(), m.getProjector(), m.getPathBuilder()).AdditionalProperties()
}

func (m *fakeText2vecContextionaryModule) getExtender() *fakeExtender {
	if m.customExtender != nil {
		return m.customExtender
	}
	return &fakeExtender{}
}

func (m *fakeText2vecContextionaryModule) getProjector() *fakeProjector {
	if m.customProjector != nil {
		return m.customProjector
	}
	return &fakeProjector{}
}

func (m *fakeText2vecContextionaryModule) getPathBuilder() *fakePathBuilder {
	if m.customPathBuilder != nil {
		return m.customPathBuilder
	}
	return &fakePathBuilder{}
}
