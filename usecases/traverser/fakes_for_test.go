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

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	libprojector "github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/sempath"
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

func (f *fakeVectorizer) Thing(ctx context.Context, thing *models.Thing) ([]float32, error) {
	panic("not implemented")
}

func (f *fakeVectorizer) Action(ctx context.Context, thing *models.Action) ([]float32, error) {
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

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

type fakeVectorRepo struct {
	mock.Mock
}

func (f *fakeVectorRepo) PutThing(ctx context.Context, index string,
	concept *models.Thing, vector []float32) error {
	return nil
}

func (f *fakeVectorRepo) PutAction(ctx context.Context, index string,
	concept *models.Action, vector []float32) error {
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

func (f *fakeVectorRepo) GetThing(ctx context.Context, uuid strfmt.UUID,
	res *models.Thing) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Thing)
	return args.Error(1)
}

func (f *fakeVectorRepo) GetAction(ctx context.Context, uuid strfmt.UUID,
	res *models.Action) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Action)
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
