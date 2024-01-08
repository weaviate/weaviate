//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"context"
	"net/http"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func newDummyModule(name string, t modulecapabilities.ModuleType) modulecapabilities.Module {
	switch t {
	case modulecapabilities.Text2Vec:
		return newDummyText2VecModule(name)
	case modulecapabilities.Ref2Vec:
		return newDummyRef2VecModule(name)
	default:
		return newDummyNonVectorizerModule(name)
	}
}

func newDummyText2VecModule(name string) dummyText2VecModuleNoCapabilities {
	return dummyText2VecModuleNoCapabilities{name: name}
}

type dummyText2VecModuleNoCapabilities struct {
	name string
}

func (m dummyText2VecModuleNoCapabilities) Name() string {
	return m.name
}

func (m dummyText2VecModuleNoCapabilities) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	return nil
}

// TODO remove as this is a capability
func (m dummyText2VecModuleNoCapabilities) RootHandler() http.Handler {
	return nil
}

func (m dummyText2VecModuleNoCapabilities) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m dummyText2VecModuleNoCapabilities) VectorizeObject(ctx context.Context,
	in *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	in.Vector = []float32{1, 2, 3}
	return nil
}

func newDummyRef2VecModule(name string) dummyRef2VecModuleNoCapabilities {
	return dummyRef2VecModuleNoCapabilities{name: name}
}

type dummyRef2VecModuleNoCapabilities struct {
	name string
}

func (m dummyRef2VecModuleNoCapabilities) Name() string {
	return m.name
}

func (m dummyRef2VecModuleNoCapabilities) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	return nil
}

// TODO remove as this is a capability
func (m dummyRef2VecModuleNoCapabilities) RootHandler() http.Handler {
	return nil
}

func (m dummyRef2VecModuleNoCapabilities) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Ref2Vec
}

func (m dummyRef2VecModuleNoCapabilities) VectorizeObject(ctx context.Context,
	in *models.Object, cfg moduletools.ClassConfig,
	findRefVecsFn modulecapabilities.FindObjectFn,
) error {
	in.Vector = []float32{1, 2, 3}
	return nil
}

func newDummyNonVectorizerModule(name string) dummyNonVectorizerModule {
	return dummyNonVectorizerModule{name: name}
}

type dummyNonVectorizerModule struct {
	name string
}

func (m dummyNonVectorizerModule) Name() string {
	return m.name
}

func (m dummyNonVectorizerModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	return nil
}

// TODO remove as this is a capability
func (m dummyNonVectorizerModule) RootHandler() http.Handler {
	return nil
}

func (m dummyNonVectorizerModule) Type() modulecapabilities.ModuleType {
	var non modulecapabilities.ModuleType = "NonVectorizer"
	return non
}

type fakeSchemaGetter struct{ schema schema.Schema }

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

type fakeObjectsRepo struct {
	mock.Mock
}

func (r *fakeObjectsRepo) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties, tenant string,
) (*search.Result, error) {
	args := r.Called(ctx, class, id, props, addl)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*search.Result), args.Error(1)
}
