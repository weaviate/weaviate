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
		return newDummyText2VecModule(name, nil)
	case modulecapabilities.Text2Multivec:
		return newDummyText2ColBERTModule(name, nil)
	case modulecapabilities.Ref2Vec:
		return newDummyRef2VecModule(name)
	default:
		return newDummyNonVectorizerModule(name)
	}
}

func newDummyText2VecModule(name string, mediaProperties []string) dummyText2VecModuleNoCapabilities {
	return dummyText2VecModuleNoCapabilities{name: name, mediaProperties: mediaProperties}
}

type dummyText2VecModuleNoCapabilities struct {
	name            string
	mediaProperties []string
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
	in *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return []float32{1, 2, 3}, nil, nil
}

func (m dummyText2VecModuleNoCapabilities) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, m.mediaProperties, nil
}

func (m dummyText2VecModuleNoCapabilities) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	errs := make(map[int]error, 0)
	vecs := make([][]float32, len(objs))
	for i := range vecs {
		vecs[i] = []float32{1, 2, 3}
	}
	return vecs, nil, errs
}

func newDummyText2ColBERTModule(name string, mediaProperties []string) dummyText2ColBERTModuleNoCapabilities {
	return dummyText2ColBERTModuleNoCapabilities{name: name, mediaProperties: mediaProperties}
}

type dummyText2ColBERTModuleNoCapabilities struct {
	name            string
	mediaProperties []string
}

func (m dummyText2ColBERTModuleNoCapabilities) Name() string {
	return m.name
}

func (m dummyText2ColBERTModuleNoCapabilities) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	return nil
}

// TODO remove as this is a capability
func (m dummyText2ColBERTModuleNoCapabilities) RootHandler() http.Handler {
	return nil
}

func (m dummyText2ColBERTModuleNoCapabilities) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Multivec
}

func (m dummyText2ColBERTModuleNoCapabilities) VectorizeObject(ctx context.Context,
	in *models.Object, cfg moduletools.ClassConfig,
) ([][]float32, models.AdditionalProperties, error) {
	return [][]float32{{0.11, 0.22, 0.33}, {0.11, 0.22, 0.33}}, nil, nil
}

func (m dummyText2ColBERTModuleNoCapabilities) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, m.mediaProperties, nil
}

func (m dummyText2ColBERTModuleNoCapabilities) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][][]float32, []models.AdditionalProperties, map[int]error) {
	errs := make(map[int]error, 0)
	vecs := make([][][]float32, len(objs))
	for i := range vecs {
		vecs[i] = [][]float32{{0.1, 0.2, 0.3}, {0.1, 0.2, 0.3}}
	}
	return vecs, nil, errs
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
) ([]float32, error) {
	return []float32{1, 2, 3}, nil
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

func (f *fakeSchemaGetter) ReadOnlyClass(name string) *models.Class {
	return f.schema.GetClass(name)
}

type fakeObjectsRepo struct {
	mock.Mock
}

func (r *fakeObjectsRepo) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties, tenant string,
) (*search.Result, error) {
	return nil, nil
}
