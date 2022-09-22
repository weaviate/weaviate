package modules

import (
	"context"
	"net/http"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/mock"
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
	in *models.Object, cfg moduletools.ClassConfig,
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
	findRefVecsFn modulecapabilities.FindRefVectorsFn,
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

type fakeRefVecRepo struct {
	mock.Mock
}

func (r *fakeRefVecRepo) ReferenceVectorSearch(ctx context.Context, obj *models.Object,
	refProps map[string]struct{},
) ([][]float32, error) {
	args := r.Called(ctx, obj, refProps)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([][]float32), args.Error(1)
}
