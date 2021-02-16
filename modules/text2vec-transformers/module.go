package modtransformers

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/usecases/modules"
)

func New() *TransformersModule {
	return &TransformersModule{}
}

type TransformersModule struct{}

func (m *TransformersModule) Name() string {
	return "text2vec-transformers"
}

func (m *TransformersModule) Init(params modules.ModuleInitParams) error {
	return nil
}

func (m *TransformersModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *TransformersModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig) error {
	return errors.Errorf("not implemented")
}

// verify we implement the modules.Module interface
var (
	_ = modules.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
