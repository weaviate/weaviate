package modcentroid

import (
	"context"
	"net/http"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus"
)

const (
	Name = "ref2vec-centroid"
)

func New() *CentroidModule {
	return &CentroidModule{}
}

type CentroidModule struct {
	logger logrus.FieldLogger
}

func (m *CentroidModule) Name() string {
	return Name
}

func (m *CentroidModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()

	return nil
}

func (m *CentroidModule) RootHandler() http.Handler {
	// TODO: remove from overall module, this is a capability
	return nil
}

func (m *CentroidModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Ref2Vec
}

func (m *CentroidModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) error {
	return nil
}
