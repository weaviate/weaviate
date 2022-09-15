package modcentroid

import (
	"context"
	"fmt"
	"net/http"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/vectorizer"
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
	obj *models.Object, cfg moduletools.ClassConfig, refVecs ...[]float32,
) error {
	if len(refVecs) == 0 {
		obj.Vector = nil
		return nil
	}

	vzr := m.vectorizer(cfg)

	vec, err := vzr.CalculateVector(refVecs...)
	if err != nil {
		return fmt.Errorf("calculate vector: %w", err)
	}

	obj.Vector = vec
	return nil
}

func (m *CentroidModule) TargetReferenceProperties(cfg moduletools.ClassConfig) (refProps []string) {
	props := cfg.Class()
	iRefProps := props[referencePropertiesField].([]interface{})
	for _, iProp := range iRefProps {
		refProps = append(refProps, iProp.(string))
	}
	return
}

func (m *CentroidModule) vectorizer(cfg moduletools.ClassConfig) *vectorizer.Vectorizer {
	props := cfg.Class()
	calcMethod := props[calculationMethodField].(vectorizer.CalculationMethod)
	return vectorizer.New(calcMethod)
}
