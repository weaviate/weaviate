package modcentroid

import (
	"context"
	"fmt"
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
	obj *models.Object, cfg moduletools.ClassConfig, refVecs ...[]float32,
) error {
	if len(refVecs) == 0 {
		return nil
	}

	// TODO: abstract out mean calc for pluggable calculation methods

	targetVecLen := len(refVecs[0])
	meanVec := make([]float32, targetVecLen)

	// TODO: find the efficient way of doing this
	for _, vec := range refVecs {
		if len(vec) != targetVecLen {
			return fmt.Errorf("vectorize object: found vectors of different length: %d and %d",
				targetVecLen, len(vec))
		}

		for i, val := range vec {
			meanVec[i] += val
		}
	}

	for i := range meanVec {
		meanVec[i] /= float32(len(refVecs))
	}

	obj.Vector = meanVec

	return nil
}

func (m *CentroidModule) TargetReferenceProperties(allProps map[string]interface{}) (refProps []string) {
	iRefProps := allProps[referencePropertiesField].([]interface{})
	for _, iProp := range iRefProps {
		refProps = append(refProps, iProp.(string))
	}
	return
}
