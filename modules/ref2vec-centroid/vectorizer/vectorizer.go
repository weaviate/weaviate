package vectorizer

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/config"
)

type calcFunc func(vecs ...[]float32) ([]float32, error)

type Vectorizer struct {
	config          *config.Config
	calcFunc        calcFunc
	findRefVecsFunc modulecapabilities.FindRefVectorsFn
}

func New(cfg moduletools.ClassConfig, findFn modulecapabilities.FindRefVectorsFn) *Vectorizer {
	v := &Vectorizer{
		config:          config.New(cfg),
		findRefVecsFunc: findFn,
	}

	switch v.config.CalculationMethod() {
	case config.MethodMean:
		v.calcFunc = calculateMean
	default:
		v.calcFunc = calculateMean
	}

	return v
}

func (v *Vectorizer) Object(ctx context.Context, obj *models.Object) error {
	props := v.config.ReferenceProperties()

	refVecs, err := v.findRefVecsFunc(ctx, obj, props)
	if err != nil {
		return fmt.Errorf("find ref vectors: %w", err)
	}

	if len(refVecs) == 0 {
		obj.Vector = nil
		return nil
	}

	vec, err := v.calcFunc(refVecs...)
	if err != nil {
		return fmt.Errorf("calculate vector: %w", err)
	}

	obj.Vector = vec
	return nil
}
