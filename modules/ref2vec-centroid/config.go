package modcentroid

import (
	"context"
	"errors"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/vectorizer"
	"github.com/sirupsen/logrus"
)

const (
	calculationMethodField   = "method"
	referencePropertiesField = "referenceProperties"
)

var errInvalidConfig = errors.New("invalid config")

func (m *CentroidModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		calculationMethodField: vectorizer.MethodDefault,
	}
}

func (m *CentroidModule) PropertyConfigDefaults(dataType *schema.DataType) map[string]interface{} {
	// no property-specific config for this module
	return nil
}

func (m *CentroidModule) ValidateClass(ctx context.Context,
	class *models.Class, classConfig moduletools.ClassConfig,
) error {
	return newConfigValidator(m.logger).do(ctx, class, classConfig)
}

var _ = modulecapabilities.ClassConfigurator(New())

type configValidator struct {
	logger logrus.FieldLogger
}

func newConfigValidator(logger logrus.FieldLogger) *configValidator {
	return &configValidator{logger: logger}
}

func (v *configValidator) do(ctx context.Context, class *models.Class,
	classConfig moduletools.ClassConfig,
) error {
	// referencePropertiesField is a required field
	cfg := classConfig.Class()
	refProps, ok := cfg[referencePropertiesField]
	if !ok {
		return fmt.Errorf("%w: must have at least one value in the %q field for class %q",
			errInvalidConfig, referencePropertiesField, class.Class)
	}

	propSlice, ok := refProps.([]interface{})
	if !ok {
		return fmt.Errorf("%w: expected array for field %q, got %T for class %q",
			errInvalidConfig, referencePropertiesField, refProps, class.Class)
	}

	if len(propSlice) == 0 {
		return fmt.Errorf("%w: must have at least one value in the %q field for class %q",
			errInvalidConfig, referencePropertiesField, class.Class)
	}

	// all provided property names must be strings
	for _, prop := range propSlice {
		if _, ok := prop.(string); !ok {
			return fmt.Errorf("%w: expected %q to contain strings, found %T: %+v for class %q",
				errInvalidConfig, referencePropertiesField, prop, refProps, class.Class)
		}
	}

	return nil
}
