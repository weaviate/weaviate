package modkserve

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/vectorizer"
)

func (m *KServeModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": vectorizer.DefaultVectorizeClassName,
	}
}

func (m *KServeModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !vectorizer.DefaultPropertyIndexed,
		"vectorizePropertyName": vectorizer.DefaultVectorizePropertyName,
	}
}

func (m *KServeModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	settings := vectorizer.NewClassSettings(cfg)

	err := settings.Validate(ctx, class)
	if err != nil {
		return err
	}

	validator, err := m.validatorFactory.ToValidator(settings.Protocol())
	if err != nil {
		return nil // Protocol doesnt support validation, therefore no validation required
	}
	return validator.Validate(ctx, settings.ToModuleConfig())
}

var _ = modulecapabilities.ClassConfigurator(New())
