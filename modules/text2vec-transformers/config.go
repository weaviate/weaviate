package modtransformers

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/vectorizer"
)

func (m *TransformersModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": vectorizer.DefaultVectorizeClassName,
		"poolingStrategy":    vectorizer.DefaultPoolingStrategy,
	}
}

func (m *TransformersModule) PropertyConfigDefaults(
	dt *schema.DataType) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !vectorizer.DefaultPropertyIndexed,
		"vectorizePropertyName": vectorizer.DefaultVectorizePropertyName,
	}
}

func (m *TransformersModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig) error {
	// icheck := vectorizer.NewIndexChecker(cfg)
	// return m.configValidator.Do(ctx, class, cfg, icheck)
	return nil // TODO: validate config
}

var _ = modulecapabilities.ClassConfigurator(New())
