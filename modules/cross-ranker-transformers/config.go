package modcrossrankertransformers

import (
	"context"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func (m *CrossRankerModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{}
}

func (m *CrossRankerModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *CrossRankerModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	return nil
}

var _ = modulecapabilities.ClassConfigurator(New())
