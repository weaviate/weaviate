package modcontextionary

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/vectorizer"
)

func (m *ContextionaryModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": vectorizer.DefaultVectorizeClassName,
	}
}

func (m *ContextionaryModule) PropertyConfigDefaults(
	dt *schema.DataType) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !vectorizer.DefaultPropertyIndexed,
		"vectorizePropertyName": vectorizer.DefaultVectorizePropertyName,
	}
}

func (m *ContextionaryModule) ValidateClass(class *models.Class,
	cfg modulecapabilities.ClassConfig) error {
	icheck := vectorizer.NewIndexChecker(cfg)
	return m.configValidator.Do(class, cfg, icheck)
}

var _ = modulecapabilities.ClassConfigurator(New())
