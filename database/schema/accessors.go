package schema

import (
	"github.com/creativesoftwarefdn/weaviate/models"
)

// Find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.SemanticSchemaClass {
	semSchemaClass, err := GetClassByName(s.Things, string(className))
	if err == nil {
		return semSchemaClass
	}

	semSchemaClass, err = GetClassByName(s.Actions, string(className))
	if err == nil {
		return semSchemaClass
	}

	return nil
}
