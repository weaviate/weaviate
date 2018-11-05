package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
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

func (s *Schema) GetProperty(kind kind.Kind, className ClassName, propName PropertyName) (error, *models.SemanticSchemaClassProperty) {
	semSchemaClass, err := GetClassByName(s.SemanticSchemaFor(kind), string(className))
	if err != nil {
		return err, nil
	}

	semProp, err := GetPropertyByName(semSchemaClass, string(propName))
	if err != nil {
		return err, nil
	}

	return nil, semProp
}
