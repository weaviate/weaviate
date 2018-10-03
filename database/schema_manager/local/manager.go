package local

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
)

func (l *LocalSchemaManager) AddClass(kind kind.Kind, class models.SemanticSchemaClass) error {
	return nil
}

func (l *LocalSchemaManager) DropClass(kind kind.Kind, className string) error {
	return nil
}

func (l *LocalSchemaManager) AddProperty(kind kind.Kind, className string, prop models.SemanticSchemaClassProperty) error {
	return nil
}

func (l *LocalSchemaManager) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaClassKeywords) error {
	return nil
}

func (l *LocalSchemaManager) DropProperty(kind kind.Kind, className string, propName string) error {
	return nil
}
