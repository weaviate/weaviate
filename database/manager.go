package database

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
)

type SchemaManager interface {
	AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error
	DropClass(kind kind.Kind, className string) error

	AddProperty(kind kind.Kind, className string, prop models.SemanticSchemaClassProperty) error
	UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaClassKeywords) error
	DropProperty(kind kind.Kind, className string, propName string) error
}
