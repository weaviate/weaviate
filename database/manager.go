package database

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

type SchemaManager interface {
	AddClass(kind schema.Kind, class models.SemanticSchemaClass) error
	DropClass(kind schema.Kind, className string) error

	AddProperty(kind schema.Kind, className string, prop models.SemanticSchemaClassProperty) error
	UpdateProperty(kind schema.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaClassKeywords) error
	DropProperty(kind schema.Kind, className string, propName string) error
}
