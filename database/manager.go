package database

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"

	"github.com/go-openapi/strfmt"
)

type Migrator interface {
	// Both a Connector and a SchemaManager are expected to implement this.

	// Update the Thing or Action schema's meta data.
	UpdateMeta(kind kind.Kind, atContext strfmt.URI, maintainer strfmt.Email, name string) error

	// Add a class to the Thing or Action schema, depending on the kind parameter.
	AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error

	// Drop a class from the schema.
	DropClass(kind kind.Kind, className string) error

	UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error

	AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error
	UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error
	DropProperty(kind kind.Kind, className string, propName string) error
}

type SchemaManager interface {
	Migrator

	// Return a reference to the database schema.
	// Note that this function can be both called from having a ConnectorLock as a SchemaLock.
	GetSchema() Schema
}
