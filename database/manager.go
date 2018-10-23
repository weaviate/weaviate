package database

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
	"github.com/go-openapi/strfmt"
)

type SchemaManager interface {
	schema_migrator.Migrator

	// Update the Thing or Action schema's meta data.
	UpdateMeta(kind kind.Kind, atContext strfmt.URI, maintainer strfmt.Email, name string) error

	// Return a reference to the database schema.
	// Note that this function can be both called from having a ConnectorLock as a SchemaLock.
	GetSchema() Schema
}
