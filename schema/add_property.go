package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// AddActionProperty to an existing Action
func (m *Manager) AddActionProperty(ctx context.Context, class string, property *models.SemanticSchemaClassProperty) error {
	return m.addClassProperty(ctx, class, property, kind.ACTION_KIND)
}

// AddThingProperty to an existing Thing
func (m *Manager) AddThingProperty(ctx context.Context, class string, property *models.SemanticSchemaClassProperty) error {
	return m.addClassProperty(ctx, class, property, kind.THING_KIND)
}

func (m *Manager) addClassProperty(ctx context.Context, class string,
	property *models.SemanticSchemaClassProperty, k kind.Kind) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return err
	}
	defer unlock(schemaLock)

	schemaManager := schemaLock.SchemaManager()
	err = schemaManager.AddProperty(ctx, k, class, property)
	if err != nil {
		return err
	}

	return nil
}
