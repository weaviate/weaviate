package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// AddAction Class to the schema
func (m *Manager) AddAction(ctx context.Context, class *models.SemanticSchemaClass) error {
	return m.addClass(ctx, class, kind.ACTION_KIND)
}

// AddThing Class to the schema
func (m *Manager) AddThing(ctx context.Context, class *models.SemanticSchemaClass) error {
	return m.addClass(ctx, class, kind.THING_KIND)
}

func (m *Manager) addClass(ctx context.Context, class *models.SemanticSchemaClass, k kind.Kind) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return err
	}
	defer unlock(schemaLock)

	schemaManager := schemaLock.SchemaManager()
	err = schemaManager.AddClass(ctx, k, class)
	if err != nil {
		return err
	}

	return nil
}
