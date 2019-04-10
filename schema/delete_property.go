package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
)

// DeleteActionProperty to an existing Action
func (m *Manager) DeleteActionProperty(ctx context.Context, class string, property string) error {
	return m.deleteClassProperty(ctx, class, property, kind.ACTION_KIND)
}

// DeleteThingProperty to an existing Thing
func (m *Manager) DeleteThingProperty(ctx context.Context, class string, property string) error {
	return m.deleteClassProperty(ctx, class, property, kind.THING_KIND)
}

func (m *Manager) deleteClassProperty(ctx context.Context, class string, property string, k kind.Kind) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return err
	}
	defer unlock(schemaLock)

	schemaManager := schemaLock.SchemaManager()
	err = schemaManager.DropProperty(ctx, k, class, property)
	if err != nil {
		return err
	}

	return nil
}
