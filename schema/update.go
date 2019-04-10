package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// UpdateAction which exists
func (m *Manager) UpdateAction(ctx context.Context, name string,
	class *models.SemanticSchemaClass) error {
	return m.updateClass(ctx, name, class, kind.ACTION_KIND)
}

// UpdateThing which exists
func (m *Manager) UpdateThing(ctx context.Context, name string,
	class *models.SemanticSchemaClass) error {
	return m.updateClass(ctx, name, class, kind.THING_KIND)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClass(ctx context.Context, name string,
	class *models.SemanticSchemaClass, k kind.Kind) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return err
	}
	defer unlock(schemaLock)

	schemaManager := schemaLock.SchemaManager()
	var newName *string
	var newKeywords *models.SemanticSchemaKeywords

	if class.Class != name {
		// the name in the URI and body don't match, so we assume the user wants to rename
		newName = &class.Class
	}

	// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
	if len(class.Keywords) > 0 {
		newKeywords = &class.Keywords
	}

	err = schemaManager.UpdateClass(ctx, k, name, newName, newKeywords)
	if err != nil {
		return err
	}

	return nil
}
