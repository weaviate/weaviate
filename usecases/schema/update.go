/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// UpdateAction which exists
func (m *Manager) UpdateAction(ctx context.Context, name string,
	class *models.SemanticSchemaClass) error {
	return m.updateClass(ctx, name, class, kind.Action)
}

// UpdateThing which exists
func (m *Manager) UpdateThing(ctx context.Context, name string,
	class *models.SemanticSchemaClass) error {
	return m.updateClass(ctx, name, class, kind.Thing)
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
