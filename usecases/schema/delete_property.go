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

	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
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
