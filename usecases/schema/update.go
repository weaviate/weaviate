//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// UpdateObject which exists
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	name string, class *models.Class) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.updateClass(ctx, name, class, kind.Object)
}

// UpdateThing which exists
// func (m *Manager) UpdateThing(ctx context.Context, principal *models.Principal,
// 	name string, class *models.Class) error {
// 	err := m.authorizer.Authorize(principal, "update", "schema/things")
// 	if err != nil {
// 		return err
// 	}

// 	return m.updateClass(ctx, name, class, kind.Thing)
// }

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClass(ctx context.Context, className string,
	class *models.Class, k kind.Kind) error {
	m.Lock()
	defer m.Unlock()

	var newName *string

	if class.Class != className {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := upperCaseClassName(class.Class)
		newName = &n
	}

	semanticSchema := m.state.SchemaFor(k)

	var err error
	class, err = schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	classNameAfterUpdate := className

	// First validate the request
	if newName != nil {
		err = m.validateClassNameUniqueness(*newName)
		classNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	// Validate name / keywords in contextionary
	if err = m.validateClassName(ctx, k, classNameAfterUpdate,
		VectorizeClassName(class)); err != nil {
		return err
	}

	// Validated! Now apply the changes.
	class.Class = classNameAfterUpdate

	err = m.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return m.migrator.UpdateClass(ctx, k, className, newName)
}
