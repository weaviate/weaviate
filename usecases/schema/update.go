//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (m *Manager) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	initial := m.getClassByName(className)
	if initial == nil {
		panic("TODO")
	}

	if err := m.validateClassNameUpdate(initial, updated); err != nil {
		return err
	}

	return nil
}

func (m *Manager) validateClassNameUpdate(initial, updated *models.Class) error {
	if initial.Class != updated.Class {
		return errors.Errorf("class name is immutable: attempted change from %q to %q",
			initial.Class, updated.Class)
	}

	return nil
}

// Below here is old - to be deleted

// UpdateObject which exists
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	name string, class *models.Class) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.updateClass(ctx, name, class)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClass(ctx context.Context, className string,
	class *models.Class) error {
	m.Lock()
	defer m.Unlock()

	var newName *string

	if class.Class != className {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := upperCaseClassName(class.Class)
		newName = &n
	}

	semanticSchema := m.state.SchemaFor()

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
	if err = m.validateClassName(ctx, classNameAfterUpdate); err != nil {
		return err
	}

	// Validated! Now apply the changes.
	class.Class = classNameAfterUpdate

	err = m.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return m.migrator.UpdateClass(ctx, className, newName)
}
