//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
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

// AddClassProperty to an existing Class
func (m *Manager) AddClassProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property,
) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.addClassProperty(ctx, class, property)
}

func (m *Manager) addClassProperty(ctx context.Context,
	className string, prop *models.Property,
) error {
	m.Lock()
	defer m.Unlock()

	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop.Name = lowerCaseFirstLetter(prop.Name)

	m.setNewPropDefaults(class, prop)

	existingPropertyNames := map[string]bool{}
	for _, existingProperty := range class.Properties {
		existingPropertyNames[existingProperty.Name] = true
	}
	if err := m.validateProperty(prop, className, existingPropertyNames, false); err != nil {
		return err
	}

	tx, err := m.cluster.BeginTransaction(ctx, AddProperty,
		AddPropertyPayload{className, prop})
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.addClassPropertyApplyChanges(ctx, className, prop)
}

func (m *Manager) setNewPropDefaults(class *models.Class, prop *models.Property) {
	m.setPropertyDefaults(prop)
	m.moduleConfig.SetSinglePropertyDefaults(class, prop)
}

func (m *Manager) addClassPropertyApplyChanges(ctx context.Context,
	className string, prop *models.Property,
) error {
	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	class.Properties = append(class.Properties, prop)
	err = m.saveSchema(ctx)
	if err != nil {
		return nil
	}

	return m.migrator.AddProperty(ctx, className, prop)
}
