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

package kinds

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds/validation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// AddActionReference Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddActionReference(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.addActionReferenceToConnectorAndSchema(ctx, principal, id, propertyName, property)
}

func (m *Manager) addActionReferenceToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {

	// get action to see if it exists
	actionRes, err := m.getActionFromRepo(ctx, id, traverser.UnderscoreProperties{})
	if err != nil {
		return err
	}

	action := actionRes.Action()

	err = m.validateReference(ctx, property)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, kind.Action, action.Class, propertyName)
	if err != nil {
		return err
	}

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForAction(ctx, principal, action)
	if err != nil {
		return NewErrInternal("could not update schema for network refs: %v", err)
	}

	err = m.vectorRepo.AddReference(ctx, kind.Action, action.ID, propertyName, property)
	if err != nil {
		return NewErrInternal("add reference to vector repo: %v", err)
	}

	return nil
}

// AddThingReference Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddThingReference(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.addThingReferenceToConnectorAndSchema(ctx, principal, id, propertyName, property)
}

func (m *Manager) addThingReferenceToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {

	// get thing to see if it exists
	thingRes, err := m.getThingFromRepo(ctx, id, traverser.UnderscoreProperties{})
	if err != nil {
		return err
	}

	thing := thingRes.Thing()
	err = m.validateReference(ctx, property)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, kind.Thing, thing.Class, propertyName)
	if err != nil {
		return err
	}

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForThing(ctx, principal, thing)
	if err != nil {
		return NewErrInternal("could not update schema for network refs: %v", err)
	}

	err = m.vectorRepo.AddReference(ctx, kind.Thing, thing.ID, propertyName, property)
	if err != nil {
		return NewErrInternal("add reference to vector repo: %v", err)
	}

	return nil
}

func (m *Manager) validateReference(ctx context.Context, reference *models.SingleRef) error {
	err := validation.New(schema.Schema{}, m.exists, m.network, m.config).
		ValidateSingleRef(ctx, reference, "reference not found")
	if err != nil {
		return NewErrInvalidUserInput("invalid reference: %v", err)
	}

	return nil
}

func (m *Manager) validateCanModifyReference(principal *models.Principal, k kind.Kind,
	className string, propertyName string) error {
	class, err := schema.ValidateClassName(className)
	if err != nil {
		return NewErrInvalidUserInput("invalid class name in reference: %v", err)
	}

	propName, err := schema.ValidatePropertyName(propertyName)
	if err != nil {
		return NewErrInvalidUserInput("invalid property name in reference: %v", err)
	}

	schema, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return err
	}

	prop, err := schema.GetProperty(k, class, propName)
	if err != nil {
		return NewErrInvalidUserInput("Could not find property '%s': %v", propertyName, err)
	}

	propertyDataType, err := schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return NewErrInternal("Could not find datatype of property '%s': %v", propertyName, err)
	}

	if propertyDataType.IsPrimitive() {
		return NewErrInvalidUserInput("property '%s' is a primitive datatype, not a reference-type", propertyName)
	}

	if prop.Cardinality == nil || *prop.Cardinality != "many" {
		return NewErrInvalidUserInput("Property '%s' has a cardinality of atMostOne", propertyName)
	}

	return nil
}
