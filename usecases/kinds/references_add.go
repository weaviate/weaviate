/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds/validation"
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
	action, err := m.getActionFromRepo(ctx, id)
	if err != nil {
		return err
	}

	err = m.validateReference(ctx, property)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, kind.Action, action.Class, propertyName)
	if err != nil {
		return err
	}

	extended, err := m.extendClassPropsWithReference(action.Schema, propertyName, property)
	if err != nil {
		return err
	}
	action.Schema = extended
	action.LastUpdateTimeUnix = unixNow()

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForAction(ctx, principal, action)
	if err != nil {
		return NewErrInternal("could not update schema for network refs: %v", err)
	}

	err = m.repo.UpdateAction(ctx, action, action.ID)
	if err != nil {
		return NewErrInternal("could not store action: %v", err)
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
	thing, err := m.getThingFromRepo(ctx, id)
	if err != nil {
		return err
	}

	err = m.validateReference(ctx, property)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, kind.Thing, thing.Class, propertyName)
	if err != nil {
		return err
	}

	extended, err := m.extendClassPropsWithReference(thing.Schema, propertyName, property)
	if err != nil {
		return err
	}
	thing.Schema = extended
	thing.LastUpdateTimeUnix = unixNow()

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForThing(ctx, principal, thing)
	if err != nil {
		return NewErrInternal("could not update schema for network refs: %v", err)
	}

	err = m.repo.UpdateThing(ctx, thing, thing.ID)
	if err != nil {
		return NewErrInternal("could not store thing: %v", err)
	}

	return nil
}

func (m *Manager) validateReference(ctx context.Context, reference *models.SingleRef) error {
	err := validation.ValidateSingleRef(ctx, m.config, reference, m.repo, m.network, "reference not found")
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

	err, prop := schema.GetProperty(k, class, propName)
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

func (m *Manager) extendClassPropsWithReference(props interface{}, propertyName string,
	property *models.SingleRef) (interface{}, error) {

	if props == nil {
		props = map[string]interface{}{}
	}

	propsMap := props.(map[string]interface{})

	_, ok := propsMap[propertyName]
	if !ok {
		propsMap[propertyName] = []interface{}{}
	}

	existingRefs := propsMap[propertyName]
	existingRefsSlice, ok := existingRefs.([]interface{})
	if !ok {
		return nil, NewErrInternal("expected list for reference props, but got %T", existingRefs)
	}

	propsMap[propertyName] = append(existingRefsSlice, property)
	return propsMap, nil
}
