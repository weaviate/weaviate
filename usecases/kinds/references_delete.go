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
 */package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/go-openapi/strfmt"
)

// DeleteActionReference from connected DB
func (m *Manager) DeleteActionReference(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	classSchema := schemaLock.GetSchema()
	dbConnector := schemaLock.Connector()

	return m.deleteActionReferenceFromConnector(ctx, id, propertyName, property,
		dbConnector, classSchema)
}

func (m *Manager) deleteActionReferenceFromConnector(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef, repo updateRepo,
	classSchema schema.Schema) error {

	// get action to see if it exists
	action, err := m.getActionFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	// NOTE: The reference itself is not being validated, to allow for deletion
	// of broken references
	err = m.validateCanModifyReference(kind.ACTION_KIND, action.Class, propertyName, classSchema)
	if err != nil {
		return err
	}

	extended, err := m.removeReferenceFromClassProps(action.Schema, propertyName, property)
	if err != nil {
		return err
	}
	action.Schema = extended
	action.LastUpdateTimeUnix = unixNow()

	repo.UpdateAction(ctx, action, action.ID)
	if err != nil {
		return newErrInternal("could not store action: %v", err)
	}

	return nil
}

// DeleteThingReference from connected DB
func (m *Manager) DeleteThingReference(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	classSchema := schemaLock.GetSchema()
	dbConnector := schemaLock.Connector()

	return m.deleteThingReferenceFromConnector(ctx, id, propertyName, property,
		dbConnector, classSchema)
}

func (m *Manager) deleteThingReferenceFromConnector(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef, repo updateRepo,
	classSchema schema.Schema) error {

	// get thing to see if it exists
	thing, err := m.getThingFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	// NOTE: The reference itself is not being validated, to allow for deletion
	// of broken references
	err = m.validateCanModifyReference(kind.THING_KIND, thing.Class, propertyName, classSchema)
	if err != nil {
		return err
	}

	extended, err := m.removeReferenceFromClassProps(thing.Schema, propertyName, property)
	if err != nil {
		return err
	}
	thing.Schema = extended
	thing.LastUpdateTimeUnix = unixNow()

	repo.UpdateThing(ctx, thing, thing.ID)
	if err != nil {
		return newErrInternal("could not store thing: %v", err)
	}

	return nil
}

func (m *Manager) removeReferenceFromClassProps(props interface{}, propertyName string,
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
		return nil, newErrInternal("expected list for reference props, but got %T", existingRefs)
	}

	propsMap[propertyName] = removeRef(existingRefsSlice, property)
	return propsMap, nil
}

func removeRef(refs []interface{}, property *models.SingleRef) []interface{} {
	crefStr := string(property.NrDollarCref)

	// Remove if this reference is found.
	for i, current := range refs {
		currentMap := current.(map[string]interface{})
		if currentMap["$cref"].(string) != crefStr {
			continue
		}

		// remove this one without memory leaks, see
		// https://github.com/golang/go/wiki/SliceTricks#delete
		copy(refs[i:], refs[i+1:])
		refs[len(refs)-1] = nil // or the zero vrefslue of T
		refs = refs[:len(refs)-1]
		break // we can only remove one at the same time, so break the loop.
	}

	return refs
}
