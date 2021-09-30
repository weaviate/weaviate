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

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
)

// UpdateObjectReferences Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObjectReferences(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, refs models.MultipleRef) error {
	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.updateObjectReferenceToConnectorAndSchema(ctx, principal, id, propertyName, refs)
}

func (m *Manager) updateObjectReferenceToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, refs models.MultipleRef) error {
	// get object to see if it exists
	objectRes, err := m.getObjectFromRepo(ctx, id, additional.Properties{})
	if err != nil {
		return err
	}

	object := objectRes.Object()
	err = m.validateReferences(ctx, refs)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, object.Class, propertyName)
	if err != nil {
		return err
	}

	updatedSchema, err := m.replaceClassPropReferences(object.Properties, propertyName, refs)
	if err != nil {
		return err
	}
	object.Properties = updatedSchema
	object.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.vectorRepo.PutObject(ctx, object, objectRes.Vector)
	if err != nil {
		return NewErrInternal("could not store object: %v", err)
	}

	return nil
}

func (m *Manager) validateReferences(ctx context.Context, references models.MultipleRef) error {
	err := validation.New(schema.Schema{}, m.exists, m.config).
		ValidateMultipleRef(ctx, references, "reference not found")
	if err != nil {
		return NewErrInvalidUserInput("invalid references: %v", err)
	}

	return nil
}

func (m *Manager) replaceClassPropReferences(props interface{}, propertyName string,
	refs models.MultipleRef) (interface{}, error) {
	if props == nil {
		props = map[string]interface{}{}
	}

	propsMap := props.(map[string]interface{})
	propsMap[propertyName] = refs
	return propsMap, nil
}
