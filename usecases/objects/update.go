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

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
)

// UpdateObject Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal, id strfmt.UUID,
	class *models.Object) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.updateObjectToConnectorAndSchema(ctx, principal, id, class)
}

func (m *Manager) updateObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, class *models.Object) (*models.Object, error) {
	if id != class.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	originalObject, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{})
	if err != nil {
		return nil, err
	}

	m.logger.
		WithField("object", "kinds_update_requested").
		WithField("original", originalObject).
		WithField("updated", class).
		WithField("id", id).
		Debug("received update kind request")

	err = m.validateObject(ctx, principal, class)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	// Set the original creation timestamp before call to put,
	// otherwise it is lost. This is because `class` is unmarshaled
	// directly from the request body, therefore `CreationTimeUnix`
	// inherits the zero value.
	class.CreationTimeUnix = originalObject.Created
	class.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.vectorizeAndPutObject(ctx, class, principal)
	if err != nil {
		return nil, NewErrInternal("update object: %v", err)
	}

	return class, nil
}
