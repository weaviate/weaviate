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

// UpdateObject updates object of class.
// If the class contains a network ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	class string, id strfmt.UUID, updates *models.Object) (*models.Object, error) {
	path := fmt.Sprintf("objects/%s/%s", class, id)
	if class == "" {
		path = fmt.Sprintf("objects/%s", id)
	}
	err := m.authorizer.Authorize(principal, "update", path)
	if err != nil {
		return nil, err
	}

	m.metrics.UpdateObjectInc()
	defer m.metrics.UpdateObjectDec()

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.updateObjectToConnectorAndSchema(ctx, principal, class, id, updates)
}

func (m *Manager) updateObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	class string, id strfmt.UUID, updates *models.Object) (*models.Object, error) {
	if id != updates.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	obj, err := m.getObjectFromRepo(ctx, class, id, additional.Properties{})
	if err != nil {
		return nil, err
	}

	m.logger.
		WithField("object", "kinds_update_requested").
		WithField("original", obj).
		WithField("updated", updates).
		WithField("id", id).
		Debug("received update kind request")

	err = m.validateObject(ctx, principal, updates)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	// Set the original creation timestamp before call to put,
	// otherwise it is lost. This is because `class` is unmarshaled
	// directly from the request body, therefore `CreationTimeUnix`
	// inherits the zero value.
	updates.CreationTimeUnix = obj.Created
	updates.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.vectorizeAndPutObject(ctx, updates, principal)
	if err != nil {
		return nil, NewErrInternal("update object: %v", err)
	}

	return updates, nil
}
