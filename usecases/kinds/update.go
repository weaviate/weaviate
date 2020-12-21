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
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// UpdateObject Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal, id strfmt.UUID,
	class *models.Object) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("%s", id.String()))
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

	originalObject, err := m.getObjectFromRepo(ctx, id, traverser.UnderscoreProperties{})
	if err != nil {
		return nil, err
	}

	m.logger.
		WithField("object", "kinds_update_requested").
		WithField("kind", kind.Object).
		WithField("original", originalObject).
		WithField("updated", class).
		WithField("id", id).
		Debug("received update kind request")

	err = m.validateObject(ctx, principal, class)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	class.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.vectorizeAndPutObject(ctx, class)
	if err != nil {
		return nil, NewErrInternal("update object: %v", err)
	}

	return class, nil
}

// UpdateThing Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
// func (m *Manager) UpdateThing(ctx context.Context, principal *models.Principal,
// 	id strfmt.UUID, class *models.Thing) (*models.Thing, error) {
// 	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("things/%s", id.String()))
// 	if err != nil {
// 		return nil, err
// 	}

// 	unlock, err := m.locks.LockSchema()
// 	if err != nil {
// 		return nil, NewErrInternal("could not acquire lock: %v", err)
// 	}
// 	defer unlock()

// 	return m.updateThingToConnectorAndSchema(ctx, principal, id, class)
// }

// func (m *Manager) updateThingToConnectorAndSchema(ctx context.Context, principal *models.Principal,
// 	id strfmt.UUID, class *models.Thing) (*models.Thing, error) {
// 	if id != class.ID {
// 		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
// 	}

// 	originalThing, err := m.getThingFromRepo(ctx, id, traverser.UnderscoreProperties{})
// 	if err != nil {
// 		return nil, err
// 	}

// 	m.logger.
// 		WithField("action", "kinds_update_requested").
// 		WithField("kind", kind.Thing).
// 		WithField("original", originalThing).
// 		WithField("updated", class).
// 		WithField("id", id).
// 		Debug("received update kind request")

// 	err = m.validateThing(ctx, principal, class)
// 	if err != nil {
// 		return nil, NewErrInvalidUserInput("invalid object: %v", err)
// 	}

// 	class.LastUpdateTimeUnix = m.timeSource.Now()

// 	err = m.vectorizeAndPutThing(ctx, class)
// 	if err != nil {
// 		return nil, NewErrInternal("update thing: %v", err)
// 	}

// 	return class, nil
// }
