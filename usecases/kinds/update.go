//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type updateAndGetRepo interface {
	// The validation package needs to be able to get existing classes as well
	getRepo

	// methods to update new items
	updateRepo
}

type updateRepo interface {
	UpdateAction(ctx context.Context, class *models.Action, id strfmt.UUID) error
	UpdateThing(ctx context.Context, class *models.Thing, id strfmt.UUID) error
}

// UpdateAction Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateAction(ctx context.Context, principal *models.Principal, id strfmt.UUID,
	class *models.Action) (*models.Action, error) {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.updateActionToConnectorAndSchema(ctx, principal, id, class)
}

func (m *Manager) updateActionToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, class *models.Action) (*models.Action, error) {
	if id != class.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	originalAction, err := m.getActionFromRepo(ctx, id)
	if err != nil {
		return nil, err
	}

	m.logger.
		WithField("action", "kinds_update_requested").
		WithField("kind", kind.Action).
		WithField("original", originalAction).
		WithField("updated", class).
		WithField("id", id).
		Debug("received update kind request")

	err = m.validateAction(ctx, principal, class)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid action: %v", err)
	}

	err = m.addNetworkDataTypesForAction(ctx, principal, class)
	if err != nil {
		return nil, NewErrInternal("could not update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()

	if !m.config.Config.EsvectorOnly {
		err = m.repo.UpdateAction(ctx, class, class.ID)
	} else {
		err = m.vectorizeAndPutAction(ctx, class)
	}
	if err != nil {
		return nil, NewErrInternal("update action: %v", err)
	}

	return class, nil
}

// UpdateThing Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, class *models.Thing) (*models.Thing, error) {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.updateThingToConnectorAndSchema(ctx, principal, id, class)
}

func (m *Manager) updateThingToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, class *models.Thing) (*models.Thing, error) {
	if id != class.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	originalThing, err := m.getThingFromRepo(ctx, id)
	if err != nil {
		return nil, err
	}

	m.logger.
		WithField("action", "kinds_update_requested").
		WithField("kind", kind.Thing).
		WithField("original", originalThing).
		WithField("updated", class).
		WithField("id", id).
		Debug("received update kind request")

	err = m.validateThing(ctx, principal, class)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid thing: %v", err)
	}

	err = m.addNetworkDataTypesForThing(ctx, principal, class)
	if err != nil {
		return nil, NewErrInternal("update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()

	if !m.config.Config.EsvectorOnly {
		err = m.repo.UpdateThing(ctx, class, class.ID)
	} else {
		err = m.vectorizeAndPutThing(ctx, class)
	}
	if err != nil {
		return nil, NewErrInternal("update thing: %v", err)
	}

	return class, nil
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
