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
	"time"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/go-openapi/strfmt"
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
func (m *Manager) UpdateAction(ctx context.Context, id strfmt.UUID, class *models.Action) (*models.Action, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.updateActionToConnectorAndSchema(ctx, id, class, dbConnector, schemaManager)
}

func (m *Manager) updateActionToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	class *models.Action, updateRepo updateAndGetRepo, schemaManager database.SchemaManager) (*models.Action,
	error) {
	if id != class.ID {
		return nil, newErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	_, err := m.getActionFromRepo(ctx, id)
	if err != nil {
		return nil, err
	}

	err = m.validateAction(ctx, class)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid action: %v", err)
	}

	err = m.addNetworkDataTypesForAction(ctx, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()
	updateRepo.UpdateAction(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store updated action: %v", err)
	}

	return class, nil
}

// UpdateThing Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateThing(ctx context.Context, id strfmt.UUID, class *models.Thing) (*models.Thing, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.updateThingToConnectorAndSchema(ctx, id, class, dbConnector, schemaManager)
}

func (m *Manager) updateThingToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	class *models.Thing, updateRepo updateAndGetRepo, schemaManager database.SchemaManager) (*models.Thing,
	error) {
	if id != class.ID {
		return nil, newErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	_, err := m.getThingFromRepo(ctx, id)
	if err != nil {
		return nil, err
	}

	err = m.validateThing(ctx, class)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid thing: %v", err)
	}

	err = m.addNetworkDataTypesForThing(ctx, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()
	updateRepo.UpdateThing(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store updated thing: %v", err)
	}

	return class, nil
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
