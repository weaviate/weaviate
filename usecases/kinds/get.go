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
	"errors"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/go-openapi/strfmt"
)

type getRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error

	ListThings(ctx context.Context, limit int, thingsResponse *models.ThingsListResponse) error
	ListActions(ctx context.Context, limit int, actionsResponse *models.ActionsListResponse) error
}

// GetThing Class from the connected DB
func (m *Manager) GetThing(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
	err := m.locks.LockConnector()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer m.locks.UnlockConnector()

	return m.getThingFromRepo(ctx, id)
}

// GetThings Class from the connected DB
func (m *Manager) GetThings(ctx context.Context, limit int) ([]*models.Thing, error) {
	err := m.locks.LockConnector()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer m.locks.UnlockConnector()

	return m.getThingsFromRepo(ctx, limit)
}

// GetAction Class from connected DB
func (m *Manager) GetAction(ctx context.Context, id strfmt.UUID) (*models.Action, error) {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return nil, newErrInternal("could not get lock: %v", err)
	}

	defer unlock(dbLock)

	return m.getActionFromRepo(ctx, id)
}

// GetActions Class from connected DB
func (m *Manager) GetActions(ctx context.Context, limit int) ([]*models.Action, error) {
	err := m.locks.LockConnector()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer m.locks.UnlockConnector()

	return m.getActionsFromRepo(ctx, limit)
}

func (m *Manager) getThingFromRepo(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
	thing := models.Thing{}
	thing.Schema = map[string]models.JSONObject{}
	err := m.repo.GetThing(ctx, id, &thing)
	if err != nil {
		switch err {
		// TODO: Replace with structured error
		case errors.New("not found"):
			return nil, newErrNotFound(err.Error())
		default:
			return nil, newErrInternal("could not get thing from db: %v", err)
		}
	}

	return &thing, nil
}

func (m *Manager) getThingsFromRepo(ctx context.Context, limit int) ([]*models.Thing, error) {
	thingsResponse := models.ThingsListResponse{}
	thingsResponse.Things = []*models.Thing{}
	err := m.repo.ListThings(ctx, limit, &thingsResponse)
	if err != nil {
		return nil, newErrInternal("could not list things: %v", err)
	}

	return thingsResponse.Things, nil
}

func (m *Manager) getActionFromRepo(ctx context.Context, id strfmt.UUID) (*models.Action, error) {
	action := models.Action{}
	action.Schema = map[string]models.JSONObject{}
	err := m.repo.GetAction(ctx, id, &action)
	if err != nil {
		switch err {
		// TODO: Replace with structured error
		case errors.New("not found"):
			return nil, newErrNotFound(err.Error())
		default:
			return nil, newErrInternal("could not get action from db: %v", err)
		}
	}

	return &action, nil
}

func (m *Manager) getActionsFromRepo(ctx context.Context, limit int) ([]*models.Action, error) {
	actionsResponse := models.ActionsListResponse{}
	actionsResponse.Actions = []*models.Action{}
	err := m.repo.ListActions(ctx, limit, &actionsResponse)
	if err != nil {
		return nil, newErrInternal("could not list actions: %v", err)
	}

	return actionsResponse.Actions, nil
}
