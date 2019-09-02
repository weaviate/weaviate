//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"fmt"
	"math"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

type getRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error

	ListThings(ctx context.Context, limit int, thingsResponse *models.ThingsListResponse) error
	ListActions(ctx context.Context, limit int, actionsResponse *models.ActionsListResponse) error
}

// GetThing Class from the connected DB
func (m *Manager) GetThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID) (*models.Thing, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getThingFromRepo(ctx, id)
}

// GetThings Class from the connected DB
func (m *Manager) GetThings(ctx context.Context, principal *models.Principal, limit *int64) ([]*models.Thing, error) {
	err := m.authorizer.Authorize(principal, "list", "things")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getThingsFromRepo(ctx, limit)
}

// GetAction Class from connected DB
func (m *Manager) GetAction(ctx context.Context, principal *models.Principal, id strfmt.UUID) (*models.Action, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getActionFromRepo(ctx, id)
}

// GetActions Class from connected DB
func (m *Manager) GetActions(ctx context.Context, principal *models.Principal, limit *int64) ([]*models.Action, error) {
	err := m.authorizer.Authorize(principal, "list", "actions")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getActionsFromRepo(ctx, limit)
}

func (m *Manager) getThingFromRepo(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
	if !m.config.Config.EsvectorOnly {
		return m.legacyThingFromConnector(ctx, id)
	}

	res, err := m.vectorRepo.ThingByID(ctx, id)
	if err != nil {
		return nil, NewErrInternal("repo: thing by id: %v", err)
	}

	if res == nil {
		return nil, NewErrNotFound("no thing with id '%s'", id)
	}

	return res.Thing(), nil
}

func (m *Manager) legacyThingFromConnector(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
	thing := models.Thing{}
	thing.Schema = map[string]models.JSONObject{}
	err := m.repo.GetThing(ctx, id, &thing)
	if err != nil {
		switch err.(type) {
		case ErrNotFound:
			return nil, err
		default:
			return nil, NewErrInternal("could not get thing from db: %v", err)
		}
	}

	return &thing, nil
}

func (m *Manager) getThingsFromRepo(ctx context.Context, limit *int64) ([]*models.Thing, error) {
	smartLimit := m.localLimitOrGlobalLimit(limit)
	if !m.config.Config.EsvectorOnly {
		return m.legacyThingsFromConnector(ctx, smartLimit)
	}

	res, err := m.vectorRepo.ThingSearch(ctx, smartLimit, nil)
	if err != nil {
		return nil, NewErrInternal("list things: %v", err)
	}

	return res.Things(), nil
}

func (m *Manager) legacyThingsFromConnector(ctx context.Context, limit int) ([]*models.Thing, error) {
	thingsResponse := models.ThingsListResponse{}
	thingsResponse.Things = []*models.Thing{}
	err := m.repo.ListThings(ctx, limit, &thingsResponse)
	if err != nil {
		return nil, NewErrInternal("could not list things: %v", err)
	}

	return thingsResponse.Things, nil
}

func (m *Manager) getActionFromRepo(ctx context.Context, id strfmt.UUID) (*models.Action, error) {
	if !m.config.Config.EsvectorOnly {
		return m.legacyActionFromConnector(ctx, id)
	}

	res, err := m.vectorRepo.ActionByID(ctx, id)
	if err != nil {
		return nil, NewErrInternal("repo: action by id: %v", err)
	}

	if res == nil {
		return nil, NewErrNotFound("no action with id '%s'", id)
	}

	return res.Action(), nil
}

func (m *Manager) legacyActionFromConnector(ctx context.Context, id strfmt.UUID) (*models.Action, error) {
	action := models.Action{}
	action.Schema = map[string]models.JSONObject{}
	err := m.repo.GetAction(ctx, id, &action)
	if err != nil {
		switch err.(type) {
		case ErrNotFound:
			return nil, err
		default:
			return nil, NewErrInternal("could not get action from db: %v", err)
		}
	}

	return &action, nil
}

func (m *Manager) getActionsFromRepo(ctx context.Context, limit *int64) ([]*models.Action, error) {
	smartLimit := m.localLimitOrGlobalLimit(limit)
	if !m.config.Config.EsvectorOnly {
		return m.legacyActionsFromConnector(ctx, smartLimit)
	}

	res, err := m.vectorRepo.ActionSearch(ctx, smartLimit, nil)
	if err != nil {
		return nil, NewErrInternal("list actions: %v", err)
	}

	return res.Actions(), nil
}

func (m *Manager) legacyActionsFromConnector(ctx context.Context, limit int) ([]*models.Action, error) {
	actionsResponse := models.ActionsListResponse{}
	actionsResponse.Actions = []*models.Action{}
	err := m.repo.ListActions(ctx, limit, &actionsResponse)
	if err != nil {
		return nil, NewErrInternal("could not list actions: %v", err)
	}

	return actionsResponse.Actions, nil
}

func (m *Manager) localLimitOrGlobalLimit(paramMaxResults *int64) int {
	maxResults := m.config.Config.QueryDefaults.Limit
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		maxResults = *paramMaxResults
	}

	// Max results form URL, otherwise max = config.Limit.
	return int(math.Min(float64(maxResults), float64(m.config.Config.QueryDefaults.Limit)))
}
