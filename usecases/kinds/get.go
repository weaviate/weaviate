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
	"math"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// GetThing Class from the connected DB
func (m *Manager) GetThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, underscore traverser.UnderscoreProperties) (*models.Thing, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return nil, err
	}

	if underscore.FeatureProjection != nil {
		return nil, fmt.Errorf("feature projection is not possible on a non-list request")
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	res, err := m.getThingFromRepo(ctx, id, underscore)
	if err != nil {
		return nil, err
	}

	return res.Thing(), nil
}

// GetThings Class from the connected DB
func (m *Manager) GetThings(ctx context.Context, principal *models.Principal,
	limit *int64, underscore traverser.UnderscoreProperties) ([]*models.Thing, error) {
	err := m.authorizer.Authorize(principal, "list", "things")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getThingsFromRepo(ctx, limit, underscore)
}

// GetAction Class from connected DB
func (m *Manager) GetAction(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, underscore traverser.UnderscoreProperties) (*models.Action, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return nil, err
	}

	if underscore.FeatureProjection != nil {
		return nil, fmt.Errorf("feature projection is not possible on a non-list request")
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	action, err := m.getActionFromRepo(ctx, id, underscore)
	if err != nil {
		return nil, err
	}

	return action.Action(), nil
}

// GetActions Class from connected DB
func (m *Manager) GetActions(ctx context.Context, principal *models.Principal,
	limit *int64, underscore traverser.UnderscoreProperties) ([]*models.Action, error) {
	err := m.authorizer.Authorize(principal, "list", "actions")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.getActionsFromRepo(ctx, limit, underscore)
}

func (m *Manager) getThingFromRepo(ctx context.Context, id strfmt.UUID,
	underscore traverser.UnderscoreProperties) (*search.Result, error) {
	res, err := m.vectorRepo.ThingByID(ctx, id, traverser.SelectProperties{}, underscore)
	if err != nil {
		return nil, NewErrInternal("repo: thing by id: %v", err)
	}

	if res == nil {
		return nil, NewErrNotFound("no thing with id '%s'", id)
	}

	if underscore.NearestNeighbors {
		res, err = m.nnExtender.Single(ctx, res, nil)
		if err != nil {
			return nil, NewErrInternal("extend nearest neighbors: %v", err)
		}
	}

	return res, nil
}

func (m *Manager) getThingsFromRepo(ctx context.Context, limit *int64,
	underscore traverser.UnderscoreProperties) ([]*models.Thing, error) {
	smartLimit := m.localLimitOrGlobalLimit(limit)

	res, err := m.vectorRepo.ThingSearch(ctx, smartLimit, nil, underscore)
	if err != nil {
		return nil, NewErrInternal("list things: %v", err)
	}

	if underscore.NearestNeighbors {
		res, err = m.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, NewErrInternal("extend nearest neighbors: %v", err)
		}
	}

	if underscore.FeatureProjection != nil {
		res, err = m.projector.Reduce(res, &projector.Params{Enabled: true})
		if err != nil {
			return nil, NewErrInternal("perform feature projection: %v", err)
		}
	}

	return res.Things(), nil
}

func (m *Manager) getActionFromRepo(ctx context.Context, id strfmt.UUID,
	underscore traverser.UnderscoreProperties) (*search.Result, error) {
	res, err := m.vectorRepo.ActionByID(ctx, id, traverser.SelectProperties{}, underscore)
	if err != nil {
		return nil, NewErrInternal("repo: action by id: %v", err)
	}

	if res == nil {
		return nil, NewErrNotFound("no action with id '%s'", id)
	}

	if underscore.NearestNeighbors {
		res, err = m.nnExtender.Single(ctx, res, nil)
		if err != nil {
			return nil, NewErrInternal("extend nearest neighbors: %v", err)
		}
	}

	return res, nil
}

func (m *Manager) getActionsFromRepo(ctx context.Context, limit *int64,
	underscore traverser.UnderscoreProperties) ([]*models.Action, error) {
	smartLimit := m.localLimitOrGlobalLimit(limit)

	res, err := m.vectorRepo.ActionSearch(ctx, smartLimit, nil, underscore)
	if err != nil {
		return nil, NewErrInternal("list actions: %v", err)
	}

	if underscore.NearestNeighbors {
		res, err = m.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, NewErrInternal("extend nearest neighbors: %v", err)
		}
	}

	if underscore.FeatureProjection != nil {
		res, err = m.projector.Reduce(res, &projector.Params{Enabled: true})
		if err != nil {
			return nil, NewErrInternal("perform feature projection: %v", err)
		}
	}

	return res.Actions(), nil
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
