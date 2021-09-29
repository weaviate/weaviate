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
	"errors"
	"fmt"
	"math"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
)

// GetObject Class from the connected DB
func (m *Manager) GetObject(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, additional additional.Properties) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	res, err := m.getObjectFromRepo(ctx, id, additional)
	if err != nil {
		return nil, err
	}

	return res.ObjectWithVector(additional.Vector), nil
}

// GetObjects Class from the connected DB
func (m *Manager) GetObjects(ctx context.Context, principal *models.Principal,
	offset, limit *int64, additional additional.Properties) ([]*models.Object, error) {
	err := m.authorizer.Authorize(principal, "list", "objects")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.getObjectsFromRepo(ctx, offset, limit, additional)
}

func (m *Manager) getObjectFromRepo(ctx context.Context, id strfmt.UUID,
	additional additional.Properties) (*search.Result, error) {
	res, err := m.vectorRepo.ObjectByID(ctx, id, search.SelectProperties{}, additional)
	if err != nil {
		return nil, NewErrInternal("repo: object by id: %v", err)
	}

	if res == nil {
		return nil, NewErrNotFound("no object with id '%s'", id)
	}

	if m.modulesProvider != nil {
		res, err = m.modulesProvider.GetObjectAdditionalExtend(ctx, res, additional.ModuleParams)
		if err != nil {
			return nil, fmt.Errorf("get extend: %v", err)
		}
	}

	return res, nil
}

func (m *Manager) getObjectsFromRepo(ctx context.Context, offset, limit *int64,
	additional additional.Properties) ([]*models.Object, error) {
	smartOffset, smartLimit, err := m.localOffsetLimit(offset, limit)
	if err != nil {
		return nil, NewErrInternal("list objects: %v", err)
	}
	res, err := m.vectorRepo.ObjectSearch(ctx, smartOffset, smartLimit, nil, additional)
	if err != nil {
		return nil, NewErrInternal("list objects: %v", err)
	}

	if m.modulesProvider != nil {
		res, err = m.modulesProvider.ListObjectsAdditionalExtend(ctx, res, additional.ModuleParams)
		if err != nil {
			return nil, NewErrInternal("list extend: %v", err)
		}
	}

	return res.ObjectsWithVector(additional.Vector), nil
}

func (m *Manager) localOffsetOrZero(paramOffset *int64) int {
	offset := int64(0)
	if paramOffset != nil {
		offset = *paramOffset
	}

	return int(offset)
}

func (m *Manager) localLimitOrGlobalLimit(offset int64, paramMaxResults *int64) int {
	maxResults := m.config.Config.QueryMaximumResults
	if offset < m.config.Config.QueryMaximumResults {
		maxResults = m.config.Config.QueryMaximumResults - offset
	}
	limit := maxResults
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		limit = *paramMaxResults
	}

	// Max results form URL, otherwise max = config.Limit.
	return int(math.Min(float64(limit), float64(maxResults)))
}

func (m *Manager) localOffsetLimit(paramOffset *int64, paramLimit *int64) (int, int, error) {
	offset := m.localOffsetOrZero(paramOffset)
	limit := m.localLimitOrGlobalLimit(int64(offset), paramLimit)

	if int64(offset+limit) > m.config.Config.QueryMaximumResults {
		return 0, 0, errors.New("query maximum results exceeded")
	}

	return offset, limit, nil
}
