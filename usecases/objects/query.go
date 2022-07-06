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

	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
)

type QueryInput struct {
	Class      string
	Offset     int
	Limit      int
	Filters    *filters.LocalFilter
	Sort       []filters.Sort
	Additional additional.Properties
}

type QueryParams struct {
	Class      string
	Offset     *int64
	Limit      *int64
	Sort       *string
	Order      *string
	Additional additional.Properties
}

func (q *QueryParams) inputs(m *Manager) (*QueryInput, error) {
	smartOffset, smartLimit, err := m.localOffsetLimit(q.Offset, q.Limit)
	if err != nil {
		return nil, err
	}
	return &QueryInput{
		Class:      q.Class,
		Offset:     smartOffset,
		Limit:      smartLimit,
		Sort:       m.getSort(q.Sort, q.Order),
		Additional: q.Additional,
	}, nil
}

func (m *Manager) Query(ctx context.Context, principal *models.Principal, params *QueryParams) ([]*models.Object, *Error) {
	path := fmt.Sprintf("objects/%s", params.Class)
	if err := m.authorizer.Authorize(principal, "list", path); err != nil {
		return nil, &Error{path, StatusForbidden, err}
	}
	unlock, err := m.locks.LockConnector()
	if err != nil {
		return nil, &Error{"cannot lock", StatusInternalServerError, err}
	}
	defer unlock()

	q, err := params.inputs(m)
	if err != nil {
		return nil, &Error{"offset or limit", StatusBadRequest, err}
	}
	res, rerr := m.vectorRepo.Query(ctx, q)
	if rerr != nil {
		return nil, rerr
	}

	if m.modulesProvider != nil {
		res, err = m.modulesProvider.ListObjectsAdditionalExtend(ctx, res, q.Additional.ModuleParams)
		if err != nil {
			return nil, &Error{"extend results", StatusInternalServerError, err}
		}
	}

	return res.ObjectsWithVector(q.Additional.Vector), nil
}
