//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
)

// GetObject Class from the connected DB
func (m *Manager) GetObject(ctx context.Context, principal *models.Principal,
	class string, id strfmt.UUID, additional additional.Properties,
	replProps *additional.ReplicationProperties, tenant string,
) (*models.Object, error) {
	err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Objects(class, tenant, id))
	if err != nil {
		return nil, err
	}

	m.metrics.GetObjectInc()
	defer m.metrics.GetObjectDec()

	res, err := m.getObjectFromRepo(ctx, class, id, additional, replProps, tenant)
	if err != nil {
		return nil, err
	}

	if additional.Vector {
		m.trackUsageSingle(res)
	}

	return res.ObjectWithVector(additional.Vector), nil
}

// GetObjects Class from the connected DB
func (m *Manager) GetObjects(ctx context.Context, principal *models.Principal,
	offset *int64, limit *int64, sort *string, order *string, after *string,
	addl additional.Properties, tenant string,
) ([]*models.Object, error) {
	err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Objects("", tenant, ""))
	if err != nil {
		return nil, err
	}

	m.metrics.GetObjectInc()
	defer m.metrics.GetObjectDec()

	objects, err := m.getObjectsFromRepo(ctx, offset, limit, sort, order, after, addl, tenant)
	if err != nil {
		return nil, err
	}

	// Filter objects based on authorization
	resourceFilter := filter.New[*models.Object](m.authorizer, m.config.Config.Authorization.Rbac)
	filteredObjects := resourceFilter.Filter(
		ctx,
		m.logger,
		principal,
		objects,
		authorization.READ,
		func(obj *models.Object) string {
			return authorization.Objects(obj.Class, tenant, obj.ID)
		},
	)

	return filteredObjects, nil
}

func (m *Manager) GetObjectsClass(ctx context.Context, principal *models.Principal,
	id strfmt.UUID,
) (*models.Class, error) {
	err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Objects("", "", id))
	if err != nil {
		return nil, err
	}

	m.metrics.GetObjectInc()
	defer m.metrics.GetObjectDec()

	res, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{}, nil, "")
	if err != nil {
		return nil, err
	}

	class, err := m.schemaManager.GetClass(ctx, principal, res.ClassName)
	return class, err
}

func (m *Manager) GetObjectClassFromName(ctx context.Context, principal *models.Principal,
	className string,
) (*models.Class, error) {
	class, err := m.schemaManager.GetClass(ctx, principal, className)
	return class, err
}

func (m *Manager) getObjectFromRepo(ctx context.Context, class string, id strfmt.UUID,
	adds additional.Properties, repl *additional.ReplicationProperties, tenant string,
) (res *search.Result, err error) {
	if class != "" {
		res, err = m.vectorRepo.Object(ctx, class, id, search.SelectProperties{}, adds, repl, tenant)
	} else {
		res, err = m.vectorRepo.ObjectByID(ctx, id, search.SelectProperties{}, adds, tenant)
	}
	if err != nil {
		switch {
		case errors.As(err, &ErrMultiTenancy{}):
			return nil, NewErrMultiTenancy(fmt.Errorf("repo: object by id: %w", err))
		default:
			if errors.As(err, &authzerrs.Forbidden{}) {
				return nil, fmt.Errorf("repo: object by id: %w", err)
			}
			return nil, NewErrInternal("repo: object by id: %v", err)
		}
	}

	if res == nil {
		return nil, NewErrNotFound("no object with id '%s'", id)
	}

	if m.modulesProvider != nil {
		res, err = m.modulesProvider.GetObjectAdditionalExtend(ctx, res, adds.ModuleParams)
		if err != nil {
			return nil, fmt.Errorf("get extend: %w", err)
		}
	}

	return res, nil
}

func (m *Manager) getObjectsFromRepo(ctx context.Context,
	offset, limit *int64, sort, order *string, after *string,
	additional additional.Properties, tenant string,
) ([]*models.Object, error) {
	smartOffset, smartLimit, err := m.localOffsetLimit(offset, limit)
	if err != nil {
		return nil, NewErrInternal("list objects: %v", err)
	}
	if after != nil {
		return nil, NewErrInternal("list objects: after parameter not allowed, cursor must be specific to one class, set class query param")
	}
	res, err := m.vectorRepo.ObjectSearch(ctx, smartOffset, smartLimit,
		nil, m.getSort(sort, order), additional, tenant)
	if err != nil {
		return nil, NewErrInternal("list objects: %v", err)
	}

	if m.modulesProvider != nil {
		res, err = m.modulesProvider.ListObjectsAdditionalExtend(ctx, res, additional.ModuleParams)
		if err != nil {
			return nil, NewErrInternal("list extend: %v", err)
		}
	}

	if additional.Vector {
		m.trackUsageList(res)
	}

	return res.ObjectsWithVector(additional.Vector), nil
}

func (m *Manager) getSort(sort, order *string) []filters.Sort {
	if sort != nil {
		sortParams := strings.Split(*sort, ",")
		var orderParams []string
		if order != nil {
			orderParams = strings.Split(*order, ",")
		}
		var res []filters.Sort
		for i := range sortParams {
			res = append(res, filters.Sort{
				Path:  []string{sortParams[i]},
				Order: m.getOrder(orderParams, i),
			})
		}
		return res
	}
	return nil
}

func (m *Manager) getOrder(order []string, i int) string {
	if len(order) > i {
		switch order[i] {
		case "asc", "desc":
			return order[i]
		default:
			return "asc"
		}
	}
	return "asc"
}

func (m *Manager) localOffsetOrZero(paramOffset *int64) int {
	offset := int64(0)
	if paramOffset != nil {
		offset = *paramOffset
	}

	return int(offset)
}

func (m *Manager) localLimitOrGlobalLimit(offset int64, paramMaxResults *int64) int {
	limit := int64(m.config.Config.QueryDefaults.Limit)
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		limit = *paramMaxResults
	}

	return int(limit)
}

func (m *Manager) localOffsetLimit(paramOffset *int64, paramLimit *int64) (int, int, error) {
	offset := m.localOffsetOrZero(paramOffset)
	limit := m.localLimitOrGlobalLimit(int64(offset), paramLimit)

	if int64(offset+limit) > m.config.Config.QueryMaximumResults {
		return 0, 0, errors.New("query maximum results exceeded")
	}

	return offset, limit, nil
}

func (m *Manager) trackUsageSingle(res *search.Result) {
	if res == nil {
		return
	}
	m.metrics.AddUsageDimensions(res.ClassName, "get_rest", "single_include_vector", res.Dims)
}

func (m *Manager) trackUsageList(res search.Results) {
	if len(res) == 0 {
		return
	}
	m.metrics.AddUsageDimensions(res[0].ClassName, "get_rest", "list_include_vector", res[0].Dims)
}

func (m *Manager) getCursor(after *string, limit *int64) *filters.Cursor {
	if after != nil {
		if limit == nil {
			// limit -1 means that no limit param was set
			return &filters.Cursor{After: *after, Limit: -1}
		}
		return &filters.Cursor{After: *after, Limit: int(*limit)}
	}
	return nil
}
