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

package schema

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ManagerWithConsistency expose the same interface as Manager but with the consistency flag.
// This is used to ensure that internal users will not miss-use the flag and it doesn't need to be set to a default
// value everytime we use the Manager.
type ManagerWithConsistency struct {
	*Manager
}

func NewManagerWithConsistency(handler *Manager) ManagerWithConsistency {
	return ManagerWithConsistency{Manager: handler}
}

// GetClass overrides the default implementation to consider the consistency flag
func (m *ManagerWithConsistency) GetClass(ctx context.Context, principal *models.Principal,
	name string, consistency bool,
) (*models.Class, error) {
	if err := m.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return nil, err
	}
	if consistency {
		return m.metaWriter.QueryReadOnlyClass(name)
	} else {
		return m.metaReader.ReadOnlyClass(name), nil
	}
}

// GetSchema retrieves a locally cached copy of the schema
func (m *ManagerWithConsistency) GetSchema(principal *models.Principal, consistency bool) (schema.Schema, error) {
	if err := m.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return schema.Schema{}, err
	}

	if !consistency {
		return m.getSchema(), nil
	}

	if consistentSchema, err := m.metaWriter.QueryGetSchema(); err != nil {
		return schema.Schema{}, fmt.Errorf("could not read schema with strong consistency: %w", err)
	} else {
		return schema.Schema{
			Objects: &consistentSchema,
		}, nil
	}
}

func (m *ManagerWithConsistency) GetTenants(ctx context.Context, principal *models.Principal, class string, consistency bool) ([]*models.Tenant, error) {
	if err := m.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return nil, err
	}

	if consistency {
		return m.metaWriter.QueryGetTenants(class)
	}

	// If non consistent, fallback to the default implementation

	// validation
	info, err := m.multiTenancy(class)
	if err != nil || info.Tenants == 0 {
		return nil, err
	}

	ts := make([]*models.Tenant, info.Tenants)
	f := func(_ *models.Class, ss *sharding.State) error {
		if N := len(ss.Physical); N > len(ts) {
			ts = make([]*models.Tenant, N)
		} else if N < len(ts) {
			ts = ts[:N]
		}
		i := 0
		for tenant := range ss.Physical {
			ts[i] = &models.Tenant{
				Name:           tenant,
				ActivityStatus: schema.ActivityStatus(ss.Physical[tenant].Status),
			}
			i++
		}
		return nil
	}
	return ts, m.metaReader.Read(class, f)
}
