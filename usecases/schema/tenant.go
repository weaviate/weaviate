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
	"regexp"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var regexTenantName = regexp.MustCompile(`^` + schema.ShardNameRegexCore + `$`)

// tenantsPath is the main path used for authorization
const tenantsPath = "schema/tenants"

// AddTenants is used to add new tenants to a class
// Class must exist and has partitioning enabled
func (h *Handler) AddTenants(ctx context.Context,
	principal *models.Principal,
	class string,
	tenants []*models.Tenant,
) (uint64, error) {
	if err := h.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return 0, err
	}

	validated, err := validateTenants(tenants)
	if err != nil {
		return 0, err
	}

	if err = validateActivityStatuses(validated, true); err != nil {
		return 0, err
	}

	request := api.AddTenantsRequest{
		ClusterNodes: h.clusterState.Candidates(),
		Tenants:      make([]*api.Tenant, 0, len(validated)),
	}
	for i, tenant := range validated {
		request.Tenants = append(request.Tenants, &api.Tenant{
			Name:   tenant.Name,
			Status: schema.ActivityStatus(validated[i].ActivityStatus),
		})
	}

	return h.metaWriter.AddTenants(class, &request)
}

func validateTenants(tenants []*models.Tenant) (validated []*models.Tenant, err error) {
	uniq := make(map[string]*models.Tenant)
	for i, requested := range tenants {
		if !regexTenantName.MatchString(requested.Name) {
			var msg string
			if requested.Name == "" {
				msg = "empty tenant name"
			} else {
				msg = "tenant name should only contain alphanumeric characters (a-z, A-Z, 0-9), " +
					"underscore (_), and hyphen (-), with a length between 1 and 64 characters"
			}
			err = uco.NewErrInvalidUserInput("tenant name at index %d: %s", i, msg)
			return
		}
		_, found := uniq[requested.Name]
		if !found {
			uniq[requested.Name] = requested
		}
	}
	validated = make([]*models.Tenant, len(uniq))
	i := 0
	for _, tenant := range uniq {
		validated[i] = tenant
		i++
	}
	return
}

func validateActivityStatuses(tenants []*models.Tenant, allowEmpty bool) error {
	msgs := make([]string, 0, len(tenants))

	for _, tenant := range tenants {
		switch status := tenant.ActivityStatus; status {
		case models.TenantActivityStatusHOT, models.TenantActivityStatusCOLD, models.TenantActivityStatusFROZEN:
			continue

		default:
			if status == "" && allowEmpty {
				continue
			}
			msgs = append(msgs, fmt.Sprintf(
				"invalid activity status '%s' for tenant %q", status, tenant.Name))
		}
	}

	if len(msgs) != 0 {
		return uco.NewErrInvalidUserInput(strings.Join(msgs, ", "))
	}
	return nil
}

// UpdateTenants is used to set activity status of tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) UpdateTenants(ctx context.Context, principal *models.Principal,
	class string, tenants []*models.Tenant,
) error {
	if err := h.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return err
	}
	validated, err := validateTenants(tenants)
	if err != nil {
		return err
	}
	if err := validateActivityStatuses(validated, false); err != nil {
		return err
	}

	req := api.UpdateTenantsRequest{
		Tenants: make([]*api.Tenant, len(tenants)),
	}
	for i, tenant := range tenants {
		req.Tenants[i] = &api.Tenant{Name: tenant.Name, Status: tenant.ActivityStatus}
	}
	_, err = h.metaWriter.UpdateTenants(class, &req)
	return err
}

// DeleteTenants is used to delete tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) DeleteTenants(ctx context.Context, principal *models.Principal, class string, tenants []string) error {
	if err := h.Authorizer.Authorize(principal, "delete", tenantsPath); err != nil {
		return err
	}
	for i, name := range tenants {
		if name == "" {
			return fmt.Errorf("empty tenant name at index %d", i)
		}
	}

	req := api.DeleteTenantsRequest{
		Tenants: tenants,
	}

	_, err := h.metaWriter.DeleteTenants(class, &req)
	return err
}

func (h *Handler) GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool,
	tenants []string, after *string, limit *int64,
) ([]*models.Tenant, error) {
	if err := h.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return nil, err
	}

	if consistency {
		tenants, _, err := h.metaWriter.QueryTenants(class, tenants, after, limit)
		return tenants, err
	}

	// If non consistent, fallback to the default implementation
	return h.getTenantsByNames(class, tenants, after, limit)
}

func (h *Handler) multiTenancy(class string) (clusterSchema.ClassInfo, error) {
	info := h.metaReader.ClassInfo(class)
	if !info.Exists {
		return info, fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !info.MultiTenancy.Enabled {
		return info, fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}
	return info, nil
}

// TenantExists is used to check if the tenant exists of a class
//
// Class must exist and has partitioning enabled
func (h *Handler) ConsistentTenantExists(ctx context.Context, principal *models.Principal, class string, consistency bool, tenant string,
	after *string, limit *int64,
) error {
	if err := h.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return err
	}

	var tenants []*models.Tenant
	var err error
	if consistency {
		tenants, _, err = h.metaWriter.QueryTenants(class, []string{tenant}, after, limit)
	} else {
		// If non-consistent, fallback to the default implementation
		tenants, err = h.getTenantsByNames(class, []string{tenant}, after, limit)
	}
	if err != nil {
		return err
	}
	if len(tenants) == 1 {
		return nil
	}

	return ErrNotFound
}

// IsLocalActiveTenant determines whether a given physical partition
// represents a tenant that is expected to be active
func IsLocalActiveTenant(phys *sharding.Physical, localNode string) bool {
	return slices.Contains(phys.BelongsToNodes, localNode) &&
		phys.Status == models.TenantActivityStatusHOT
}

func (h *Handler) getTenantsByNames(class string, names []string, after *string, limit *int64) ([]*models.Tenant, error) {
	info, err := h.multiTenancy(class)
	if err != nil || info.Tenants == 0 {
		return nil, err
	}

	ts := make([]*models.Tenant, 0, len(names))
	f := func(_ *models.Class, ss *sharding.State) error {
		if after != nil && limit != nil {
			ts = store.GetAllTenantsPriorityQueue(ss.Physical, *after, int(*limit))
			return nil
		}

		ts = store.CappedTenants(limit, ts, names, ss)
		return nil
	}
	return ts, h.metaReader.Read(class, f)
}
