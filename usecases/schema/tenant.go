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
	"github.com/weaviate/weaviate/cluster/store"
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

	info, err := h.multiTenancy(class)
	if err != nil {
		return 0, err
	}

	names := make([]string, len(validated))
	for i, tenant := range validated {
		names[i] = tenant.Name
	}

	// create transaction payload
	var partitions map[string][]string
	f := func(_ *models.Class, st *sharding.State) (err error) {
		if st == nil {
			return fmt.Errorf("sharding state %w", ErrNotFound)
		}
		partitions, err = st.GetPartitions(h.clusterState, names, int64(info.ReplicationFactor))
		return err
	}

	if err := h.metaReader.Read(class, f); err != nil {
		return 0, fmt.Errorf("get partitions from class %q: %w", class, err)
	}

	if len(partitions) != len(names) {
		h.logger.WithField("action", "add_tenants").
			WithField("#partitions", len(partitions)).
			WithField("#requested", len(names)).
			Tracef("number of partitions for class %q does not match number of requested tenants", class)
	}
	request := api.AddTenantsRequest{
		Tenants: make([]*api.Tenant, 0, len(partitions)),
	}
	for i, name := range names {
		part, ok := partitions[name]
		if ok {
			request.Tenants = append(request.Tenants, &api.Tenant{
				Name:   name,
				Nodes:  part,
				Status: schema.ActivityStatus(validated[i].ActivityStatus),
			})
		}
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
		case models.TenantActivityStatusHOT, models.TenantActivityStatusCOLD:
			// ok
		case models.TenantActivityStatusWARM, models.TenantActivityStatusFROZEN:
			msgs = append(msgs, fmt.Sprintf(
				"not yet supported activity status '%s' for tenant %q", status, tenant.Name))
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
	if _, err := h.multiTenancy(class); err != nil {
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

	// TODO-RAFT when should we update metadata before or after apply
	// Same thing for the other operation
	if _, err := h.multiTenancy(class); err != nil {
		return err
	}

	req := api.DeleteTenantsRequest{
		Tenants: tenants,
	}

	_, err := h.metaWriter.DeleteTenants(class, &req)
	return err
}

// GetTenants is used to get tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) GetTenants(ctx context.Context, principal *models.Principal, class string) ([]*models.Tenant, error) {
	if err := h.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return nil, err
	}
	return h.getTenants(class)
}

func (h *Handler) GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool) ([]*models.Tenant, error) {
	if err := h.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return nil, err
	}

	if consistency {
		tenants, _, err := h.metaWriter.QueryTenants(class)
		return tenants, err
	}

	// If non consistent, fallback to the default implementation
	return h.getTenants(class)
}

func (h *Handler) getTenants(class string) ([]*models.Tenant, error) {
	info, err := h.multiTenancy(class)
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
	return ts, h.metaReader.Read(class, f)
}

func (h *Handler) multiTenancy(class string) (store.ClassInfo, error) {
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
func (m *Manager) TenantExists(ctx context.Context, principal *models.Principal, class string, tenant string) error {
	tenants, err := m.GetTenants(ctx, principal, class)
	if err != nil {
		return err
	}

	for _, t := range tenants {
		if t.Name == tenant {
			return nil
		}
	}
	return ErrNotFound
}

// IsLocalActiveTenant determines whether a given physical partition
// represents a tenant that is expected to be active
func IsLocalActiveTenant(phys *sharding.Physical, localNode string) bool {
	return slices.Contains(phys.BelongsToNodes, localNode) &&
		phys.Status == models.TenantActivityStatusHOT
}
