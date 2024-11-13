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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var regexTenantName = regexp.MustCompile(`^` + schema.ShardNameRegexCore + `$`)

const (
	ErrMsgMaxAllowedTenants = "maximum number of tenants allowed to be updated simultaneously is 100. Please reduce the number of tenants in your request and try again"
)

// AddTenants is used to add new tenants to a class
// Class must exist and has partitioning enabled
func (h *Handler) AddTenants(ctx context.Context,
	principal *models.Principal,
	class string,
	tenants []*models.Tenant,
) (uint64, error) {
	if err := h.Authorizer.Authorize(principal, authorization.CREATE, authorization.Shards(class)...); err != nil {
		return 0, err
	}

	validated, err := validateTenants(tenants, true)
	if err != nil {
		return 0, err
	}

	if err = h.validateActivityStatuses(ctx, validated, true, false); err != nil {
		return 0, err
	}

	request := api.AddTenantsRequest{
		ClusterNodes: h.schemaManager.StorageCandidates(),
		Tenants:      make([]*api.Tenant, 0, len(validated)),
	}
	for i, tenant := range validated {
		request.Tenants = append(request.Tenants, &api.Tenant{
			Name:   tenant.Name,
			Status: schema.ActivityStatus(validated[i].ActivityStatus),
		})
	}

	return h.schemaManager.AddTenants(ctx, class, &request)
}

func validateTenants(tenants []*models.Tenant, allowOverHundred bool) (validated []*models.Tenant, err error) {
	if !allowOverHundred && len(tenants) > 100 {
		err = uco.NewErrInvalidUserInput(ErrMsgMaxAllowedTenants)
		return
	}
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
		if found {
			err = uco.NewErrInvalidUserInput("tenant name %s existed multiple times", requested.Name)
			return
		}
		uniq[requested.Name] = requested
	}
	validated = make([]*models.Tenant, len(uniq))
	i := 0
	for _, tenant := range uniq {
		validated[i] = tenant
		i++
	}
	return
}

func (h *Handler) validateActivityStatuses(ctx context.Context, tenants []*models.Tenant,
	allowEmpty, allowFrozen bool,
) error {
	msgs := make([]string, 0, len(tenants))

	for _, tenant := range tenants {
		tenant.ActivityStatus = convertNewTenantNames(tenant.ActivityStatus)
		switch status := tenant.ActivityStatus; status {
		case models.TenantActivityStatusHOT, models.TenantActivityStatusCOLD:
			continue
		case models.TenantActivityStatusFROZEN:
			if mod := h.moduleConfig.GetByName(modsloads3.Name); mod == nil {
				return fmt.Errorf(
					"can't offload tenants, because offload-s3 module is not enabled")
			}

			if allowFrozen && h.cloud != nil {
				if err := h.cloud.VerifyBucket(ctx); err != nil {
					return err
				}
			}

			if allowFrozen {
				continue
			}

		default:
			if status == "" && allowEmpty {
				continue
			}
		}
		msgs = append(msgs, fmt.Sprintf(
			"invalid activity status '%s' for tenant %q", tenant.ActivityStatus, tenant.Name))
	}

	if len(msgs) != 0 {
		return uco.NewErrInvalidUserInput("%s", strings.Join(msgs, ", "))
	}
	return nil
}

// UpdateTenants is used to set activity status of tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) UpdateTenants(ctx context.Context, principal *models.Principal,
	class string, tenants []*models.Tenant,
) ([]*models.Tenant, error) {
	shardNames := make([]string, len(tenants))
	for idx := range tenants {
		shardNames[idx] = tenants[idx].Name
	}

	if err := h.Authorizer.Authorize(principal, authorization.UPDATE, authorization.Shards(class, shardNames...)...); err != nil {
		return nil, err
	}

	h.logger.WithFields(logrus.Fields{
		"class":   class,
		"tenants": tenants,
	}).Debug("update tenants status")

	validated, err := validateTenants(tenants, false)
	if err != nil {
		return nil, err
	}
	if err := h.validateActivityStatuses(ctx, validated, false, true); err != nil {
		return nil, err
	}

	req := api.UpdateTenantsRequest{
		Tenants:      make([]*api.Tenant, len(tenants)),
		ClusterNodes: h.schemaManager.StorageCandidates(),
	}
	tNames := make([]string, len(tenants))
	for i, tenant := range tenants {
		tNames[i] = tenant.Name
		req.Tenants[i] = &api.Tenant{Name: tenant.Name, Status: tenant.ActivityStatus}
	}

	if _, err = h.schemaManager.UpdateTenants(ctx, class, &req); err != nil {
		return nil, err
	}

	// we get the new state to return correct status
	// specially in FREEZING and UNFREEZING
	uTenants, _, err := h.schemaManager.QueryTenants(class, tNames)
	if err != nil {
		return nil, err
	}
	return TenantResponsesToTenants(uTenants), err
}

// DeleteTenants is used to delete tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) DeleteTenants(ctx context.Context, principal *models.Principal, class string, tenants []string) error {
	if err := h.Authorizer.Authorize(principal, authorization.DELETE, authorization.Shards(class, tenants...)...); err != nil {
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

	_, err := h.schemaManager.DeleteTenants(ctx, class, &req)
	return err
}

// GetTenants is used to get tenants of a class.
//
// Class must exist and has partitioning enabled
func (h *Handler) GetTenants(ctx context.Context, principal *models.Principal, class string) ([]*models.Tenant, error) {
	if err := h.Authorizer.Authorize(principal, authorization.READ, authorization.Shards(class)...); err != nil {
		return nil, err
	}
	return h.getTenants(class)
}

func (h *Handler) GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool, tenants []string) ([]*models.TenantResponse, error) {
	if err := h.Authorizer.Authorize(principal, authorization.READ, authorization.Shards(class)...); err != nil {
		return nil, err
	}

	if consistency {
		tenants, _, err := h.schemaManager.QueryTenants(class, tenants)
		return tenants, err
	}

	// If non consistent, fallback to the default implementation
	return h.getTenantsByNames(class, tenants)
}

func (h *Handler) getTenants(class string) ([]*models.Tenant, error) {
	info, err := h.multiTenancy(class)
	if err != nil || info.Tenants == 0 {
		return nil, err
	}

	ts := make([]*models.Tenant, info.Tenants)
	f := func(_ *models.Class, ss *sharding.State) error {
		if n := len(ss.Physical); n > len(ts) {
			ts = make([]*models.Tenant, n)
		} else if n < len(ts) {
			ts = ts[:n]
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
	return ts, h.schemaReader.Read(class, f)
}

func (h *Handler) multiTenancy(class string) (clusterSchema.ClassInfo, error) {
	info := h.schemaReader.ClassInfo(class)
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
func (h *Handler) ConsistentTenantExists(ctx context.Context, principal *models.Principal, class string, consistency bool, tenant string) error {
	if err := h.Authorizer.Authorize(principal, authorization.READ, authorization.Shards(class)...); err != nil {
		return err
	}

	var tenantResponses []*models.TenantResponse
	var err error
	if consistency {
		tenantResponses, _, err = h.schemaManager.QueryTenants(class, []string{tenant})
	} else {
		// If non consistent, fallback to the default implementation
		tenantResponses, err = h.getTenantsByNames(class, []string{tenant})
	}
	if err != nil {
		return err
	}
	if len(tenantResponses) == 1 {
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

func (h *Handler) getTenantsByNames(class string, names []string) ([]*models.TenantResponse, error) {
	info, err := h.multiTenancy(class)
	if err != nil || info.Tenants == 0 {
		return nil, err
	}

	ts := make([]*models.TenantResponse, 0, len(names))
	f := func(_ *models.Class, ss *sharding.State) error {
		for _, name := range names {
			if _, ok := ss.Physical[name]; !ok {
				continue
			}
			physical := ss.Physical[name]
			ts = append(ts, clusterSchema.MakeTenantWithDataVersion(name, schema.ActivityStatus(physical.Status), physical.DataVersion))
		}
		return nil
	}
	return ts, h.schemaReader.Read(class, f)
}

// convert the new tenant names (that are only used as input) to the old tenant names that are used throughout the code
func convertNewTenantNames(status string) string {
	if status == models.TenantActivityStatusACTIVE {
		return models.TenantActivityStatusHOT
	}
	if status == models.TenantActivityStatusINACTIVE {
		return models.TenantActivityStatusCOLD
	}
	if status == models.TenantActivityStatusOFFLOADED {
		return models.TenantActivityStatusFROZEN
	}
	return status
}

func tenantResponseToTenant(tenantResponse *models.TenantResponse) *models.Tenant {
	return &models.Tenant{
		Name:           tenantResponse.Name,
		ActivityStatus: tenantResponse.ActivityStatus,
	}
}

func TenantResponsesToTenants(tenantResponses []*models.TenantResponse) []*models.Tenant {
	tenants := make([]*models.Tenant, len(tenantResponses))
	for i, tenantResponse := range tenantResponses {
		tenants[i] = tenantResponseToTenant(tenantResponse)
	}
	return tenants
}
